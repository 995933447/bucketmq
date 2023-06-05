package handler

import (
	"context"
	"encoding/json"
	"github.com/995933447/autoelectv2"
	"github.com/995933447/autoelectv2/factory"
	"github.com/995933447/bucketmq/internal/engine"
	nodegrpha "github.com/995933447/bucketmq/internal/ha"
	"github.com/995933447/bucketmq/internal/ha/synclog"
	"github.com/995933447/bucketmq/internal/syscfg"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/bucketmq/pkg/rpc/errs"
	"github.com/995933447/bucketmq/pkg/rpc/ha"
	"github.com/995933447/gonetutil"
	"github.com/995933447/microgosuit/discovery"
	clientv3 "go.etcd.io/etcd/client/v3"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var _ ha.HAServer = (*HA)(nil)

type HA struct {
	discovery.Discovery
	isMaster           atomic.Bool
	hALogSync          synclog.Sync
	maxSyncedLogId     uint64
	termOfMaxSyncedLog uint64
	syncLogMu          sync.RWMutex
	etcdCli            *clientv3.Client
	elect              autoelectv2.AutoElection
}

func (h *HA) init() error {
	if err := h.initElect(); err != nil {
		return err
	}

	return nil
}

func (h *HA) initElect() (err error) {
	electKey := nodegrpha.GetNodeGrpElectEtcdKey()
	h.elect, err = factory.NewAutoElection(factory.ElectDriverDistribMuEtcdv3, factory.NewDistribMuEtcdv3Cfg(electKey, h.etcdCli, 5))
	if err != nil {
		return err
	}

	h.elect.OnBeMaster(func() bool {
		meta, err := nodegrpha.GetNodeGrpHAMeta(h.etcdCli)
		if err != nil {
			util.Logger.Error(nil, err)
			return false
		}

		if h.maxSyncedLogId < meta.MaxSyncLogId || h.termOfMaxSyncedLog < meta.TermOfMaxSyncLog {
			return false
		}

		resp, err := h.etcdCli.Get(context.Background(), electKey, clientv3.WithFirstCreate()...)
		if err != nil {
			util.Logger.Error(nil, err)
			return false
		}

		if len(resp.Kvs) == 0 {
			util.Logger.Error(nil, "not master")
			return false
		}

		nodegrpha.NodeGrpMasterElectEtcdKey = string(resp.Kvs[0].Key)
		nodegrpha.NodeGrpMasterElectEtcdKeyCreateVersion = resp.Kvs[0].CreateRevision

		meta.ElectTerm++
		succ, err := nodegrpha.SaveNodeGrpHAMetaByMasterRole(h.etcdCli, meta)
		if err != nil {
			util.Logger.Error(nil, err)
			return false
		}

		if !succ {
			return false
		}

		nodegrpha.SwitchMasterRole(meta.ElectTerm)

		unlock, err := util.DistributeLockNodeGrpForUpdateDiscovery(h.etcdCli)
		if err != nil {
			return false
		}

		defer unlock()

		if ok, err := nodegrpha.RefreshIsMasterRole(h.etcdCli); err != nil {
			util.Logger.Error(nil, err)
			return false
		} else if !ok {
			return false
		}

		brokerCfg, err := h.Discovery.Discover(context.Background(), discover.SrvNameBroker)
		if err != nil {
			util.Logger.Error(nil, err)
			return false
		}

		sysCfg := syscfg.MustCfg()
		curHost, err := gonetutil.EvalVarToParseIp(sysCfg.Host)
		if err != nil {
			util.Logger.Error(context.Background(), err)
			return false
		}

		for _, node := range brokerCfg.Nodes {
			var nodeDesc ha.Node
			err = json.Unmarshal([]byte(node.Extra), &nodeDesc)
			if err != nil {
				util.Logger.Error(context.Background(), err)
				return false
			}

			nodeDesc.IsMaster = false

			if node.Host == curHost && node.Port == sysCfg.Port2 {
				nodeDesc.IsMaster = true
			}

			nodeDescJ, err := json.Marshal(nodeDesc)
			if err != nil {
				util.Logger.Error(context.Background(), err)
				return false
			}

			node.Extra = string(nodeDescJ)

			err = h.Discovery.Register(context.Background(), discover.SrvNameBroker, node)
			if err != nil {
				util.Logger.Error(context.Background(), err)
				return false
			}
		}

		for {
			ok, err := nodegrpha.RefreshIsMasterRole(h.etcdCli)
			if err != nil {
				util.Logger.Error(context.Background(), err)
				return false
			}

			if !ok {
				break
			}

			if h.hALogSync.Consumer.IsExiting() {
				time.Sleep(time.Second)
				continue
			}

			go func() {
				if err = h.hALogSync.Consumer.Start(); err != nil {
					util.Logger.Error(nil, err)
				}
			}()

			break
		}

		return true
	})

	h.elect.OnLostMaster(func() {
		h.hALogSync.Writer.Stop()
		time.Sleep(time.Millisecond * 5)
		nodegrpha.RevokeMasterRole()
		h.hALogSync.Writer.Resume()
		h.hALogSync.Consumer.Exit()
	})

	electErrCh := make(chan error)
	go func() {
		for {
			err := <-electErrCh
			util.Logger.Error(nil, err)
		}
	}()
	h.elect.LoopInElect(context.Background(), electErrCh)

	return nil
}

func (h *HA) PullRemoteReplica(_ context.Context, req *ha.PullRemoteReplicaReq) (*ha.PullRemoteReplicaResp, error) {
	sysCfg := syscfg.MustCfg()
	dir := synclog.GetHADataDir(sysCfg.Dir)

	if _, err := os.Stat(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var idxFilesSortedBySeq []os.DirEntry
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), synclog.IdxFileSuffix) {
			continue
		}

		idxFilesSortedBySeq = append(idxFilesSortedBySeq, file)
	}

	sort.Slice(idxFilesSortedBySeq, func(i, j int) bool {
		oneNOSeqStr, _ := synclog.ParseFileNOSeqStr(idxFilesSortedBySeq[i])
		otherNOSeqStr, _ := synclog.ParseFileNOSeqStr(idxFilesSortedBySeq[j])
		return oneNOSeqStr < otherNOSeqStr
	})

	var (
		destNOSeq       uint64
		destDateTimeSeq string
		fileNum         = len(idxFilesSortedBySeq)
	)
	for i, idxFile := range idxFilesSortedBySeq {
		nOSeqStr, _ := synclog.ParseFileNOSeqStr(idxFile)
		curNOSeq, err := strconv.ParseUint(nOSeqStr, 10, 64)
		if err != nil {
			return nil, err
		}

		if curNOSeq == req.LastSyncedLogId {
			destNOSeq = curNOSeq
			destDateTimeSeq = idxFile.Name()[:10]
			break
		}

		if curNOSeq < req.LastSyncedLogId {
			if i+1 < fileNum {
				nextIdxFile := idxFilesSortedBySeq[i+1]
				nextNOSeqStr, _ := synclog.ParseFileNOSeqStr(nextIdxFile)
				nextNOSeq, err := strconv.ParseUint(nextNOSeqStr, 10, 64)
				if err != nil {
					return nil, err
				}
				if nextNOSeq < req.LastSyncedLogId {
					continue
				}
				if nextNOSeq == req.LastSyncedLogId {
					destNOSeq = nextNOSeq
					destDateTimeSeq = nextIdxFile.Name()[:10]
					break
				}
			}

			fileInfo, err := os.Stat(idxFile.Name())
			if err != nil {
				return nil, err
			}

			if uint64(fileInfo.Size())+curNOSeq-1 > req.LastSyncedLogId {
				destNOSeq = curNOSeq
				destDateTimeSeq = idxFile.Name()[:10]
				break
			}

			continue
		}

		destNOSeq = curNOSeq
		destDateTimeSeq = idxFile.Name()[:10]
		break
	}

	if destNOSeq == 0 {
		return &ha.PullRemoteReplicaResp{}, nil
	}

	idxFp, err := synclog.MakeSeqIdxFp(sysCfg.Dir, destDateTimeSeq, destNOSeq, os.O_RDONLY)
	if err != nil {
		return nil, err
	}

	dataFp, err := synclog.MakeSeqDataFp(sysCfg.Dir, destDateTimeSeq, destNOSeq, os.O_RDONLY)
	if err != nil {
		return nil, err
	}

	var idxOffset uint64
	if destNOSeq <= req.LastSyncedLogId+1 {
		idxOffset = req.LastSyncedLogId - destNOSeq + 1
	}

	var (
		resp              ha.PullRemoteReplicaResp
		fileBufTotalBytes int
	)
	for {
		item, hasMore, err := synclog.ReadLogItem(idxFp, dataFp, idxOffset)
		if err != nil {
			return nil, err
		}

		resp.LogItems = append(resp.LogItems, item)

		if !hasMore {
			break
		}

		fileBufTotalBytes += len(item.FileBuf)
		if fileBufTotalBytes > 10*1024*1024 {
			break
		}
	}

	return &resp, nil
}

func (h *HA) SyncRemoteReplica(ctx context.Context, req *ha.SyncRemoteReplicaReq) (*ha.SyncRemoteReplicaResp, error) {
	if len(req.LogItems) == 0 {
		return &ha.SyncRemoteReplicaResp{}, nil
	}

	if nodegrpha.IsMasterRole() {
		return nil, errs.GRPCErr(errs.ErrCode_ErrCodeLastSyncedMsgLogFallBehind, "")
	}

	h.syncLogMu.Lock()
	defer h.syncLogMu.Unlock()

	defer func() {
		brokerCfg, err := h.Discovery.Discover(ctx, discover.SrvNameBroker)
		if err != nil {
			util.Logger.Error(context.Background(), err)
			return
		}

		sysCfg := syscfg.MustCfg()
		curHost, err := gonetutil.EvalVarToParseIp(sysCfg.Host)
		if err != nil {
			util.Logger.Error(context.Background(), err)
			return
		}

		var myNode *discovery.Node
		for _, node := range brokerCfg.Nodes {
			if node.Host != curHost {
				continue
			}

			if node.Port != sysCfg.Port2 {
				continue
			}

			myNode = node
			break
		}

		if myNode == nil {
			return
		}

		var nodeDesc ha.Node
		err = json.Unmarshal([]byte(myNode.Extra), &nodeDesc)
		if err != nil {
			util.Logger.Error(context.Background(), err)
			return
		}

		nodeDesc.MaxSyncedLogId = h.maxSyncedLogId

		nodeDescJ, err := json.Marshal(nodeDesc)
		if err != nil {
			util.Logger.Error(context.Background(), err)
			return
		}

		myNode.Extra = string(nodeDescJ)

		err = h.Discovery.Register(ctx, discover.SrvNameBroker, myNode)
		if err != nil {
			util.Logger.Error(context.Background(), err)
			return
		}
	}()

	if h.maxSyncedLogId < req.LastSyncedLogId {
		return nil, errs.GRPCErr(errs.ErrCode_ErrCodeLastSyncedMsgLogFallBehind, "")
	}

	for _, logItem := range req.LogItems {
		if logItem.LogId <= h.maxSyncedLogId {
			continue
		}

		var (
			fp  *os.File
			err error
		)
		switch logItem.MsgFileType {
		case ha.MsgFileType_MsgFileTypeIdx, ha.MsgFileType_MsgFileTypeData:
			fp, err = os.OpenFile(engine.GetTopicFileDir(syscfg.MustCfg().DataDir, logItem.Topic)+"/"+logItem.FileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
		case ha.MsgFileType_MsgFileTypeFinish:
			fp, err = os.OpenFile(engine.GetSubscriberFileDir(syscfg.MustCfg().DataDir, logItem.Topic, logItem.Subscriber)+"/"+logItem.FileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
		case ha.MsgFileType_MsgFileTypeMsgId, ha.MsgFileType_MsgFileTypeLoadBoot:
			fp, err = os.OpenFile(engine.GetSubscriberFileDir(syscfg.MustCfg().DataDir, logItem.Topic, logItem.Subscriber)+"/"+logItem.FileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
		}
		if err != nil {
			return nil, err
		}

		_, err = fp.WriteAt(logItem.FileBuf, int64(logItem.FileOffset))
		if err != nil {
			return nil, err
		}

		err = engine.OnOutputFile(logItem.FileName, logItem.FileBuf, logItem.FileOffset, &engine.OutputExtra{
			Topic:            logItem.Topic,
			Subscriber:       logItem.Subscriber,
			ContentCreatedAt: logItem.CreatedAt,
		})
		if err != nil {
			return nil, err
		}

		h.maxSyncedLogId = logItem.LogId
	}

	return &ha.SyncRemoteReplicaResp{}, nil
}
