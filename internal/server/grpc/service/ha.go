package service

import (
	"context"
	"encoding/json"
	"github.com/995933447/bucketmq/internal/engine"
	nodegrpha "github.com/995933447/bucketmq/internal/ha"
	"github.com/995933447/bucketmq/internal/ha/synclog"
	"github.com/995933447/bucketmq/internal/syscfg"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/bucketmq/pkg/rpc/errs"
	"github.com/995933447/bucketmq/pkg/rpc/ha"
	"github.com/995933447/microgosuit/discovery"
	clientv3 "go.etcd.io/etcd/client/v3"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var _ ha.HAServer = (*HA)(nil)

func NewHA(etcdCli *clientv3.Client, disc discovery.Discovery) (*HA, error) {
	return &HA{
		etcdCli:   etcdCli,
		Discovery: disc,
	}, nil
}

type HA struct {
	discovery.Discovery
	syncLogMu sync.RWMutex
	etcdCli   *clientv3.Client
}

func (h *HA) PullRemoteReplica(_ context.Context, req *ha.PullRemoteReplicaReq) (*ha.PullRemoteReplicaResp, error) {
	isMaster, err := nodegrpha.RefreshIsMasterRole(h.etcdCli)
	if err != nil {
		return nil, err
	}

	if !isMaster {
		return &ha.PullRemoteReplicaResp{}, nil
	}

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

	fileNum := len(idxFilesSortedBySeq)

	var (
		destIdxFp      *os.File
		destDataFp     *os.File
		startIdxOffset uint32
	)
	for i, idxFile := range idxFilesSortedBySeq {
		curNOSeqStr, _ := synclog.ParseFileNOSeqStr(idxFile)
		curNOSeq, err := strconv.ParseUint(curNOSeqStr, 10, 64)
		if err != nil {
			return nil, err
		}

		curDateTimeSeq := idxFile.Name()[:10]

		if curNOSeq < req.LastSyncedLogId && i+1 < fileNum {
			nextIdxFile := idxFilesSortedBySeq[i+1]

			nextNOSeqStr, _ := synclog.ParseFileNOSeqStr(nextIdxFile)
			nextNOSeq, err := strconv.ParseUint(nextNOSeqStr, 10, 64)
			if err != nil {
				return nil, err
			}

			if nextNOSeq < req.LastSyncedLogId {
				continue
			}
		}

		fileInfo, err := os.Stat(idxFile.Name())
		if err != nil {
			return nil, err
		}

		idxNum := fileInfo.Size() / synclog.IdxBytes

		idxFp, err := synclog.MakeSeqIdxFp(sysCfg.Dir, curDateTimeSeq, curNOSeq, os.O_RDONLY)
		if err != nil {
			return nil, err
		}

		dataFp, err := synclog.MakeSeqDataFp(sysCfg.Dir, curDateTimeSeq, curNOSeq, os.O_RDONLY)
		if err != nil {
			return nil, err
		}

		for offset := int64(0); offset < idxNum; i++ {
			logItem, _, err := synclog.ReadLogItem(idxFp, dataFp, uint32(offset))
			if err != nil {
				return nil, err
			}

			if logItem.LogId != req.LastSyncedLogId || logItem.Term != req.TermOfLastSyncedLog {
				continue
			}

			startIdxOffset = uint32(offset)
			destIdxFp = idxFp
			destDataFp = dataFp

			break
		}

		if destIdxFp != nil {
			break
		}
	}

	if destIdxFp == nil {
		return &ha.PullRemoteReplicaResp{}, nil
	}

	var (
		resp              ha.PullRemoteReplicaResp
		fileBufTotalBytes int
	)
	for {
		item, hasMore, err := synclog.ReadLogItem(destIdxFp, destDataFp, startIdxOffset)
		if err != nil {
			return nil, err
		}

		if item.LogId > nodegrpha.MaxSyncedLogId && item.Term >= nodegrpha.TermOfMaxSyncedLog {
			return &resp, nil
		}

		resp.LogItems = append(resp.LogItems, item)

		if !hasMore {
			break
		}

		fileBufTotalBytes += len(item.FileBuf)
		if fileBufTotalBytes > 10*1024*1024 {
			break
		}

		startIdxOffset++
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

	origMaxSyncedLogId := nodegrpha.MaxSyncedLogId
	origTermOfMaxSyncedLog := nodegrpha.TermOfMaxSyncedLog
	defer func() {
		if nodegrpha.MaxSyncedLogId == origMaxSyncedLogId && nodegrpha.TermOfMaxSyncedLog == origTermOfMaxSyncedLog {
			return
		}

		unlock, err := util.DistributeLockForUpdateDiscovery(h.etcdCli)
		if err != nil {
			util.Logger.Error(nil, err)
			return
		}

		defer unlock()

		myNode, exist, err := util.DiscoverMyNode(h.Discovery)
		if err != nil {
			return
		}

		if !exist {
			return
		}

		var nodeDesc ha.Node
		err = json.Unmarshal([]byte(myNode.Extra), &nodeDesc)
		if err != nil {
			util.Logger.Error(nil, err)
			return
		}

		nodeDesc.MaxSyncedLogId = nodegrpha.MaxSyncedLogId
		nodeDesc.TermOfMaxSyncedLog = nodegrpha.TermOfMaxSyncedLog

		nodeDescJ, err := json.Marshal(nodeDesc)
		if err != nil {
			util.Logger.Error(nil, err)
			return
		}

		myNode.Extra = string(nodeDescJ)

		err = h.Discovery.Register(ctx, discover.SrvNameBroker, myNode)
		if err != nil {
			util.Logger.Error(nil, err)
			return
		}
	}()

	if nodegrpha.MaxSyncedLogId < req.LastSyncedLogId || nodegrpha.TermOfMaxSyncedLog < req.TermOfLastSyncedLog {
		return nil, errs.GRPCErr(errs.ErrCode_ErrCodeLastSyncedMsgLogFallBehind, "")
	}

	for _, logItem := range req.LogItems {
		if logItem.Term <= nodegrpha.TermOfMaxSyncedLog && logItem.LogId <= nodegrpha.MaxSyncedLogId {
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

		fileInfo, err := fp.Stat()
		if err != nil {
			fp.Close()
			return nil, err
		}

		if uint32(fileInfo.Size()) > logItem.FileOffset {
			if err = fp.Truncate(int64(logItem.FileOffset - 1)); err != nil {
				if err = os.Truncate(fp.Name(), int64(logItem.FileOffset-1)); err != nil {
					fp.Close()
					return nil, err
				}
			}
		}

		_, err = fp.WriteAt(logItem.FileBuf, int64(logItem.FileOffset))
		if err != nil {
			fp.Close()
			return nil, err
		}

		err = engine.LogMsgFileOp(logItem.FileName, logItem.FileBuf, logItem.FileOffset, &engine.OutputExtra{
			Topic:            logItem.Topic,
			Subscriber:       logItem.Subscriber,
			ContentCreatedAt: logItem.CreatedAt,
		})
		if err != nil {
			return nil, err
		}

		nodegrpha.MaxSyncedLogId = logItem.LogId
		nodegrpha.TermOfMaxSyncedLog = logItem.Term
		nodegrpha.LastSyncedLogAt = uint32(time.Now().Unix())
	}

	return &ha.SyncRemoteReplicaResp{}, nil
}
