package handler

import (
	"context"
	"encoding/json"
	"github.com/995933447/bucketmq/internal/engine"
	"github.com/995933447/bucketmq/internal/ha/synclog"
	"github.com/995933447/bucketmq/internal/syscfg"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/bucketmq/pkg/rpc/errs"
	"github.com/995933447/bucketmq/pkg/rpc/ha"
	"github.com/995933447/gonetutil"
	"github.com/995933447/microgosuit/discovery"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var _ ha.HAServer = (*HA)(nil)

type HA struct {
	discovery.Discovery
	maxSyncedLogId uint64
	syncLogMu      sync.RWMutex
}

func (h *HA) PullRemoteReplica(ctx context.Context, req *ha.PullRemoteReplicaReq) (*ha.PullRemoteReplicaResp, error) {
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
		case ha.MsgFileType_MsgFileTypeFinish, ha.MsgFileType_MsgFileTypeMsgId, ha.MsgFileType_MsgFileTypeLoadBoot:
			fp, err = os.OpenFile(engine.GetSubscriberFileDir(syscfg.MustCfg().DataDir, logItem.Topic, logItem.Subscriber)+"/"+logItem.FileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
		}
		if err != nil {
			return nil, err
		}

		_, err = fp.Write(logItem.FileBuf)
		if err != nil {
			return nil, err
		}

		engine.OnAnyFileWritten(logItem.FileName, logItem.FileBuf, &engine.ExtraOfFileWritten{
			Topic:            logItem.Topic,
			Subscriber:       logItem.Subscriber,
			ContentCreatedAt: logItem.CreatedAt,
		})

		h.maxSyncedLogId = logItem.LogId
	}

	return &ha.SyncRemoteReplicaResp{}, nil
}
