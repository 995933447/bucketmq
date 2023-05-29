package synclog

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/bucketmq/pkg/rpc/ha"
	"github.com/995933447/microgosuit/discovery"
	"github.com/995933447/microgosuit/grpcsuit"
	"github.com/fsnotify/fsnotify"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var errConsumerNotRunning = errors.New("consumer not running")

func newConsumer(baseDir string) (*Consumer, error) {
	consumer := &Consumer{
		baseDir:           baseDir,
		confirmMsgCh:      make(chan *confirmMsgReq),
		unsubscribeSignCh: make(chan struct{}),
	}

	finishRec, err := newConsumeWaterMarkRec(baseDir)
	if err != nil {
		return nil, err
	}

	consumer.finishRec = finishRec

	nOSeq, dateTimeSeq, idxNum := finishRec.getWaterMark()

	if nOSeq != 0 && dateTimeSeq != "" {
		consumer.nextIdxCursor = idxNum

		consumer.idxFp, err = MakeSeqIdxFp(consumer.baseDir, dateTimeSeq, nOSeq, os.O_CREATE|os.O_RDONLY)
		if err != nil {
			return nil, err
		}

		consumer.dataFp, err = MakeSeqDataFp(consumer.baseDir, dateTimeSeq, nOSeq, os.O_CREATE|os.O_RDONLY)
		if err != nil {
			return nil, err
		}

		if err = consumer.refreshMsgNum(); err != nil {
			return nil, err
		}
	}

	return consumer, nil
}

type confirmMsgReq struct {
	nOSeq       uint64
	dateTimeSeq string
	idxOffset   uint64
}

type Consumer struct {
	discovery.Discovery
	baseDir           string
	status            runState
	idxFp             *os.File
	dataFp            *os.File
	nextIdxCursor     uint64
	finishRec         *ConsumeWaterMarkRec
	opFinishRecMu     sync.RWMutex
	msgNum            uint64
	unsubscribeSignCh chan struct{}
	confirmMsgCh      chan *confirmMsgReq
}

func (c *Consumer) Start() error {
	fileWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	if c.isSubscribed() {
		return errors.New("already in running")
	}

	c.status = runStateRunning

	// windows no fsnotify
	directlyTrySwitchFileTk := time.NewTicker(time.Second * 2)
	defer directlyTrySwitchFileTk.Stop()
	syncDiskTk := time.NewTicker(time.Second * 5)
	defer syncDiskTk.Stop()
	needWatchNewFile := c.idxFp != nil
	for {
		if c.nextIdxCursor < c.msgNum {
			if err = c.consumeBatch(); err != nil {
				util.Logger.Error(context.Background(), err)
			}
			continue
		}

		needSwitchNewFile, err := c.finishRec.isOffsetsFinishedInSeq()
		if err != nil {
			util.Logger.Error(nil, err)
			time.Sleep(time.Millisecond * 500)
			continue
		}

		if needSwitchNewFile {
			if c.idxFp != nil {
				_ = fileWatcher.Remove(c.idxFp.Name())
			}
			for {
				if err = c.switchNextSeqFile(); err != nil {
					if err != errSeqNotFound {
						util.Logger.Error(nil, err)
					}
					time.Sleep(time.Millisecond * 500)
					continue
				}

				needWatchNewFile = true
				break
			}
		}

		if needWatchNewFile {
			util.Logger.Debug(nil, "watched new index file:"+c.idxFp.Name())
			if err = fileWatcher.Add(c.idxFp.Name()); err != nil {
				util.Logger.Error(nil, err)
				time.Sleep(time.Second)
				continue
			}

			needWatchNewFile = false
			if err = c.refreshMsgNum(); err != nil {
				util.Logger.Error(nil, err)
			}
			continue
		}

		util.Logger.Debug(nil, "loop into select")
		select {
		case <-directlyTrySwitchFileTk.C:
		case <-syncDiskTk.C:
			c.finishRec.syncDisk()
		case event := <-fileWatcher.Events:
			util.Logger.Debug(nil, "watch file changed")
			if event.Has(fsnotify.Chmod) {
				break
			}

			if event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
				needSwitchNewFile = true
				break
			}

			if err = c.refreshMsgNum(); err != nil {
				util.Logger.Debug(nil, err)
			}
		case <-c.unsubscribeSignCh:
			util.Logger.Debug(nil, "unwatched")
			c.status = runStateExited
			goto out
		}
	}
out:
	c.finishRec.syncDisk()
	return nil
}

func (c *Consumer) refreshMsgNum() error {
	idxFileState, err := c.idxFp.Stat()
	if err != nil {
		return err
	}
	c.msgNum = uint64(idxFileState.Size()) / idxBytes
	return nil
}

func (c *Consumer) SyncRemoteReplicas(logItems []*ha.SyncMsgFileLogItem) (bool, error) {
	brokerCfg, err := c.Discovery.Discover(context.Background(), discover.SrvNameBroker)
	if err != nil {
		return false, err
	}

	var (
		hasSucc atomic.Bool
		wg      sync.WaitGroup
	)
	for _, node := range brokerCfg.Nodes {
		var nodeDesc ha.Node
		err = json.Unmarshal([]byte(node.Extra), &nodeDesc)
		if err != nil {
			return false, err
		}

		if nodeDesc.MaxSyncedLogId < c.finishRec.nOSeq+c.finishRec.idxNum-1 {
			continue
		}

		wg.Add(1)
		go func(node *discovery.Node) {
			defer wg.Done()

			conn, err := grpc.Dial(fmt.Sprintf("%s:%d", node.Host, node.Port), grpcsuit.NotRoundRobinDialOpts...)
			if err != nil {
				util.Logger.Warn(context.Background(), err)
				return
			}

			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
			defer cancel()

			_, err = ha.NewHAClient(conn).SyncRemoteReplica(ctx, &ha.SyncRemoteReplicaReq{
				LogItems:        logItems,
				LastSyncedLogId: c.finishRec.nOSeq + c.finishRec.idxNum - 1,
			})
			if err != nil {
				util.Logger.Warn(context.Background(), err)
				return
			}

			hasSucc.Store(true)
		}(node)
	}
	wg.Wait()

	return hasSucc.Load(), nil
}

type PoppedMsgItem struct {
	Topic     string
	Seq       uint64
	IdxOffset uint32
	Data      []byte
	CreatedAt int64
	RetryAt   uint32
	RetryCnt  uint32
}

func (c *Consumer) consumeBatch() error {
	if !c.isSubscribed() {
		return errConsumerNotRunning
	}

	if c.msgNum <= c.nextIdxCursor {
		return nil
	}

	var (
		items          []*ha.SyncMsgFileLogItem
		totalDataBytes int
	)
	for {
		if totalDataBytes > 10*1024*1024 {
			break
		}

		if c.msgNum <= c.nextIdxCursor {
			break
		}

		item, hasMore, err := ReadLogItem(c.idxFp, c.dataFp, c.nextIdxCursor)
		if err != nil {
			return err
		}

		items = append(items, item)

		if !hasMore {
			break
		}

		c.nextIdxCursor++
	}

	var retryCnt int
	for {
		succ, err := c.SyncRemoteReplicas(items)
		if err != nil {
			return err
		}

		if !succ {
			retryCnt++
			if retryCnt < 15 {
				time.Sleep(time.Second * time.Duration(retryCnt))
			} else {
				time.Sleep(time.Second * 15)
			}
		}

		break
	}

	err := c.updateFinishWaterMark(c.finishRec.nOSeq, c.finishRec.dateTimeSeq, c.finishRec.idxNum+uint64(len(items)))
	if err != nil {
		return err
	}

	return nil
}

func (c *Consumer) unsubscribe() {
	c.unsubscribeSignCh <- struct{}{}
}

func (c *Consumer) isSubscribed() bool {
	return c.status == runStateRunning
}

func (c *Consumer) updateFinishWaterMark(nOSeq uint64, dateTimeSeq string, idxOffset uint64) error {
	c.opFinishRecMu.Lock()
	defer c.opFinishRecMu.Unlock()

	nOSeq, dateTimeSeq, consumedIdxNum := c.finishRec.getWaterMark()

	if consumedIdxNum >= idxOffset+1 {
		return nil
	}

	if err := c.finishRec.updateWaterMark(nOSeq, dateTimeSeq, idxOffset+1); err != nil {
		return err
	}

	return nil
}

func (c *Consumer) switchNextSeqFile() error {
	nextNOSeq, nextDateTimeSeq, err := scanDirToParseNextSeq(c.baseDir, c.finishRec.nOSeq)
	if err != nil {
		return err
	}

	c.idxFp, err = MakeSeqIdxFp(c.baseDir, nextDateTimeSeq, nextNOSeq, os.O_RDONLY)
	if err != nil {
		return err
	}

	c.dataFp, err = MakeSeqDataFp(c.baseDir, nextDateTimeSeq, nextNOSeq, os.O_RDONLY)
	if err != nil {
		return err
	}

	err = c.finishRec.updateWaterMark(nextNOSeq, nextDateTimeSeq, 0)
	if err != nil {
		return err
	}

	c.nextIdxCursor = 0
	if err = c.refreshMsgNum(); err != nil {
		return err
	}

	return nil
}
