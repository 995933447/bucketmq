package synclog

import (
	"encoding/binary"
	"errors"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/rpc/ha"
	"github.com/fsnotify/fsnotify"
	"github.com/golang/protobuf/proto"
	"io"
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

	idxNum := finishRec.getWaterMark()

	consumer.nextIdxCursor = idxNum

	consumer.idxFp, err = makeIdxFp(consumer.baseDir, os.O_CREATE|os.O_RDONLY)
	if err != nil {
		return nil, err
	}

	consumer.dataFp, err = makeDataFp(consumer.baseDir, os.O_CREATE|os.O_RDONLY)
	if err != nil {
		return nil, err
	}

	if err = consumer.refreshMsgNum(); err != nil {
		return nil, err
	}

	consumer.pendingRec, err = newConsumePendingRec(consumer.baseDir)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

type confirmMsgReq struct {
	idxOffset uint64
}

type Consumer struct {
	baseDir             string
	status              runState
	idxFp               *os.File
	dataFp              *os.File
	nextIdxCursor       uint64
	finishRec           *ConsumeWaterMarkRec
	opFinishRecMu       sync.RWMutex
	pendingRec          *ConsumePendingRec
	msgNum              uint64
	isWaitingMsgConfirm atomic.Value // bool
	unsubscribeSignCh   chan struct{}
	confirmMsgCh        chan *confirmMsgReq
}

func (c *Consumer) IsWaitingMsgConfirm() bool {
	isWaiting := c.isWaitingMsgConfirm.Load()
	if isWaiting != nil {
		return isWaiting.(bool)
	}
	return false
}

func (c *Consumer) subscribe() error {
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
	var needWatchNewFile bool
	for {
		needSwitchNewFile, err := c.finishRec.isFinished()
		if err != nil {
			util.Logger.Error(nil, err)
			time.Sleep(time.Millisecond * 500)
			continue
		}

		if needSwitchNewFile {
			_ = fileWatcher.Remove(c.idxFp.Name())
			for {
				if err = c.reOpenFile(); err != nil {
					util.Logger.Error(nil, err)
					time.Sleep(time.Millisecond * 500)
					continue
				}
				needWatchNewFile = true
				break
			}
		}

		if !c.IsWaitingMsgConfirm() && c.nextIdxCursor < c.msgNum {
			c.isWaitingMsgConfirm.Store(true)
			// do remote sync
			continue
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
			c.pendingRec.syncDisk()
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
		case confirmed := <-c.confirmMsgCh:
			util.Logger.Debug(nil, "rev confirmed")
			var confirmedList []*confirmMsgReq
			confirmedList = append(confirmedList, confirmed)
			for {
				select {
				case more := <-c.confirmMsgCh:
					confirmedList = append(confirmedList, more)
				default:
				}
				break
			}

			var (
				confirmedMax *confirmMsgReq
				unPends      []*pendingMsgIdx
			)
			for _, confirmed := range confirmedList {
				unPends = append(unPends, &pendingMsgIdx{
					idxOffset: confirmed.idxOffset,
				})

				if confirmedMax == nil {
					confirmedMax = confirmed
					continue
				}

				if confirmed.idxOffset > confirmedMax.idxOffset {
					confirmedMax = confirmed
				}
			}

			if notPending, err := c.unPendMsg(unPends); err != nil {
				util.Logger.Error(nil, err)
				break
			} else if !notPending {
				break
			}

			if err = c.updateFinishWaterMark(confirmedMax.idxOffset); err != nil {
				util.Logger.Error(nil, err)
				break
			}

			if confirmedMax.idxOffset < c.nextIdxCursor-1 {
				break
			}

			c.isWaitingMsgConfirm.Store(false)
		}
	}
out:
	c.finishRec.syncDisk()
	c.pendingRec.syncDisk()
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

type PoppedMsgItem struct {
	Topic     string
	Seq       uint64
	IdxOffset uint32
	Data      []byte
	CreatedAt int64
	RetryAt   uint32
	RetryCnt  uint32
}

func (c *Consumer) consumeBatch() ([]*ha.SyncMsgFileLogItem, bool, error) {
	if !c.isSubscribed() {
		return nil, false, errConsumerNotRunning
	}

	if c.msgNum <= c.nextIdxCursor {
		return nil, false, nil
	}

	var (
		items          []*ha.SyncMsgFileLogItem
		pendings       []*pendingMsgIdx
		totalDataBytes int
		bin            = binary.LittleEndian
	)
	for {
		if totalDataBytes > 2*1024*1024 {
			break
		}

		if c.msgNum <= c.nextIdxCursor {
			break
		}

		idxBuf := make([]byte, idxBytes)
		seekIdxBufOffset := c.nextIdxCursor * idxBytes
		var isEOF bool
		_, err := c.idxFp.ReadAt(idxBuf, int64(seekIdxBufOffset))
		if err != nil {
			if err != io.EOF {
				return nil, false, err
			}
			isEOF = true
		}

		if len(idxBuf) == 0 {
			break
		}

		boundaryBegin := bin.Uint16(idxBuf[:bufBoundaryBytes])
		boundaryEnd := bin.Uint16(idxBuf[idxBytes-bufBoundaryBytes:])
		if boundaryBegin != bufBoundaryBegin || boundaryEnd != bufBoundaryEnd {
			return nil, false, errFileCorrupted
		}

		offset := bin.Uint32(idxBuf[bufBoundaryBytes+4 : bufBoundaryBytes+8])
		dataBytes := bin.Uint32(idxBuf[bufBoundaryBytes+8 : bufBoundaryBytes+12])

		dataBuf := make([]byte, dataBytes)
		_, err = c.dataFp.ReadAt(dataBuf, int64(offset))
		if err != nil {
			if err != io.EOF {
				return nil, false, err
			}
			isEOF = true
		}

		if len(dataBuf) == 0 {
			break
		}

		boundaryBegin = bin.Uint16(dataBuf[:bufBoundaryBytes])
		boundaryEnd = bin.Uint16(dataBuf[dataBytes-bufBoundaryBytes:])
		if boundaryBegin != bufBoundaryBegin || boundaryEnd != bufBoundaryEnd {
			return nil, false, errFileCorrupted
		}

		data := dataBuf[bufBoundaryBytes : dataBytes-bufBoundaryBytes]

		var item ha.SyncMsgFileLogItem
		err = proto.Unmarshal(data, &item)
		if err != nil {
			return nil, false, err
		}

		if !c.pendingRec.isConfirmed(c.nextIdxCursor) {
			pendings = append(pendings, &pendingMsgIdx{
				idxOffset: c.nextIdxCursor,
			})

			items = append(items, &item)

			totalDataBytes += len(data)
		}

		if isEOF {
			break
		}

		c.nextIdxCursor++
	}

	if err := c.pendingRec.pending(pendings, false); err != nil {
		return nil, false, err
	}

	return items, true, nil
}

func (c *Consumer) unsubscribe() {
	c.unsubscribeSignCh <- struct{}{}
}

func (c *Consumer) isSubscribed() bool {
	return c.status == runStateRunning
}

func (c *Consumer) confirmMsg(idxOffset uint64) {
	if c.isSubscribed() {
		c.confirmMsgCh <- &confirmMsgReq{
			idxOffset: idxOffset,
		}
		return
	}

	if notPending, err := c.unPendMsg([]*pendingMsgIdx{{idxOffset: idxOffset}}); err != nil {
		util.Logger.Error(nil, err)
		return
	} else if !notPending {
		return
	}
	if err := c.updateFinishWaterMark(idxOffset); err != nil {
		util.Logger.Error(nil, err)
	}
}

func (c *Consumer) isNotConfirmed(idxOffset uint64) bool {
	if c.pendingRec.isEmpty() {
		return false
	}

	return c.pendingRec.isPending(idxOffset)
}

func (c *Consumer) unPendMsg(pendings []*pendingMsgIdx) (bool, error) {
	if err := c.pendingRec.unPend(pendings, false); err != nil {
		return false, err
	}

	if !c.pendingRec.isEmpty() {
		return false, nil
	}

	return true, nil
}

func (c *Consumer) updateFinishWaterMark(offset uint64) error {
	c.opFinishRecMu.Lock()
	defer c.opFinishRecMu.Unlock()

	consumedIdxNum := c.finishRec.getWaterMark()

	if consumedIdxNum >= offset+1 {
		return nil
	}

	if err := c.finishRec.updateWaterMark(offset + 1); err != nil {
		return err
	}

	return nil
}

func (c *Consumer) reOpenFile() error {
	var err error
	c.idxFp, err = makeIdxFp(c.baseDir, os.O_RDONLY)
	if err != nil {
		return err
	}

	c.dataFp, err = makeDataFp(c.baseDir, os.O_RDONLY)
	if err != nil {
		return err
	}

	c.finishRec, err = newConsumeWaterMarkRec(c.baseDir)
	if err != nil {
		return err
	}

	idxNum := c.finishRec.getWaterMark()

	c.nextIdxCursor = idxNum

	if err = c.refreshMsgNum(); err != nil {
		return err
	}

	return nil
}
