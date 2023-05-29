package engine

import (
	"github.com/995933447/bucketmq/internal/util"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Msg struct {
	Topic      string
	Priority   uint8
	DelayMs    uint32
	RetryCnt   uint32
	BucketId   uint32
	Buf        []byte
	compressed []byte
	*WriteMsgResWait
}

type WriteMsgResWait struct {
	Err        error
	Wg         sync.WaitGroup
	IsFinished bool
}

type Writer struct {
	topic             string
	baseDir           string
	idxFileMaxItemNum uint32
	dataFileMaxBytes  uint32
	enabledCompress   atomic.Bool
	msgChan           chan *Msg
	unwatchCfgSignCh  chan struct{}
	flushSignCh       chan struct{}
	flushWait         sync.WaitGroup
	status            runState
	output            *output
	afterWriteChs     []chan uint64
}

func (w *Writer) loop() {
	syncDiskTk := time.NewTicker(5 * time.Second)
	checkCorruptTk := time.NewTicker(30 * time.Second)
	defer func() {
		syncDiskTk.Stop()
		checkCorruptTk.Stop()
	}()

	for {
		select {
		case <-syncDiskTk.C:
			if w.output != nil {
				w.output.syncDisk()
			}
		case <-checkCorruptTk.C:
			corrupted, err := w.output.isCorrupted()
			if err != nil {
				util.Logger.Error(nil, err)
				continue
			}

			// blessing for god, never been true
			if corrupted {
				continue
			}

			if err = w.output.openNewFile(); err != nil {
				w.output.idxFp = nil
				w.output.dataFp = nil
				util.Logger.Error(nil, err)
			}
		case msg := <-w.msgChan:
			if err := w.doWriteMost([]*Msg{msg}); err != nil {
				util.Logger.Error(nil, err)
			}
		case <-w.flushSignCh:
			if err := w.doWriteMost(nil); err != nil {
				util.Logger.Error(nil, err)
			}

			w.output.syncDisk()

			if w.IsRunning() {
				w.flushWait.Done()
				break
			}

			if w.IsExiting() {
				w.status = runStateExited
				w.output.close()
			} else {
				w.status = runStateStopped
			}

			w.unwatchCfgSignCh <- struct{}{}
			w.flushWait.Done()

			return
		}
	}
}

// write msg as many as we can
func (w *Writer) doWriteMost(msgList []*Msg) error {
	var total int
	for _, msg := range msgList {
		total += len(msg.Buf)
	}
	for {
		select {
		case more := <-w.msgChan:
			msgList = append(msgList, more)
			total += len(more.Buf)
			if total >= 2*1024*1024 {
				goto doWrite
			}
		default:
			goto doWrite
		}
	}
doWrite:
	if err := w.doWrite(msgList); err != nil {
		for _, msg := range msgList {
			if msg.WriteMsgResWait == nil {
				continue
			}
			if msg.IsFinished {
				continue
			}
			msg.IsFinished = true
			msg.Err = err
			msg.Wg.Done()
		}
		return err
	}
	return nil
}

func (w *Writer) doWrite(msgList []*Msg) error {
	if w.output == nil {
		newestSeq, err := scanDirToParseNewestSeq(w.baseDir, w.topic)
		if err != nil {
			return err
		}
		w.output, err = newOutput(w, newestSeq)
		if err != nil {
			return err
		}
	}

	if err := w.output.write(msgList); err != nil {
		return err
	}

	return nil
}

func (w *Writer) Write(msg *Msg) {
	w.msgChan <- msg
}

func (w *Writer) Flush() {
	w.flushWait.Add(1)
	w.flushSignCh <- struct{}{}
	w.flushWait.Wait()
}

func (w *Writer) Stop() {
	w.flushWait.Add(1)
	w.status = runStateStopping
	w.flushSignCh <- struct{}{}
	w.flushWait.Wait()
}

func (w *Writer) Exit() {
	w.flushWait.Add(1)
	w.status = runStateExiting
	w.flushSignCh <- struct{}{}
	w.flushWait.Wait()
}

func (w *Writer) IsStopping() bool {
	return w.status == runStateRunning
}

func (w *Writer) IsStopped() bool {
	return w.status == runStateStopped
}

func (w *Writer) IsExiting() bool {
	return w.status == runStateExiting
}

func (w *Writer) IsExited() bool {
	return w.status == runStateExited
}

func (w *Writer) IsRunning() bool {
	return w.status == runStateRunning
}

func (w *Writer) AddAfterWriteCh(afterWriteCh chan uint64) {
	w.afterWriteChs = append(w.afterWriteChs, afterWriteCh)
}

func NewWriter(cfg *WriterCfg) *Writer {
	writer := &Writer{
		topic:             cfg.Topic,
		baseDir:           strings.TrimRight(cfg.BaseDir, string(filepath.Separator)),
		msgChan:           make(chan *Msg, 100000),
		flushSignCh:       make(chan struct{}),
		unwatchCfgSignCh:  make(chan struct{}),
		idxFileMaxItemNum: cfg.IdxFileMaxItemNum,
		dataFileMaxBytes:  cfg.DataFileMaxBytes,
	}

	go func() {
		writer.loop()
	}()

	return writer
}
