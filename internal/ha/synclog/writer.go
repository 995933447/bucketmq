package synclog

import (
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/rpc/ha"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Writer struct {
	baseDir          string
	output           *output
	msgChan          chan *ha.SyncMsgFileLogItem
	flushSignCh      chan struct{}
	flushWait        sync.WaitGroup
	status           runState
	unwatchCfgSignCh chan struct{}
	onFileCorrupted  func()
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
				util.Logger.Debug(nil, err)
				continue
			}

			// blessing for god, never been true
			if corrupted {
				continue
			}

			w.onFileCorrupted()
		case msg := <-w.msgChan:
			if err := w.doWriteMost([]*ha.SyncMsgFileLogItem{msg}); err != nil {
				util.Logger.Debug(nil, err)
			}
		case <-w.flushSignCh:
			if err := w.doWriteMost(nil); err != nil {
				util.Logger.Debug(nil, err)
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
func (w *Writer) doWriteMost(msgList []*ha.SyncMsgFileLogItem) error {
	var total int
	for _, msg := range msgList {
		total += len(msg.FileBuf)
	}
	for {
		select {
		case more := <-w.msgChan:
			msgList = append(msgList, more)
			total += len(more.FileBuf)
			if total >= 2*1024*1024 {
				goto doWrite
			}
		default:
			goto doWrite
		}
	}
doWrite:
	if err := w.doWrite(msgList); err != nil {
		return err
	}
	return nil
}

func (w *Writer) doWrite(msgList []*ha.SyncMsgFileLogItem) error {
	if w.output == nil {
		var err error
		w.output, err = newOutput(w)
		if err != nil {
			return err
		}
	}

	if err := w.output.write(msgList); err != nil {
		return err
	}

	return nil
}

func (w *Writer) Write(msg *ha.SyncMsgFileLogItem) {
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

func NewWriter(baseDir string) *Writer {
	writer := &Writer{
		baseDir:          strings.TrimRight(baseDir, string(filepath.Separator)),
		msgChan:          make(chan *ha.SyncMsgFileLogItem, 100000),
		flushSignCh:      make(chan struct{}),
		unwatchCfgSignCh: make(chan struct{}),
	}

	go func() {
		writer.loop()
	}()

	return writer
}
