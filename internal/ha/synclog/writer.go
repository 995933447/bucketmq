package synclog

import (
	nodegrpha "github.com/995933447/bucketmq/internal/ha"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/rpc/ha"
	"sync"
	"time"
)

type Msg struct {
	logItem    *ha.SyncMsgFileLogItem
	resWait    sync.WaitGroup
	isFinished bool
	err        error
}

type Writer struct {
	*Sync
	output                        *output
	msgChan                       chan *Msg
	flushSignCh                   chan struct{}
	flushWait                     sync.WaitGroup
	status                        runState
	isRealTimeBackupMasterLogMeta bool
}

func (w *Writer) loop() {
	syncDiskTk := time.NewTicker(5 * time.Second)
	checkCorruptTk := time.NewTicker(30 * time.Second)
	remoteSyncMaxLogIdDuration := time.Second
	if w.isRealTimeBackupMasterLogMeta {
		remoteSyncMaxLogIdDuration = time.Second * 15
	}
	remoteBackupMasterLogMetaTK := time.NewTicker(remoteSyncMaxLogIdDuration)
	defer func() {
		syncDiskTk.Stop()
		checkCorruptTk.Stop()
		remoteBackupMasterLogMetaTK.Stop()
	}()

	for {
		select {
		case <-remoteBackupMasterLogMetaTK.C:
			if !nodegrpha.IsMasterRole() {
				return
			}
			if err := w.BackupMasterLogMeta(); err != nil {
				util.Logger.Error(nil, err)
			}
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

			panic(errFileCorrupted)
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
				w.flushWait.Done()
				return
			}

			w.status = runStateStopped
			w.msgChan = nil
		}
	}
}

func (w *Writer) Resume() {
	if w.status != runStateStopped {
		return
	}
	w.status = runStateRunning
	w.msgChan = make(chan *Msg)
}

// write msg as more we can
func (w *Writer) doWriteMost(msgList []*Msg) error {
	var total int
	for _, msg := range msgList {
		total += len(msg.logItem.FileBuf)
	}
	for {
		select {
		case more := <-w.msgChan:
			msgList = append(msgList, more)
			total += len(more.logItem.FileBuf)
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

func (w *Writer) doWrite(msgList []*Msg) error {
	if w.output == nil {
		var err error
		w.output, err = newOutput(w)
		if err != nil {
			return err
		}
	}

	if err := w.output.write(msgList); err != nil {
		for _, msg := range msgList {
			if !msg.isFinished {
				continue
			}
			msg.err = err
			msg.resWait.Done()
		}

		return err
	}

	return nil
}

func (w *Writer) Write(logItem *ha.SyncMsgFileLogItem) error {
	msg := &Msg{
		logItem: logItem,
	}
	msg.resWait.Add(1)
	w.msgChan <- msg
	msg.resWait.Wait()
	return msg.err
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

func NewWriter(sync *Sync) (*Writer, error) {
	writer := &Writer{
		Sync:        sync,
		msgChan:     make(chan *Msg, 100000),
		flushSignCh: make(chan struct{}),
	}

	var err error
	writer.etcdCli, err = util.GetOrNewEtcdCli()
	if err != nil {
		return nil, err
	}

	go func() {
		writer.loop()
	}()

	return writer, nil
}
