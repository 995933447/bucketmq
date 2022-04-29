package file

import (
	"context"
	"github.com/995933447/bucketmq/core/log"
	"github.com/995933447/bucketmq/core/msgstorage"
	errdef "github.com/995933447/bucketmqerrdef"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	defaultSyncToDiskInterval = time.Second * 5
)

type indexFileWriter struct {
	fp *os.File
	maxWritableIndexNum uint32
	writtenIndexNum uint32
}

type dataFileWriter struct {
	fp *os.File
	maxWritableDataBytes uint32
	writtenDataBytes uint32
}

type filesWriter struct {
	*topicMsgWriter

	*indexFileWriter
	*dataFileWriter
}

func (fw *filesWriter) syncToDisk(ctx context.Context) error {
	if err := fw.indexFileWriter.fp.Sync(); err != nil {
		fw.logger.Error(ctx, err)
		return err
	}
	if err := fw.dataFileWriter.fp.Sync(); err != nil {
		return err
	}
	return nil
}

func (fw *filesWriter) checkFilesCorruption(ctx context.Context) (bool, error) {
	indexFileInfo, err := fw.indexFileWriter.fp.Stat()
	if err != nil {
		fw.logger.Error(ctx, err)
		return false, err
	}

	if indexFileInfo.Size() % indexBufSize > 0 || uint32(indexFileInfo.Size() / indexBufSize) != fw.indexFileWriter.writtenIndexNum {
		return true, nil
	}

	dataFileInfo, err := fw.dataFileWriter.fp.Stat()
	if err != nil {
		fw.logger.Error(ctx, err)
		return false, err
	}

	if uint32(dataFileInfo.Size()) != fw.dataFileWriter.writtenDataBytes {
		return true, nil
	}

	return false, nil
}

type NewFilesOpenedSignal struct {
	fileSeq uint32 `access:"r"`
}

func (s *NewFilesOpenedSignal) getFileSeq() uint32 {
	return s.fileSeq
}

type topicMsgWriter struct {
	indexDir string
	dataDir string
	topicName string
	fileSeq uint32
	*filesWriter
	msgCh chan *msgstorage.Message
	stopLoopCh chan struct{}
	notifyNewFilesOpenedCh chan *NewFilesOpenedSignal
	syncToDiskInterval time.Duration
	hasFileCorruption bool
	finishInit bool
	logger log.Logger `access:"r"`
	readyStopLoop bool
}

func newTopicMsgWriter(
	ctx context.Context,
	topicName, indexDir, dataDir string,
	beginFileSeq, maxWritableMsgNum, maxWritableMsgBytes uint32,
	syncToDiskInterval time.Duration,
	logger log.Logger,
	) (*topicMsgWriter, error) {
	if syncToDiskInterval == 0 {
		syncToDiskInterval = defaultSyncToDiskInterval
	}

	writer := &topicMsgWriter{
		indexDir: indexDir,
		dataDir: dataDir,
		topicName: topicName,
		fileSeq: beginFileSeq,
		syncToDiskInterval: syncToDiskInterval,
		logger: logger,
	}

	if err := writer.init(ctx, maxWritableMsgNum, maxWritableMsgBytes); err != nil {
		logger.Error(ctx, err)
		return nil, err
	}

	return writer, nil
}

func (w *topicMsgWriter) getLogger() log.Logger {
	return w.logger
}

func (w *topicMsgWriter) setLogger(logger log.Logger) error {
	if logger == nil {
		err := errdef.NewErr(errdef.ErrCodeArgsInvalid, "expected " + reflect.TypeOf(logger).Name() + ", but nil")
		return err
	}
	w.logger = logger
	return nil
}

func (w *topicMsgWriter) checkFilesCorruption(ctx context.Context) (bool, error) {
	var err error
	if w.hasFileCorruption, err = w.filesWriter.checkFilesCorruption(ctx); err != nil {
		return false, err
	}
	return w.hasFileCorruption, nil
}

func (w *topicMsgWriter) makeIndexFp(ctx context.Context) (*os.File, error) {
	_, err := os.Stat(w.indexDir)
	if err != nil {
		if !os.IsNotExist(err) {
			w.logger.Error(ctx, err)
			return nil, err
		}
		if err = os.MkdirAll(w.indexDir, os.FileMode(0755)); err != nil {
			return nil, err
		}
	}

	fileSeqStr := strconv.FormatUint(uint64(w.fileSeq), 10)
	indexFileName := strings.TrimRight(w.indexDir, "/") + "/" + w.topicName + "." + fileSeqStr + ".idx"
	indexFp, err := os.OpenFile(indexFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		return nil, err
	}
	return indexFp, nil
}

func (w *topicMsgWriter) makeDataFp(ctx context.Context) (*os.File, error) {
	_, err := os.Stat(w.dataDir)
	if err != nil {
		if !os.IsNotExist(err) {
			w.logger.Error(ctx, err)
			return nil, err
		}
		if err = os.MkdirAll(w.dataDir, os.FileMode(0755)); err != nil {
			return nil, err
		}
	}

	fileSeqStr := strconv.FormatUint(uint64(w.fileSeq), 10)
	dataFileName := strings.TrimRight(w.dataDir, "/") + "/" + w.topicName + "." + fileSeqStr + ".dat"
	dataFp, err := os.OpenFile(dataFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		return nil, err
	}
	return dataFp, nil
}

func (w *topicMsgWriter) init(ctx context.Context, maxWritableMsgNum, maxWritableMsgBytes uint32) error {
	if w.finishInit {
		return nil
	}
	
	dataFp, err := w.makeDataFp(ctx)
	if err != nil {
		return err
	}

	indexFp, err := w.makeIndexFp(ctx)
	if err != nil {
		return err
	}

	w.filesWriter = &filesWriter{
		dataFileWriter: &dataFileWriter{
			maxWritableDataBytes: maxWritableMsgBytes,
			fp: dataFp,
		},
		indexFileWriter: &indexFileWriter{
			maxWritableIndexNum: maxWritableMsgNum,
			fp: indexFp,
		},
		topicMsgWriter: w,
	}

	hasFileCorruption, err := w.checkFilesCorruption(ctx)
	if err != nil {
		return err
	} else if hasFileCorruption {
		return errdef.FileCorruptionErr
	}

	w.msgCh = make(chan *msgstorage.Message, 10000)
	w.stopLoopCh = make(chan struct{})
	w.notifyNewFilesOpenedCh = make(chan *NewFilesOpenedSignal)
	w.finishInit = true

	return nil
}

func (w *topicMsgWriter) openNewMsgFiles(ctx context.Context) error {
	w.fileSeq++

	dataFp, err := w.makeDataFp(ctx)
	if err != nil {
		return err
	}

	indexFp, err := w.makeIndexFp(ctx)
	if err != nil {
		return err
	}
	w.filesWriter = &filesWriter{
		dataFileWriter: &dataFileWriter{
			maxWritableDataBytes: w.maxWritableDataBytes,
			fp: dataFp,
		},
		indexFileWriter: &indexFileWriter{
			maxWritableIndexNum: w.maxWritableIndexNum,
			fp: indexFp,
		},
		topicMsgWriter: w,
	}
	w.notifyNewFilesOpenedCh <- &NewFilesOpenedSignal{
		fileSeq: w.fileSeq,
	}
	
	return nil
}

func (w *topicMsgWriter) writeMsgs(ctx context.Context, msgs []*msgstorage.Message) error {
	if w.hasFileCorruption {
		w.logger.Error(ctx, errdef.FileCorruptionErr)
		return errdef.FileCorruptionErr
	}

	for len(msgs) > 0 {
		var (
			batchWritableMsgs []*msgstorage.Message
			batchWritableMsgDataBufBytes uint32
			areMsgFilesFull bool
		)

		for _, msg := range msgs {
			batchWritableMsgDataBufBytes += getDefaultMsgEncoder().getMsgsDataBufBytes([]*msgstorage.Message{msg})
			if w.indexFileWriter.writtenIndexNum + uint32(len(batchWritableMsgs)) >= w.indexFileWriter.maxWritableIndexNum ||
				w.dataFileWriter.writtenDataBytes + batchWritableMsgDataBufBytes >= w.dataFileWriter.maxWritableDataBytes {
				areMsgFilesFull = true
				break
			}

			batchWritableMsgs = append(batchWritableMsgs, msg)
		}

		batchWritableMsgNum := len(batchWritableMsgs)

		if batchWritableMsgNum > 0 {
			indexesBuf, dataBuf := getDefaultMsgEncoder().encodeBuf(msgs)

			var(
				dataBufLen = len(dataBuf)
				indexesBufLen = len(indexesBuf)
				totalWrittenNum int
			)
			for {
				writtenNum, err := w.dataFileWriter.fp.Write(dataBuf)
				if err != nil {
					return err
				}

				totalWrittenNum += writtenNum
				if totalWrittenNum >= dataBufLen {
					break
				}

				dataBuf = dataBuf[writtenNum:]
			}
			w.dataFileWriter.writtenDataBytes += uint32(dataBufLen)

			totalWrittenNum = 0
			for {
				writtenNum, err := w.indexFileWriter.fp.Write(indexesBuf)
				if err != nil {
					return err
				}

				totalWrittenNum += writtenNum
				if totalWrittenNum >= indexesBufLen {
					break
				}

				indexesBuf = indexesBuf[writtenNum:]
			}
			w.indexFileWriter.writtenIndexNum += uint32(batchWritableMsgNum)

			if len(msgs) <= batchWritableMsgNum {
				msgs = nil
			} else {
				msgs = msgs[batchWritableMsgNum:]
			}
		}

		if !areMsgFilesFull {
			continue
		}

		if err := w.openNewMsgFiles(ctx); err != nil {
			w.logger.Error(ctx, err)
			return err
		}
	}

	return nil
}

func (w *topicMsgWriter) loop(ctx context.Context) error {
	syncToDiskTick := time.NewTicker(w.syncToDiskInterval)
	defer syncToDiskTick.Stop()

	writeMsgsAsPossible := func(firstMsg *msgstorage.Message) error {
		var msgs []*msgstorage.Message
		if firstMsg != nil {
			msgs = append(msgs, firstMsg)
		}
		for {
			var moreMsg *msgstorage.Message
			select {
				case moreMsg = <- w.msgCh:
					msgs = append(msgs, moreMsg)
				default:
			}
			if moreMsg == nil {
				break
			}
		}

		err := w.writeMsgs(ctx, msgs)
		if err != nil {
			w.logger.Error(ctx, err)
			return err
		}

		return nil
	}

	for {
		select {
			case msg := <- w.msgCh:
				err := writeMsgsAsPossible(msg)
				if err != nil {
					w.logger.Error(ctx, err)
					return err
				}
			case <- syncToDiskTick.C:
				var (
					hasFileCorruption bool
					err error
				)
				if hasFileCorruption, err = w.checkFilesCorruption(ctx); err != nil {
					return err
				} else if hasFileCorruption {
					w.logger.Error(ctx, errdef.FileCorruptionErr)
					return errdef.FileCorruptionErr
				} else {
					_ =	w.syncToDisk(ctx)
				}
			case <- w.stopLoopCh:
				w.readyStopLoop = true
				err := writeMsgsAsPossible(nil)
				if err != nil {
					w.logger.Error(ctx, err)
					return err
				}
				return nil
		}
	}
}
