package file

import (
	"context"
	"github.com/995933447/bucketmq/core/log"
	"github.com/995933447/bucketmq/core/msgstorage"
	"github.com/995933447/bucketmq/core/utils/fileutil"
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

type topicFileWritersWrapper struct {
	*indexFileWriter
	*dataFileWriter
	logger log.Logger
}

func (fw *topicFileWritersWrapper) syncToDisk(ctx context.Context) error {
	if err := fw.indexFileWriter.fp.Sync(); err != nil {
		fw.logger.Error(ctx, err)
		return err
	}
	if err := fw.dataFileWriter.fp.Sync(); err != nil {
		return err
	}
	return nil
}

func (fw *topicFileWritersWrapper) checkFilesCorruption(ctx context.Context) (bool, error) {
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

type topicMsgWriter struct {
	// 索引目录
	indexDir string
	// 数据目录
	dataDir string
	// topic名称
	topicName string
	// 消息文件序列号
	fileSeq uint32
	// 消息文件写入处理器
	fileWritersWrapper *topicFileWritersWrapper
	//　消息文件是否被污染
	hasFileCorruption bool
	//　进入准备停止事件循环状态,不再写入消息
	readyStopLoop bool
	//　日志组件
	logger log.Logger `access:"r"`
	// 接收消息的chan
	msgCh chan *msgstorage.Message
	//　接收停止时间循环的信号的chan
	stopLoopEventCh chan struct{}
	//　发送有新的写入文件被打开的通知的chan
	newFilesOpenEventCh chan *newFilesOpenEvent
	// 定时冲刷磁盘的时间间隔
	syncToDiskInterval time.Duration
	//　是否已经完成初始化
	finishInit bool
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
	if w.hasFileCorruption, err = w.fileWritersWrapper.checkFilesCorruption(ctx); err != nil {
		return false, err
	}
	return w.hasFileCorruption, nil
}

func (w *topicMsgWriter) makeIndexFp(ctx context.Context) (*os.File, error) {
	if err := fileutil.MkdirIfNotExist(w.indexDir); err != nil {
		w.logger.Error(ctx, err)
		return nil, err
	}

	fileSeqStr := strconv.FormatUint(uint64(w.fileSeq), 10)
	indexFileName := strings.TrimRight(w.indexDir, "/") + "/" + w.topicName + "." + fileSeqStr + "." + indexFileSuffixName
	fp, err := os.OpenFile(indexFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		return nil, err
	}
	fileInfo, err := fp.Stat()
	if err != nil {
		return nil, err
	}
	if fileInfo.IsDir() {
		err = errdef.FileIsNotRegularFileErr
		w.logger.Error(ctx, err)
		return nil, err
	}
	return fp, nil
}

func (w *topicMsgWriter) makeDataFp(ctx context.Context) (*os.File, error) {
	if err := fileutil.MkdirIfNotExist(w.dataDir); err != nil {
		w.logger.Error(ctx, err)
		return nil, err
	}

	fileSeqStr := strconv.FormatUint(uint64(w.fileSeq), 10)
	dataFileName := strings.TrimRight(w.dataDir, "/") + "/" + w.topicName + "." + fileSeqStr + "." + dataFileSuffixName
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

	w.fileWritersWrapper = &topicFileWritersWrapper{
		dataFileWriter: &dataFileWriter{
			maxWritableDataBytes: maxWritableMsgBytes,
			fp: dataFp,
		},
		indexFileWriter: &indexFileWriter{
			maxWritableIndexNum: maxWritableMsgNum,
			fp: indexFp,
		},
		logger: w.logger,
	}

	hasFileCorruption, err := w.checkFilesCorruption(ctx)
	if err != nil {
		return err
	} else if hasFileCorruption {
		return errdef.FileCorruptionErr
	}

	w.msgCh = make(chan *msgstorage.Message, 10000)
	w.stopLoopEventCh = make(chan struct{})
	w.newFilesOpenEventCh = make(chan *newFilesOpenEvent)
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
	w.fileWritersWrapper = &topicFileWritersWrapper{
		dataFileWriter: &dataFileWriter{
			maxWritableDataBytes: w.fileWritersWrapper.dataFileWriter.maxWritableDataBytes,
			fp: dataFp,
		},
		indexFileWriter: &indexFileWriter{
			maxWritableIndexNum: w.fileWritersWrapper.indexFileWriter.maxWritableIndexNum,
			fp: indexFp,
		},
		logger: w.logger,
	}
	w.newFilesOpenEventCh <- &newFilesOpenEvent{
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
			if w.fileWritersWrapper.indexFileWriter.writtenIndexNum + uint32(len(batchWritableMsgs)) >= w.fileWritersWrapper.indexFileWriter.maxWritableIndexNum ||
				w.fileWritersWrapper.dataFileWriter.writtenDataBytes + batchWritableMsgDataBufBytes >= w.fileWritersWrapper.dataFileWriter.maxWritableDataBytes {
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
				writtenNum, err := w.fileWritersWrapper.dataFileWriter.fp.Write(dataBuf)
				if err != nil {
					return err
				}

				totalWrittenNum += writtenNum
				if totalWrittenNum >= dataBufLen {
					break
				}

				dataBuf = dataBuf[writtenNum:]
			}
			w.fileWritersWrapper.dataFileWriter.writtenDataBytes += uint32(dataBufLen)

			totalWrittenNum = 0
			for {
				writtenNum, err := w.fileWritersWrapper.indexFileWriter.fp.Write(indexesBuf)
				if err != nil {
					return err
				}

				totalWrittenNum += writtenNum
				if totalWrittenNum >= indexesBufLen {
					break
				}

				indexesBuf = indexesBuf[writtenNum:]
			}
			w.fileWritersWrapper.indexFileWriter.writtenIndexNum += uint32(batchWritableMsgNum)

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

		if len(msgs) == 0 {
			return nil
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
				if w.readyStopLoop {
					continue
				}
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
					err = w.fileWritersWrapper.syncToDisk(ctx)
					w.logger.Warn(ctx, err.Error())
				}
			case <- w.stopLoopEventCh:
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
