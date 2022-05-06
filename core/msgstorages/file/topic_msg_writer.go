package file

import (
	"context"
	"github.com/995933447/bucketmq/core/log"
	"github.com/995933447/bucketmq/core/msgstorages"
	"github.com/995933447/bucketmq/core/utils/fileutil"
	errdef "github.com/995933447/bucketmqerrdef"
	"io/ioutil"
	"os"
	"reflect"
	"time"
)

const (
	//　默认刷新文件消息数据到磁盘的时间间隔
	defaultSyncToDiskInterval = time.Second * 5
)

// topic消息文件写入处理器包装器
type topicFileWritersWrapper struct {
	//　索引文件写入处理器
	*indexFileWriter
	//　数据文件写入处理器
	*dataFileWriter
	// topic名称
	topicName string
	// 消息文件序列号
	fileSeq uint32
	// 定时冲刷磁盘的时间间隔
	syncToDiskInterval time.Duration
	//　消息文件是否被污染
	hasFileCorruption bool
	// 消息编码器
	*msgEncoder
	//　发送有新的写入文件被打开的通知的chan
	nextSeqFilesOpenEventCh chan *nextSeqFilesOpenEvent
	//　日志
	logger log.Logger
	//　是否初始化完成
	finishInit bool
}

// 文件写入处理器包装器初始化工作
func (fw *topicFileWritersWrapper) init(ctx context.Context) error {
	if fw.finishInit {
		return nil
	}

	if err := fw.assetRequiredFieldsBeforeInit(ctx); err != nil {
		fw.logger.Error(ctx, err)
		return err
	}

	if err := fw.sureWritableFileSeq(ctx); err != nil {
		fw.logger.Error(ctx, err)
		return err
	}

	indexFp, err := fw.makeIndexFp(ctx)
	if err != nil {
		fw.logger.Error(ctx, err)
		return err
	}
	indexFileInfo, err := indexFp.Stat()
	if err != nil {
		fw.logger.Error(ctx, err)
		return err
	}
	writtenIndexBytes := indexFileInfo.Size()
	if writtenIndexBytes % indexBufSize > 0 {
		fw.hasFileCorruption = true
		err = errdef.FileCorruptionErr
		fw.logger.Error(ctx, err)
		return err
	}

	dataFp, err := fw.makeDataFp(ctx)
	if err != nil {
		fw.logger.Error(ctx, err)
		return err
	}
	dataFileInfo, err := dataFp.Stat()
	if err != nil {
		fw.logger.Error(ctx, err)
		return err
	}

	fw.indexFileWriter.fp = indexFp
	fw.dataFileWriter.fp = dataFp
	fw.writtenIndexNum = uint32(writtenIndexBytes / indexBufSize)
	fw.writtenDataBytes = uint32(dataFileInfo.Size())
	fw.msgEncoder = &msgEncoder{}
	fw.nextSeqFilesOpenEventCh = make(chan *nextSeqFilesOpenEvent, 10000)
	fw.finishInit = true

	return nil
}

// 同步文件消息数据到磁盘
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

//　检查文件是否被污染
func (fw *topicFileWritersWrapper) checkFilesForCorrupt(ctx context.Context) (bool, error) {
	indexFileInfo, err := fw.indexFileWriter.fp.Stat()
	if err != nil {
		fw.logger.Error(ctx, err)
		return false, err
	}

	if indexFileInfo.Size() % indexBufSize > 0 || uint32(indexFileInfo.Size() / indexBufSize) != fw.indexFileWriter.writtenIndexNum {
		fw.hasFileCorruption = true
		return true, nil
	}

	dataFileInfo, err := fw.dataFileWriter.fp.Stat()
	if err != nil {
		fw.logger.Error(ctx, err)
		return false, err
	}

	if uint32(dataFileInfo.Size()) != fw.dataFileWriter.writtenDataBytes {
		fw.hasFileCorruption = true
		return true, nil
	}

	fw.hasFileCorruption = false

	return false, nil
}

//　确定可写入文件序号
func (fw *topicFileWritersWrapper) sureWritableFileSeq(ctx context.Context) error {
	indexDir := fw.indexFileWriter.dir

	if err := fileutil.MkdirIfNotExist(indexDir); err != nil {
		fw.logger.Error(ctx, err)
		return err
	}

	indexFiles, err := ioutil.ReadDir(indexDir)
	if err != nil {
		fw.logger.Error(ctx, err)
		return err
	}

	var maxFileSeq uint32
	for _, file := range indexFiles {
		if file.IsDir() {
			continue
		}

		seq, err := fileutil.TrimPreSuffixToParseFileSeq(file.Name(), fw.topicName, indexFileSuffixName)
		if err != nil {
			if err == errdef.FileSeqNotFoundErr {
				continue
			}
			fw.logger.Error(ctx, err)
			return err
		}

		if maxFileSeq == 0 || maxFileSeq < seq {
			maxFileSeq = seq
		}
	}
	var writableFileSeq uint32
	if maxFileSeq > 0 {
		writableFileSeq = maxFileSeq
	} else {
		writableFileSeq = minFileSeq
	}

	indexFileName := fileutil.BuildIndexFileName(fw.topicName, indexDir, indexFileSuffixName, writableFileSeq)
	indexFileInfo, err := os.Stat(indexFileName)
	if err == nil {
		indexFileSize := indexFileInfo.Size()
		if indexFileSize % indexBufSize > 0 {
			fw.hasFileCorruption = true
			err = errdef.FileCorruptionErr
			fw.logger.Error(ctx, err)
			return err
		}

		dataDir := fw.dataFileWriter.dir
		dataFileName := fileutil.BuildDataFileName(fw.topicName, dataDir, dataFileSuffixName, writableFileSeq)
		dataFileInfo, err := os.Stat(dataFileName)
		if err != nil {
			fw.logger.Error(ctx, err)
			return err
		}

		if uint32(indexFileSize / indexBufSize) >= fw.maxWritableIndexNum || uint32(dataFileInfo.Size()) >= fw.maxWritableDataBytes {
			writableFileSeq++
		}
	} else if !os.IsNotExist(err) {
		fw.logger.Error(ctx, err)
		return err
	}

	fw.fileSeq = writableFileSeq

	return nil
}

//　构造数据文件句柄
func (fw *topicFileWritersWrapper) makeDataFp(ctx context.Context) (*os.File, error) {
	if err := fw.assetConfirmedWritableFileSeq(ctx); err != nil {
		fw.logger.Error(ctx, err)
		return nil, err
	}

	dataDir := fw.dataFileWriter.dir

	if err := fileutil.MkdirIfNotExist(dataDir); err != nil {
		fw.logger.Error(ctx, err)
		return nil, err
	}

	dataFileName := fileutil.BuildDataFileName(fw.topicName, dataDir, dataFileSuffixName, fw.fileSeq)
	dataFp, err := os.OpenFile(dataFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		return nil, err
	}
	return dataFp, nil
}

// 构造索引文件句柄
func (fw *topicFileWritersWrapper) makeIndexFp(ctx context.Context) (*os.File, error) {
	if err := fw.assetConfirmedWritableFileSeq(ctx); err != nil {
		fw.logger.Error(ctx, err)
		return nil, err
	}

	indexDir := fw.indexFileWriter.dir

	if err := fileutil.MkdirIfNotExist(indexDir); err != nil {
		fw.logger.Error(ctx, err)
		return nil, err
	}

	indexFileName := fileutil.BuildIndexFileName(fw.topicName, indexDir, indexFileSuffixName, fw.fileSeq)
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
		fw.logger.Error(ctx, err)
		return nil, err
	}
	return fp, nil
}

// 打开下个可写入消息文件序号
func (fw *topicFileWritersWrapper) openNextSeqMsgFiles(ctx context.Context) error {
	fw.fileSeq++

	indexFp, err := fw.makeIndexFp(ctx)
	if err != nil {
		return err
	}
	indexFileInfo, err := indexFp.Stat()
	if err != nil {
		fw.logger.Error(ctx, err)
		return err
	}
	writtenIndexBytes := indexFileInfo.Size()
	if writtenIndexBytes % indexBufSize > 0 {
		fw.hasFileCorruption = true
		err = errdef.FileCorruptionErr
		fw.logger.Error(ctx, err)
		return err
	}

	dataFp, err := fw.makeDataFp(ctx)
	if err != nil {
		fw.logger.Error(ctx, err)
		return err
	}
	dataFileInfo, err := dataFp.Stat()
	if err != nil {
		fw.logger.Error(ctx, err)
		return err
	}

	fw.indexFileWriter.fp = indexFp
	fw.indexFileWriter.writtenIndexNum = uint32(writtenIndexBytes / indexBufSize)
	fw.dataFileWriter.fp = dataFp
	fw.dataFileWriter.writtenDataBytes = uint32(dataFileInfo.Size())

	if fw.nextSeqFilesOpenEventCh != nil {
		fw.nextSeqFilesOpenEventCh <- &nextSeqFilesOpenEvent{
			fileSeq: fw.fileSeq,
		}
	}

	return nil
}

//　检查初始化前结构体不可为空字段
func (fw *topicFileWritersWrapper) assetRequiredFieldsBeforeInit(ctx context.Context) error {
	if fw.topicName == "" || fw.logger == nil || fw.syncToDiskInterval <= 0 ||
		fw.indexFileWriter == nil || fw.dataFileWriter == nil ||
		fw.maxWritableIndexNum == 0 || fw.maxWritableDataBytes == 0 ||
		fw.indexFileWriter.dir == ""  || fw.dataFileWriter.dir == ""  {
		var logger log.Logger
		if fw.logger == nil {
			logger = log.DefaultLogger
		} else {
			logger = fw.logger
		}
		err := errdef.MadeStructNotByNewFuncErr
		logger.Error(ctx, err)
		return err
	}

	return nil
}

//　检查结构体是否确认了可写入文件序号
func (fw *topicFileWritersWrapper) assetConfirmedWritableFileSeq(ctx context.Context) error {
	if fw.fileSeq > 0 {
		return nil
	}

	err := errdef.NewErr(errdef.ErrCodeAssetStructFailed, "must call *topicMsgWriter.ConfirmWritableFileSeq(context.Context) to set *consumerMsgReadWriter.fileSeq first.")
	fw.logger.Error(ctx, err)
	return err
}

//　topic消息写入处理器
type topicMsgWriter struct {
	// topic名称
	topicName string
	// 消息文件写入处理器包装器
	fileWritersWrapper *topicFileWritersWrapper
	//　进入准备停止事件循环状态,不再写入消息
	readyStopLoop bool
	//　日志组件
	logger log.Logger `access:"r"`
	// 接收消息的chan
	msgCh chan *msgstorages.Message
	//　接收停止时间循环的信号的chan
	stopLoopEventCh chan struct{}
	//　是否已经完成初始化
	finishInit bool
}

//　构造函数
func newTopicMsgWriter(
	ctx context.Context,
	topicName, indexDir, dataDir string,
	maxWritableMsgNum, maxWritableMsgDataBytes uint32,
	syncToDiskInterval time.Duration,
	logger log.Logger,
	) (*topicMsgWriter, error) {
	if syncToDiskInterval == 0 {
		syncToDiskInterval = defaultSyncToDiskInterval
	}

	writer := &topicMsgWriter{
		topicName: topicName,
		logger: logger,
		fileWritersWrapper: &topicFileWritersWrapper{
			topicName: topicName,
			indexFileWriter: &indexFileWriter{
				dir: indexDir,
				maxWritableIndexNum: maxWritableMsgNum,
			},
			dataFileWriter: &dataFileWriter{
				dir: dataDir,
				maxWritableDataBytes: maxWritableMsgDataBytes,
			},
			syncToDiskInterval: syncToDiskInterval,
			logger: logger,
		},
	}

	if err := writer.init(ctx); err != nil {
		logger.Error(ctx, err)
		return nil, err
	}

	return writer, nil
}

//　topic消息处理器初始化工作
func (w *topicMsgWriter) init(ctx context.Context) error {
	if w.finishInit {
		return nil
	}

	if err := w.assetRequiredFieldsBeforeInit(ctx); err != nil {
		w.logger.Error(ctx, err)
		return err
	}

	if err := w.fileWritersWrapper.init(ctx); err != nil {
		w.logger.Error(ctx, err)
		return err
	}

	w.msgCh = make(chan *msgstorages.Message, 10000)
	w.stopLoopEventCh = make(chan struct{}, 10000)
	w.finishInit = true

	return nil
}

//　检查初始化前不可为空字段
func (w *topicMsgWriter) assetRequiredFieldsBeforeInit(ctx context.Context) error {
	if w.topicName == "" || w.logger == nil || w.fileWritersWrapper == nil {
		var logger log.Logger
		if w.logger == nil {
			logger = log.DefaultLogger
		} else {
			logger = w.logger
		}
		err := errdef.MadeStructNotByNewFuncErr
		logger.Error(ctx, err)
		return err
	}
	return nil
}

//　设置日志组件
func (w *topicMsgWriter) setLogger(logger log.Logger) error {
	if logger == nil {
		err := errdef.NewErr(errdef.ErrCodeArgsInvalid, "expected " + reflect.TypeOf(logger).Name() + ", but nil")
		return err
	}
	w.logger = logger
	w.fileWritersWrapper.logger = logger
	return nil
}

//　批量写入消息
func (w *topicMsgWriter) writeMsgs(ctx context.Context, msgs []*msgstorages.Message) error {
	if w.fileWritersWrapper.hasFileCorruption {
		w.logger.Error(ctx, errdef.FileCorruptionErr)
		return errdef.FileCorruptionErr
	}

	for len(msgs) > 0 {
		var (
			batchWritableMsgs []*msgstorages.Message
			batchWritableMsgDataBufBytes uint32
			areMsgFilesFull bool
		)

		for _, msg := range msgs {
			msgBytes := w.fileWritersWrapper.msgEncoder.getMsgsDataBufBytes([]*msgstorages.Message{msg})
			batchWritableMsgDataBufBytes += msgBytes
			if w.fileWritersWrapper.indexFileWriter.writtenIndexNum + uint32(len(batchWritableMsgs)) >= w.fileWritersWrapper.indexFileWriter.maxWritableIndexNum ||
				w.fileWritersWrapper.dataFileWriter.writtenDataBytes + batchWritableMsgDataBufBytes >= w.fileWritersWrapper.dataFileWriter.maxWritableDataBytes {
				areMsgFilesFull = true
				batchWritableMsgDataBufBytes -= msgBytes
				break
			}

			batchWritableMsgs = append(batchWritableMsgs, msg)
		}

		batchWritableMsgNum := len(batchWritableMsgs)

		if batchWritableMsgNum > 0 {
			indexesBuf, dataBuf := w.fileWritersWrapper.msgEncoder.encodeBuf(batchWritableMsgs)

			var(
				indexesBufLen = len(indexesBuf)
				totalWrittenNum int
			)
			for {
				writtenNum, err := w.fileWritersWrapper.dataFileWriter.fp.Write(dataBuf)
				if err != nil {
					return err
				}

				totalWrittenNum += writtenNum
				if uint32(totalWrittenNum) >= batchWritableMsgDataBufBytes {
					break
				}

				dataBuf = dataBuf[writtenNum:]
			}
			w.fileWritersWrapper.dataFileWriter.writtenDataBytes += batchWritableMsgDataBufBytes

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

		if err := w.fileWritersWrapper.openNextSeqMsgFiles(ctx); err != nil {
			w.logger.Error(ctx, err)
			return err
		}
	}

	return nil
}

//　事件循环
func (w *topicMsgWriter) loop(ctx context.Context) error {
	syncToDiskTick := time.NewTicker(w.fileWritersWrapper.syncToDiskInterval)
	defer syncToDiskTick.Stop()

	writeMsgsAsPossible := func(firstMsg *msgstorages.Message) error {
		var msgs []*msgstorages.Message
		if firstMsg != nil {
			msgs = append(msgs, firstMsg)
		}
		for {
			var moreMsg *msgstorages.Message
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
				if hasFileCorruption, err = w.fileWritersWrapper.checkFilesForCorrupt(ctx); err != nil {
					return err
				} else if hasFileCorruption {
					err =  errdef.FileCorruptionErr
					w.logger.Error(ctx, err)
					return err
				} else {
					if err = w.fileWritersWrapper.syncToDisk(ctx); err != nil {
						w.logger.Warn(ctx, err)
					}
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
