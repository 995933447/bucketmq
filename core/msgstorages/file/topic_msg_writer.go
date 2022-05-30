package file

import (
	"context"
	"github.com/995933447/bucketmq/core/log"
	"github.com/995933447/bucketmq/core/msgstorages"
	"github.com/995933447/bucketmq/core/utils/fileutil"
	errdef "github.com/995933447/bucketmqerrdef"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// 默认刷新文件消息数据到磁盘的时间间隔
	defaultSyncToDiskInterval = time.Second * 5
)

// topic消息文件处理器
type topicMultiFileHandler struct {
	// 索引文件写入处理器
	*indexFileWriter
	// 数据文件写入处理器
	*dataFileWriter
	// topic名称
	topicName string
	// 文件目录
	dir string
	// 消息文件序列号
	fileSeq string
	// 文件创建时间
	fileSeqCreatedAt uint32
	// 开始的消息位移
	firstMsgOffset uint64
	// 定时冲刷磁盘的时间间隔
	syncToDiskInterval time.Duration
	// 消息文件是否被污染
	hasFileCorruption bool
	// 消息编码器
	*msgEncoder
	// 发送有新的写入文件被打开的通知的chan
	nextSeqFilesOpenEventCh chan *nextSeqFilesOpenEvent
	// 是否初始化完成
	finishInit bool
}

// 构造函数
func newTopicMultiFileHandler(topicName, dir string, maxWritableMsgNum, maxWritableMsgDataBytes uint32, syncToDiskInterval time.Duration) *topicMultiFileHandler {
	return &topicMultiFileHandler{
		topicName: topicName,
		dir: dir,
		indexFileWriter: &indexFileWriter{
			maxWritableIndexNum: maxWritableMsgNum,
		},
		dataFileWriter: &dataFileWriter{
			maxWritableDataBytes: maxWritableMsgDataBytes,
		},
		syncToDiskInterval: syncToDiskInterval,
	}
}

// 文件写入处理器包装器初始化工作
func (fh *topicMultiFileHandler) init() error {
	if fh.finishInit {
		return nil
	}

	if err := fh.assetRequiredFieldsBeforeInit(); err != nil {
		return err
	}

	if err := fh.sureWritableFiles(); err != nil {
		return err
	}

	indexFp, err := fh.makeIndexFp()
	if err != nil {
		return err
	}
	indexFileInfo, err := indexFp.Stat()
	if err != nil {
		return err
	}
	writtenIndexBytes := indexFileInfo.Size()
	if writtenIndexBytes % indexBufSize > 0 {
		fh.hasFileCorruption = true
		return  errdef.FileCorruptionErr
	}

	dataFp, err := fh.makeDataFp()
	if err != nil {
		return err
	}
	dataFileInfo, err := dataFp.Stat()
	if err != nil {
		return err
	}

	fh.indexFileWriter.fp = indexFp
	fh.dataFileWriter.fp = dataFp
	fh.indexFileWriter.writtenIndexNum = uint32(writtenIndexBytes / indexBufSize)
	fh.writtenDataBytes = uint32(dataFileInfo.Size())
	fh.msgEncoder = &msgEncoder{}
	fh.nextSeqFilesOpenEventCh = make(chan *nextSeqFilesOpenEvent, 10000)
	fh.finishInit = true

	return nil
}

// 同步文件消息数据到磁盘
func (fh *topicMultiFileHandler) syncToDisk() error {
	if err := fh.indexFileWriter.fp.Sync(); err != nil {
		return err
	}
	if err := fh.dataFileWriter.fp.Sync(); err != nil {
		return err
	}
	return nil
}

// 检查文件是否被污染
func (fh *topicMultiFileHandler) checkForCorruptFiles() (bool, error) {
	indexFileInfo, err := fh.indexFileWriter.fp.Stat()
	if err != nil {
		return false, err
	}

	if indexFileInfo.Size() % indexBufSize > 0 || uint32(indexFileInfo.Size() / indexBufSize) != fh.indexFileWriter.writtenIndexNum {
		fh.hasFileCorruption = true
		return true, nil
	}

	dataFileInfo, err := fh.dataFileWriter.fp.Stat()
	if err != nil {
		return false, err
	}

	if uint32(dataFileInfo.Size()) != fh.dataFileWriter.writtenDataBytes {
		fh.hasFileCorruption = true
		return true, nil
	}

	fh.hasFileCorruption = false

	return false, nil
}

// 确定可写入文件序号
func (fh *topicMultiFileHandler) sureWritableFiles() error {
	if err := fileutil.MkdirIfNotExist(fh.dir); err != nil {
		return err
	}

	fileSeq, err := calMaxFileSeqFromDir(fh.dir, fh.topicName, indexFileSuffixName)
	if err != nil {
		if err != errdef.FileSeqNotFoundErr {
			return err
		}
		fileSeq = buildFileSeq(time.Now(), msgstorages.GlobalFirstMsgOffset)
	}

	fh.fileSeq = fileSeq

	if fh.fileSeqCreatedAt, fh.firstMsgOffset, err = parseCreatedAtAndFirstMsgOffsetFromSeq(fileSeq); err != nil {
		return err
	}

	indexFileName := buildIndexFileName(fh.topicName, fh.dir, indexFileSuffixName, fileSeq)
	indexFileInfo, err := os.Stat(indexFileName)
	if err == nil {
		indexFileSize := indexFileInfo.Size()
		if indexFileSize % indexBufSize > 0 {
			fh.hasFileCorruption = true
			return errdef.FileCorruptionErr
		}
		fh.writtenIndexNum = uint32(indexFileSize / indexFileSize)

		dataFileName := buildDataFileName(fh.topicName, fh.dir, dataFileSuffixName, fileSeq)
		dataFileInfo, err := os.Stat(dataFileName)
		if err != nil {
			return err
		}
		fh.dataFileWriter.writtenDataBytes = uint32(dataFileInfo.Size())

		if fh.writtenIndexNum >= fh.indexFileWriter.maxWritableIndexNum || fh.writtenDataBytes >= fh.maxWritableDataBytes {
			if err = fh.openNextSeqMsgFiles(); err != nil {
				return err
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	fh.fileSeq = fileSeq

	return nil
}

// 构造数据文件句柄
func (fh *topicMultiFileHandler) makeDataFp() (*os.File, error) {
	if err := fh.assetConfirmedWritableFiles(); err != nil {
		return nil, err
	}

	if err := fileutil.MkdirIfNotExist(fh.dir); err != nil {
		return nil, err
	}

	fileName := buildDataFileName(fh.topicName, fh.dir, dataFileSuffixName, fh.fileSeq)
	fp, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		return nil, err
	}

	return fp, nil
}

// 构造索引文件句柄
func (fh *topicMultiFileHandler) makeIndexFp() (*os.File, error) {
	if err := fh.assetConfirmedWritableFiles(); err != nil {
		return nil, err
	}

	if err := fileutil.MkdirIfNotExist(fh.dir); err != nil {
		return nil, err
	}

	fileName := buildIndexFileName(fh.topicName, fh.dir, indexFileSuffixName, fh.fileSeq)
	fp, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		return nil, err
	}

	fileInfo, err := fp.Stat()
	if err != nil {
		return nil, err
	}

	if fileInfo.IsDir() {
		return nil, errdef.FileIsNotRegularFileErr
	}

	return fp, nil
}

// 打开下个可写入消息文件序号
func (fh *topicMultiFileHandler) openNextSeqMsgFiles() error {
	firstMsgOffsetOfNewFileSeq := fh.firstMsgOffset + uint64(fh.writtenIndexNum)
	newFileSeqCreatedAt := time.Now()
	fh.fileSeq = buildFileSeq(newFileSeqCreatedAt, firstMsgOffsetOfNewFileSeq)
	fh.firstMsgOffset = firstMsgOffsetOfNewFileSeq
	fh.fileSeqCreatedAt = uint32(newFileSeqCreatedAt.Unix())

	indexFp, err := fh.makeIndexFp()
	if err != nil {
		return err
	}
	indexFileInfo, err := indexFp.Stat()
	if err != nil {
		return err
	}
	writtenIndexBytes := indexFileInfo.Size()
	if writtenIndexBytes % indexBufSize > 0 {
		fh.hasFileCorruption = true
		return errdef.FileCorruptionErr
	}

	dataFp, err := fh.makeDataFp()
	if err != nil {
		return err
	}
	dataFileInfo, err := dataFp.Stat()
	if err != nil {
		return err
	}

	fh.indexFileWriter.fp = indexFp
	fh.indexFileWriter.writtenIndexNum = uint32(writtenIndexBytes / indexBufSize)
	fh.dataFileWriter.fp = dataFp
	fh.dataFileWriter.writtenDataBytes = uint32(dataFileInfo.Size())

	if fh.nextSeqFilesOpenEventCh != nil {
		fh.nextSeqFilesOpenEventCh <- &nextSeqFilesOpenEvent{
			fileSeq: fh.fileSeq,
		}
	}

	return nil
}

// 检查初始化前结构体不可为空字段
func (fh *topicMultiFileHandler) assetRequiredFieldsBeforeInit() error {
	if fh.topicName == "" || fh.syncToDiskInterval <= 0 ||
		fh.indexFileWriter == nil || fh.dataFileWriter == nil ||
		fh.indexFileWriter.maxWritableIndexNum == 0 || fh.maxWritableDataBytes == 0 || fh.dir == "" {
		return errdef.MadeStructNotByNewFuncErr
	}

	return nil
}

// 检查结构体是否确认了可写入文件序号
func (fh *topicMultiFileHandler) assetConfirmedWritableFiles() error {
	if fh.fileSeq != "" || fh.fileSeqCreatedAt > 0 {
		return nil
	}

	return errdef.NewErr(errdef.ErrCodeAssetStructFailed, "must call *topicMsgWriter.ConfirmWritableFiles(context.Context) to confirm writable files.")
}

type WriteMsgReq struct {
	msg *msgstorages.Message
	writtenEventChan chan *WrittenMsgEvent
}

// topic消息写入处理器
type topicMsgWriter struct {
	// topic名称
	topicName string
	// 消息文件处理器
	multiFileHandler *topicMultiFileHandler
	//　进入准备停止事件循环状态,不再写入消息
	readyStopLoop atomic.Value
	// 日志组件
	logger log.Logger `access:"r"`
	// 接收消息的chan
	writeMsgReqChan chan *WriteMsgReq `access:"r"`
	// 接收停止时间循环的信号的chan
	stopLoopEventCh chan struct{}
	// 是否已经完成初始化
	finishInit bool
}

// 构造函数
func newTopicMsgWriter(
	topicName, baseDir string,
	maxWritableMsgNum, maxWritableMsgDataBytes uint32,
	syncToDiskInterval time.Duration,
	logger log.Logger,

	) (*topicMsgWriter, error) {
	if logger == nil {
		return nil, errdef.NewErr(errdef.ErrCodeArgsInvalid, "logger is nil")
	}

	if syncToDiskInterval == 0 {
		syncToDiskInterval = defaultSyncToDiskInterval
	}

	writer := &topicMsgWriter{
		topicName: topicName,
		logger: logger,
		multiFileHandler: newTopicMultiFileHandler(
			 topicName,
			 strings.TrimRight(baseDir, "/") + "/" + topicName,
			 maxWritableMsgNum,
			 maxWritableMsgDataBytes,
			 syncToDiskInterval,
		),
	}

	if err := writer.init(); err != nil {
		return nil, err
	}

	return writer, nil
}

// topic消息处理器初始化工作
func (w *topicMsgWriter) init() error {
	if w.finishInit {
		return nil
	}

	if err := w.assetRequiredFieldsBeforeInit(); err != nil {
		return err
	}

	if err := w.multiFileHandler.init(); err != nil {
		return err
	}

	w.writeMsgReqChan = make(chan *WriteMsgReq, 10000)
	w.stopLoopEventCh = make(chan struct{}, 10000)
	w.finishInit = true

	return nil
}

// 检查初始化前不可为空字段
func (w *topicMsgWriter) assetRequiredFieldsBeforeInit() error {
	if w.topicName == "" || w.logger == nil || w.multiFileHandler == nil {
		return errdef.MadeStructNotByNewFuncErr
	}
	return nil
}

func (w *topicMsgWriter) readyToStopLoop() {
	w.readyStopLoop.Store(true)
}

func (w *topicMsgWriter) isReadyStopLoop() bool {
	val := w.readyStopLoop.Load()
	if val == nil || !val.(bool) {
		return false
	}

	return true
}

func (w *topicMsgWriter) WriteReqChan() chan *WriteMsgReq {
	return w.writeMsgReqChan
}

// 设置日志组件
func (w *topicMsgWriter) setLogger(logger log.Logger) error {
	if logger == nil {
		err := errdef.NewErr(errdef.ErrCodeArgsInvalid, "expected " + reflect.TypeOf(logger).Name() + ", but nil")
		return err
	}
	w.logger = logger
	return nil
}

// 批量写入消息
func (w *topicMsgWriter) writeMsgs(msgs []*msgstorages.Message) error {
	if w.multiFileHandler.hasFileCorruption {
		err := errdef.FileCorruptionErr
		return err
	}

	for len(msgs) > 0 {
		var (
			batchWritableMsgs []*msgstorages.Message
			batchWritableMsgDataBufBytes uint32
			areMsgFilesFull bool
		)

		for _, msg := range msgs {
			msgBytes := w.multiFileHandler.msgEncoder.getMsgsDataBufBytes([]*msgstorages.Message{msg})
			batchWritableMsgDataBufBytes += msgBytes
			if w.multiFileHandler.indexFileWriter.writtenIndexNum + uint32(len(batchWritableMsgs)) >= w.multiFileHandler.indexFileWriter.maxWritableIndexNum ||
				w.multiFileHandler.dataFileWriter.writtenDataBytes + batchWritableMsgDataBufBytes >= w.multiFileHandler.dataFileWriter.maxWritableDataBytes {
				areMsgFilesFull = true
				batchWritableMsgDataBufBytes -= msgBytes
				break
			}

			batchWritableMsgs = append(batchWritableMsgs, msg)
		}

		batchWritableMsgNum := len(batchWritableMsgs)

		if batchWritableMsgNum > 0 {
			indexesBuf, dataBuf := w.multiFileHandler.msgEncoder.encodeBuf(batchWritableMsgs)

			var(
				indexesBufLen = len(indexesBuf)
				totalWrittenNum int
			)
			for {
				writtenNum, err := w.multiFileHandler.dataFileWriter.fp.Write(dataBuf)
				if err != nil {
					return err
				}

				totalWrittenNum += writtenNum
				if uint32(totalWrittenNum) >= batchWritableMsgDataBufBytes {
					break
				}

				dataBuf = dataBuf[writtenNum:]
			}
			w.multiFileHandler.dataFileWriter.writtenDataBytes += batchWritableMsgDataBufBytes

			totalWrittenNum = 0
			for {
				writtenNum, err := w.multiFileHandler.indexFileWriter.fp.Write(indexesBuf)
				if err != nil {
					return err
				}

				totalWrittenNum += writtenNum
				if totalWrittenNum >= indexesBufLen {
					break
				}

				indexesBuf = indexesBuf[writtenNum:]
			}
			w.multiFileHandler.indexFileWriter.writtenIndexNum += uint32(batchWritableMsgNum)

			if len(msgs) <= batchWritableMsgNum {
				msgs = nil
			} else {
				msgs = msgs[batchWritableMsgNum:]
			}
		}

		if !areMsgFilesFull {
			continue
		}

		if err := w.multiFileHandler.openNextSeqMsgFiles(); err != nil {
			return err
		}
	}

	return nil
}

// 事件循环
func (w *topicMsgWriter) loop(ctx context.Context) error {
	syncToDiskTick := time.NewTicker(w.multiFileHandler.syncToDiskInterval)
	defer syncToDiskTick.Stop()

	writeMsgsAsPossible := func(firstWriteReq *WriteMsgReq) error {
		type writtenMsgEventChWrapper struct {
			ch chan *WrittenMsgEvent
			msgOffset uint64
		}
		var (
			msgClones []*msgstorages.Message
			allocNextMsgOffset = w.multiFileHandler.firstMsgOffset + uint64(w.multiFileHandler.writtenIndexNum)
			writtenMsgEventChWrappers []*writtenMsgEventChWrapper
		)

		handleDataForReadyWrite := func(writeMsgReq *WriteMsgReq) {
			// 消息不一定写入成功所以这里分配的message offset最终不一定会生效,
			// 使用消息克隆体赋值message offset避免写入失败时影响外面传入的message值.
			msgClone := writeMsgReq.msg.DeepClone()
			msgClones = append(msgClones, msgClone)
			msgCloneMetadata := msgClone.GetMetadata()
			msgCloneMetadata.MsgOffset = allocNextMsgOffset
			if writeMsgReq.writtenEventChan != nil {
				writtenMsgEventChWrappers = append(writtenMsgEventChWrappers, &writtenMsgEventChWrapper{
					msgOffset: msgCloneMetadata.MsgOffset,
					ch: writeMsgReq.writtenEventChan,
				})
			}
			allocNextMsgOffset++
		}

		if firstWriteReq != nil {
			handleDataForReadyWrite(firstWriteReq)
		}

		for {
			var moreWriteMsgReq *WriteMsgReq
			select {
				case moreWriteMsgReq = <- w.writeMsgReqChan:
					if moreWriteMsgReq != nil {
						handleDataForReadyWrite(moreWriteMsgReq)
					}
				default:
			}
			if moreWriteMsgReq == nil {
				break
			}
		}

		if len(msgClones) == 0 {
			return nil
		}

		notifyWrittenMsgsResult := func(err error) {
			for _, wrapper := range writtenMsgEventChWrappers {
				var notifyResultEvent *WrittenMsgEvent
				if err != nil {
					notifyResultEvent = &WrittenMsgEvent{
						err: err,
					}
				} else {
					notifyResultEvent = &WrittenMsgEvent{
						msgOffset: wrapper.msgOffset,
					}
				}
				wrapper.ch <- notifyResultEvent
			}
		}

		err := w.writeMsgs(msgClones)
		if err != nil {
			go notifyWrittenMsgsResult(err)
			return err
		}

		go notifyWrittenMsgsResult(nil)

		return nil
	}

	for {
		select {
			case writeMsgReq := <- w.writeMsgReqChan:
				if w.isReadyStopLoop() {
					continue
				}
				err := writeMsgsAsPossible(writeMsgReq)
				if err != nil {
					return err
				}
			case <- syncToDiskTick.C:
				if hasFileCorruption, err := w.multiFileHandler.checkForCorruptFiles(); err != nil {
					return err
				} else if hasFileCorruption {
					return errdef.FileCorruptionErr
				} else if err = w.multiFileHandler.syncToDisk(); err != nil {
					w.logger.Warn(ctx, err)
				}
			case <- w.stopLoopEventCh:
				w.readyToStopLoop()
				err := writeMsgsAsPossible(nil)
				if err != nil {
					return err
				}
				return nil
		}
	}
}
