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

type topicSegFileGroupMsgWriter struct {
	// 索引文件写入处理器
	*indexFileWriter
	// 数据文件写入处理器
	*dataFileWriter
	// 消息文件序列号
	fileSeq string
	// 文件创建时间
	fileSeqCreatedAt uint32
	// 开始的消息位移
	firstMsgOffset uint64
	// 发送有新的写入文件被打开的通知的chan
	nextSeqFilesOpenEventCh chan *nextSeqFilesOpenEvent
	// 多联的多文件处理器
	multiFileHandler *topicMultiFileHandler
	// 是否已经完成初始化
	finishInit bool
}

func newTopicSegFileGroupMsgWriter(maxWritableMsgNum, maxWritableMsgDataBytes uint32, multiFileHandler *topicMultiFileHandler) (*topicSegFileGroupMsgWriter, error) {
	writer := &topicSegFileGroupMsgWriter{
		indexFileWriter: &indexFileWriter{
			maxWritableIndexNum: maxWritableMsgNum,
		},
		dataFileWriter: &dataFileWriter{
			maxWritableDataBytes: maxWritableMsgDataBytes,
		},
		multiFileHandler: multiFileHandler,
	}

	if err := writer.init(); err != nil {
		return nil, err
	}

	return writer, nil
}

func (gw *topicSegFileGroupMsgWriter) init() error {
	if err := gw.assetRequiredFieldsBeforeInit(); err != nil {
		return err
	}

	if err := gw.sureWritableFiles(); err != nil {
		return err
	}

	indexFp, err := gw.newIndexFp()
	if err != nil {
		return err
	}
	indexFileInfo, err := indexFp.Stat()
	if err != nil {
		return err
	}
	writtenIndexBytes := indexFileInfo.Size()
	if writtenIndexBytes % indexBufSize > 0 {
		gw.multiFileHandler.hasFileCorruption = true
		return errdef.FileCorruptionErr
	}

	dataFp, err := gw.newDataFp()
	if err != nil {
		return err
	}
	dataFileInfo, err := dataFp.Stat()
	if err != nil {
		return err
	}

	gw.indexFileWriter.fp = indexFp
	gw.dataFileWriter.fp = dataFp
	gw.indexFileWriter.writtenIndexNum = uint32(writtenIndexBytes / indexBufSize)
	gw.writtenDataBytes = uint32(dataFileInfo.Size())
	gw.nextSeqFilesOpenEventCh = make(chan *nextSeqFilesOpenEvent, 10000)
	gw.finishInit = true

	return nil
}

// 检查初始化前结构体不可为空字段
func (gw *topicSegFileGroupMsgWriter) assetRequiredFieldsBeforeInit() error {
	if gw.maxWritableIndexNum == 0 || gw.maxWritableDataBytes == 0 || gw.multiFileHandler == nil {
		return errdef.MadeStructNotByNewFuncErr
	}

	return nil
}

func (gw *topicSegFileGroupMsgWriter) syncToDisk() error {
	if err := gw.indexFileWriter.fp.Sync(); err != nil {
		return err
	}
	if err := gw.dataFileWriter.fp.Sync(); err != nil {
		return err
	}
	return nil
}

// 确定可写入文件序号
func (gw *topicSegFileGroupMsgWriter) sureWritableFiles() error {
	if err := fileutil.MkdirIfNotExist(gw.multiFileHandler.dir); err != nil {
		return err
	}

	fileSeq, err := calMaxFileSeqFromDir(gw.multiFileHandler.dir, gw.multiFileHandler.topicName, indexFileSuffixName)
	if err != nil {
		if err != errdef.FileSeqNotFoundErr {
			return err
		}
		fileSeq = buildFileSeq(time.Now(), msgstorages.GlobalFirstMsgOffset)
	}

	gw.fileSeq = fileSeq

	if gw.fileSeqCreatedAt, gw.firstMsgOffset, err = parseCreatedAtAndFirstMsgOffsetFromSeq(fileSeq); err != nil {
		return err
	}

	indexFileName := buildIndexFileName(gw.multiFileHandler.topicName, gw.multiFileHandler.dir, indexFileSuffixName, fileSeq)
	indexFileInfo, err := os.Stat(indexFileName)
	if err == nil {
		indexFileSize := indexFileInfo.Size()
		if indexFileSize % indexBufSize > 0 {
			gw.multiFileHandler.hasFileCorruption = true
			return errdef.FileCorruptionErr
		}
		gw.writtenIndexNum = uint32(indexFileSize / indexBufSize)

		dataFileName := buildDataFileName(gw.multiFileHandler.topicName, gw.multiFileHandler.dir, dataFileSuffixName, fileSeq)
		dataFileInfo, err := os.Stat(dataFileName)
		if err != nil {
			return err
		}

		gw.dataFileWriter.writtenDataBytes = uint32(dataFileInfo.Size())
		if gw.writtenIndexNum >= gw.indexFileWriter.maxWritableIndexNum ||
			gw.writtenDataBytes >= gw.maxWritableDataBytes {
			if err = gw.openNextSeqMsgFiles(); err != nil {
				return err
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	return nil
}

// 打开下个可写入消息文件序号
func (gw *topicSegFileGroupMsgWriter) openNextSeqMsgFiles() error {
	firstMsgOffsetOfNewFileSeq := gw.firstMsgOffset + uint64(gw.writtenIndexNum)
	newFileSeqCreatedAt := time.Now()
	gw.fileSeq = buildFileSeq(newFileSeqCreatedAt, firstMsgOffsetOfNewFileSeq)
	gw.firstMsgOffset = firstMsgOffsetOfNewFileSeq
	gw.fileSeqCreatedAt = uint32(newFileSeqCreatedAt.Unix())

	indexFp, err := gw.newIndexFp()
	if err != nil {
		return err
	}
	indexFileInfo, err := indexFp.Stat()
	if err != nil {
		return err
	}
	writtenIndexBytes := indexFileInfo.Size()
	if writtenIndexBytes % indexBufSize > 0 {
		gw.multiFileHandler.hasFileCorruption = true
		return errdef.FileCorruptionErr
	}

	dataFp, err := gw.newDataFp()
	if err != nil {
		return err
	}
	dataFileInfo, err := dataFp.Stat()
	if err != nil {
		return err
	}

	gw.indexFileWriter.fp = indexFp
	gw.writtenIndexNum = uint32(writtenIndexBytes / indexBufSize)
	gw.dataFileWriter.fp = dataFp
	gw.writtenDataBytes = uint32(dataFileInfo.Size())

	if gw.nextSeqFilesOpenEventCh != nil {
		gw.nextSeqFilesOpenEventCh <- &nextSeqFilesOpenEvent{
			fileSeq: gw.fileSeq,
		}
	}

	return nil
}

// 构造数据文件句柄
func (gw *topicSegFileGroupMsgWriter) newDataFp() (*os.File, error) {
	if err := gw.assetConfirmedWritableFiles(); err != nil {
		return nil, err
	}

	if err := fileutil.MkdirIfNotExist(gw.multiFileHandler.dir); err != nil {
		return nil, err
	}

	fileName := buildDataFileName(gw.multiFileHandler.topicName, gw.multiFileHandler.dir, dataFileSuffixName, gw.fileSeq)
	fp, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		return nil, err
	}

	return fp, nil
}

// 构造索引文件句柄
func (gw *topicSegFileGroupMsgWriter) newIndexFp() (*os.File, error) {
	if err := gw.assetConfirmedWritableFiles(); err != nil {
		return nil, err
	}

	if err := fileutil.MkdirIfNotExist(gw.multiFileHandler.dir); err != nil {
		return nil, err
	}

	fileName := buildIndexFileName(gw.multiFileHandler.topicName, gw.multiFileHandler.dir, indexFileSuffixName, gw.fileSeq)

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

// 检查文件是否被污染
func (gw *topicSegFileGroupMsgWriter) checkForCorruptFiles() (bool, error) {
	indexFileInfo, err := gw.indexFileWriter.fp.Stat()
	if err != nil {
		return false, err
	}

	if indexFileInfo.Size()%indexBufSize > 0 ||
		uint32(indexFileInfo.Size() / indexBufSize) != gw.indexFileWriter.writtenIndexNum {
		gw.multiFileHandler.hasFileCorruption = true
		return true, nil
	}

	dataFileInfo, err := gw.dataFileWriter.fp.Stat()
	if err != nil {
		return false, err
	}

	if uint32(dataFileInfo.Size()) != gw.dataFileWriter.writtenDataBytes {
		gw.multiFileHandler.hasFileCorruption = true
		return true, nil
	}

	gw.multiFileHandler.hasFileCorruption = false

	return false, nil
}

// 检查结构体是否确认了可写入文件序号
func (gw *topicSegFileGroupMsgWriter) assetConfirmedWritableFiles() error {
	if gw.fileSeq != "" || gw.fileSeqCreatedAt > 0 {
		return nil
	}

	return errdef.NewErr(errdef.ErrCodeAssetStructFailed, "must call *consumerSegFileGroupMsgLoader.ConfirmWritableFiles(context.Context) to confirm writable files.")
}

// topic消息文件处理器
type topicMultiFileHandler struct {
	// topic名称
	topicName string
	// 文件目录
	dir string
	// 消息分片文件组写入器
	segFileGroupMsgWriter *topicSegFileGroupMsgWriter
	// 定时冲刷磁盘的时间间隔
	syncToDiskInterval time.Duration
	// 消息文件是否被污染
	hasFileCorruption bool
	// 消息编码器
	*msgBufEncoder
	// 是否初始化完成
	finishInit bool
}

// 构造函数
func newTopicMultiFileHandler(topicName, dir string, maxWritableMsgNum, maxWritableMsgDataBytes uint32, syncToDiskInterval time.Duration) (*topicMultiFileHandler, error) {
	var(
		handler = &topicMultiFileHandler{
			topicName: topicName,
			dir:       dir,
			syncToDiskInterval: syncToDiskInterval,
		}
		err error
	)

	handler.segFileGroupMsgWriter, err = newTopicSegFileGroupMsgWriter(maxWritableMsgNum, maxWritableMsgDataBytes, handler)
	if err != nil {
		return nil, err
	}

	if err = handler.init(); err != nil {
		return nil, err
	}

	return handler, nil
}

// 文件写入处理器包装器初始化工作
func (fh *topicMultiFileHandler) init() error {
	if fh.finishInit {
		return nil
	}

	if err := fh.assetRequiredFieldsBeforeInit(); err != nil {
		return err
	}

	fh.msgBufEncoder = &msgBufEncoder{}
	fh.finishInit = true

	return nil
}

// 同步文件消息数据到磁盘
func (fh *topicMultiFileHandler) syncToDisk() error {
	if err := fh.segFileGroupMsgWriter.syncToDisk(); err != nil {
		return err
	}
	return nil
}

// 检查文件是否被污染
func (fh *topicMultiFileHandler) checkForCorruptFiles() (bool, error) {
	return fh.segFileGroupMsgWriter.checkForCorruptFiles()
}

// 检查初始化前结构体不可为空字段
func (fh *topicMultiFileHandler) assetRequiredFieldsBeforeInit() error {
	if fh.topicName == "" || fh.syncToDiskInterval <= 0 || fh.dir == "" || fh.segFileGroupMsgWriter == nil || !fh.segFileGroupMsgWriter.finishInit {
		return errdef.MadeStructNotByNewFuncErr
	}

	return nil
}

type WriteMsgReq struct {
	msg              *msgstorages.Message
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

	multiFileHandler, err := newTopicMultiFileHandler(
		topicName,
		strings.TrimRight(baseDir, "/")+"/"+topicName,
		maxWritableMsgNum,
		maxWritableMsgDataBytes,
		syncToDiskInterval,
	)
	if err != nil {
		return nil, err
	}

	writer := &topicMsgWriter{
		topicName: topicName,
		logger:    logger,
		multiFileHandler: multiFileHandler,
	}
	if err = writer.init(); err != nil {
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

	w.writeMsgReqChan = make(chan *WriteMsgReq, 10000)
	w.stopLoopEventCh = make(chan struct{}, 10000)
	w.finishInit = true

	return nil
}

// 检查初始化前不可为空字段
func (w *topicMsgWriter) assetRequiredFieldsBeforeInit() error {
	if w.topicName == "" || w.logger == nil || w.multiFileHandler == nil || !w.multiFileHandler.finishInit {
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
		err := errdef.NewErr(errdef.ErrCodeArgsInvalid, "expected "+reflect.TypeOf(logger).Name()+", but nil")
		return err
	}
	w.logger = logger
	return nil
}

// 批量写入消息
func (w *topicMsgWriter) writeMsgs(msgItems []*fileMsgWrapper) error {
	if w.multiFileHandler.hasFileCorruption {
		err := errdef.FileCorruptionErr
		return err
	}

	for len(msgItems) > 0 {
		var (
			batchWritableMsgs            []*fileMsgWrapper
			batchWritableMsgDataBufBytes uint32
			areMsgFilesFull              bool
		)

		for _, msgItem := range msgItems {
			msgBytes := w.multiFileHandler.msgBufEncoder.getMsgsDataBufBytes([]*fileMsgWrapper{msgItem})
			batchWritableMsgDataBufBytes += msgBytes
			if w.multiFileHandler.segFileGroupMsgWriter.writtenIndexNum + uint32(len(batchWritableMsgs)) >= w.multiFileHandler.segFileGroupMsgWriter.maxWritableIndexNum ||
				w.multiFileHandler.segFileGroupMsgWriter.writtenDataBytes + batchWritableMsgDataBufBytes >= w.multiFileHandler.segFileGroupMsgWriter.maxWritableDataBytes {
				areMsgFilesFull = true
				batchWritableMsgDataBufBytes -= msgBytes
				break
			}

			batchWritableMsgs = append(batchWritableMsgs, msgItem)
		}

		batchWritableMsgNum := len(batchWritableMsgs)

		if batchWritableMsgNum > 0 {
			indexesBuf, dataBuf := w.multiFileHandler.msgBufEncoder.encodeMsgs(batchWritableMsgs)

			var (
				indexesBufLen   = len(indexesBuf)
				totalWrittenNum int
			)
			for {
				writtenNum, err := w.multiFileHandler.segFileGroupMsgWriter.dataFileWriter.fp.Write(dataBuf)
				if err != nil {
					return err
				}

				totalWrittenNum += writtenNum
				if uint32(totalWrittenNum) >= batchWritableMsgDataBufBytes {
					break
				}

				dataBuf = dataBuf[writtenNum:]
			}
			w.multiFileHandler.segFileGroupMsgWriter.writtenDataBytes += batchWritableMsgDataBufBytes

			totalWrittenNum = 0
			for {
				writtenNum, err := w.multiFileHandler.segFileGroupMsgWriter.indexFileWriter.fp.Write(indexesBuf)
				if err != nil {
					return err
				}

				totalWrittenNum += writtenNum
				if totalWrittenNum >= indexesBufLen {
					break
				}

				indexesBuf = indexesBuf[writtenNum:]
			}
			w.multiFileHandler.segFileGroupMsgWriter.indexFileWriter.writtenIndexNum += uint32(batchWritableMsgNum)

			if len(msgItems) <= batchWritableMsgNum {
				msgItems = nil
			} else {
				msgItems = msgItems[batchWritableMsgNum:]
			}
		}

		if !areMsgFilesFull {
			continue
		}

		if err := w.multiFileHandler.segFileGroupMsgWriter.openNextSeqMsgFiles(); err != nil {
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
			ch        chan *WrittenMsgEvent
			msgOffset uint64
		}
		var (
			msgItems                 []*fileMsgWrapper
			allocNextMsgOffset        = w.multiFileHandler.segFileGroupMsgWriter.firstMsgOffset + uint64(w.multiFileHandler.segFileGroupMsgWriter.writtenIndexNum)
			allocNextMsgDataOffset    = w.multiFileHandler.segFileGroupMsgWriter.writtenDataBytes
			writtenMsgEventChWrappers []*writtenMsgEventChWrapper
		)

		handleDataForReadyWrite := func(writeMsgReq *WriteMsgReq) {
			// 消息不一定写入成功所以这里分配的message offset最终不一定会生效,
			// 使用消息克隆体赋值message offset避免写入失败时影响外面传入的message值
			msgMetadata := writeMsgReq.msg.GetMetadata()
			msgDataPayload := writeMsgReq.msg.GetDataPayload()
			msgClone := msgstorages.NewMsg(&msgstorages.NewMsgReq{
				Data: msgDataPayload.GetData(),
				Bucket: msgMetadata.GetBucket(),
				CreatedAt: msgMetadata.GetCreatedAt(),
				Priority: msgMetadata.GetPriority(),
				DelaySeconds: msgMetadata.GetDelaySeconds(),
				Id: msgMetadata.GetMsgId(),
				ExpireAt: msgMetadata.GetExpireAt(),
				MsgOffset: allocNextMsgOffset,
				RetryCnt: msgMetadata.GetRetryCnt(),
				ExpectRetryAt: msgMetadata.GetExpectRetryAt(),
			})
			msgItem := &fileMsgWrapper{
				msg: msgClone,
				fileSeq: w.multiFileHandler.segFileGroupMsgWriter.fileSeq,
				dataOffset: allocNextMsgDataOffset,
			}
			msgItems = append(msgItems, msgItem)
			if writeMsgReq.writtenEventChan != nil {
				writtenMsgEventChWrappers = append(writtenMsgEventChWrappers, &writtenMsgEventChWrapper{
					msgOffset: msgClone.GetMetadata().GetMsgOffset(),
					ch:        writeMsgReq.writtenEventChan,
				})
			}
			allocNextMsgOffset++
			allocNextMsgDataOffset += w.multiFileHandler.msgBufEncoder.getMsgsDataBufBytes([]*fileMsgWrapper{msgItem})
		}

		if firstWriteReq != nil {
			handleDataForReadyWrite(firstWriteReq)
		}

		for {
			var moreWriteMsgReq *WriteMsgReq
			select {
			case moreWriteMsgReq = <- w.writeMsgReqChan:
				if w.isReadyStopLoop() {
					continue
				}

				if moreWriteMsgReq != nil {
					handleDataForReadyWrite(moreWriteMsgReq)
				}
			default:
			}
			if moreWriteMsgReq == nil {
				break
			}
		}

		if len(msgItems) == 0 {
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

		err := w.writeMsgs(msgItems)
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
			return nil
		}
	}
}
