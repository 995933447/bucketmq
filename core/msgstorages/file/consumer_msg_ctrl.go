package file

import (
	"context"
	"github.com/995933447/bucketmq/core/log"
	"github.com/995933447/bucketmq/core/msgstorages"
	"github.com/995933447/bucketmq/core/utils/fileutil"
	"github.com/995933447/bucketmq/core/utils/structs"
	errdef "github.com/995933447/bucketmqerrdef"
	"io"
	"os"
)

type consumerSegFileGroupMsgLoader struct {
	// 索引文件读取器
	*indexFileReader
	// 数据文件读取器
	*dataFileReader
	// 消息消费完成记录文件读写器
	*doneFileRWriter
	// 消息消费记录文件读写器
	*attemptFileRWriter
	// 消息文件序号
	fileSeq string
	// 文件创建时间
	fileSeqCreatedAt uint32
	// 开始的消息位移
	firstMsgOffset uint64
	// 完成消费的位移
	doneMsgOffsetSet *structs.Uint64Set
	// 各消息尝试次数
	msgOffsetToAttemptMap map[uint64]*attemptFileMsgMetadataWrapper
	// 消息编码器
	*msgBufEncoder
	// 是否已经关闭
	isClosed bool
	// 日志
	logger log.Logger
	// 关联的多文件处理器
	multiFileHandler *consumerMultiFileHandler
	// 是否已经初始化
	finishInit bool
}

func newConsumerSegFileGroupMsgLoader(fileSeq string, msgBufEncoder *msgBufEncoder, logger log.Logger) (*consumerSegFileGroupMsgLoader, error) {
	loader := &consumerSegFileGroupMsgLoader{
		fileSeq: fileSeq,
		msgBufEncoder: msgBufEncoder,
		logger: logger,
	}

	if err := loader.init(); err != nil {
		return nil, err
	}

	return loader, nil
}

func (l *consumerSegFileGroupMsgLoader) init() error {
	if l.finishInit {
		return nil
	}

	var err error

	if err = l.assetRequiredFieldsBeforeInit(); err != nil {
		return err
	}

	if l.fileSeqCreatedAt, l.firstMsgOffset, err = parseCreatedAtAndFirstMsgOffsetFromSeq(l.fileSeq); err != nil {
		return err
	}

	indexFp, err := l.newIndexFp()
	if err != nil {
		return err
	}
	indexFileInfo, err := indexFp.Stat()
	if err != nil {
		return err
	}
	writtenIndexBytes := indexFileInfo.Size()
	if writtenIndexBytes % indexBufSize > 0 {
		l.multiFileHandler.hasFileCorruption = true
		return errdef.FileCorruptionErr
	}

	dataFp, err := l.newDataFp()
	if err != nil {
		return err
	}

	doneFp, err := l.newDoneFp()
	if err != nil {
		return err
	}
	doneMetadataBytes := indexFileInfo.Size()
	if doneMetadataBytes % doneMetadataBufSize > 0 {
		l.multiFileHandler.hasFileCorruption = true
		return errdef.FileCorruptionErr
	}

	attemptFp, err := l.newAttemptFp()
	if err != nil {
		return err
	}
	attemptFileInfo, err := attemptFp.Stat()
	if err != nil {
		return err
	}
	attemptMetadataBytes := attemptFileInfo.Size()
	if attemptMetadataBytes % attemptCntMetadataBufSize > 0 {
		l.multiFileHandler.hasFileCorruption = true
		return errdef.FileCorruptionErr
	}

	l.indexFileReader.fp = indexFp
	l.dataFileReader.fp = dataFp
	l.doneFileRWriter.fp = doneFp
	l.attemptFileRWriter.fp = attemptFp
	l.indexFileReader.indexNum = uint32(writtenIndexBytes / indexBufSize)
	l.msgOffsetToAttemptMap = make(map[uint64]*attemptFileMsgMetadataWrapper)
	l.doneMsgOffsetSet = &structs.Uint64Set{}
	
	return nil
}

func (l *consumerSegFileGroupMsgLoader) assetRequiredFieldsBeforeInit() error {
	if l.fileSeq == "" || l.msgBufEncoder == nil || l.logger == nil {
		return errdef.MadeStructNotByNewFuncErr
	}

	return nil
}

// 构造数据文件句柄
func (l *consumerSegFileGroupMsgLoader) newAttemptFp() (*os.File, error) {
	if err := fileutil.MkdirIfNotExist(l.multiFileHandler.dir); err != nil {
		return nil, err
	}

	fileName := buildAttemptFileName(
		l.multiFileHandler.topicName,
		l.multiFileHandler.consumerGroupName,
		l.multiFileHandler.dir, attemptFileSuffixName,
		l.fileSeq,
		)
	fp, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		return nil, err
	}

	return fp, nil
}

// 构造数据文件句柄
func (l *consumerSegFileGroupMsgLoader) newDoneFp() (*os.File, error) {
	if err := fileutil.MkdirIfNotExist(l.multiFileHandler.dir); err != nil {
		return nil, err
	}

	fileName := buildDoneFileName(
		l.multiFileHandler.topicName,
		l.multiFileHandler.consumerGroupName,
		l.multiFileHandler.dir, doneFileSuffixName,
		l.fileSeq,
		)
	fp, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		return nil, err
	}

	return fp, nil
}

// 构造数据文件句柄
func (l *consumerSegFileGroupMsgLoader) newDataFp() (*os.File, error) {
	if err := fileutil.MkdirIfNotExist(l.multiFileHandler.dir); err != nil {
		return nil, err
	}

	fileName := buildDataFileName(l.multiFileHandler.topicName, l.multiFileHandler.dir, dataFileSuffixName, l.fileSeq)
	fp, err := os.OpenFile(fileName, os.O_RDONLY|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		return nil, err
	}

	return fp, nil
}

// 构造索引文件句柄
func (l *consumerSegFileGroupMsgLoader) newIndexFp() (*os.File, error) {
	if err := fileutil.MkdirIfNotExist(l.multiFileHandler.dir); err != nil {
		return nil, err
	}

	fileName := buildIndexFileName(l.multiFileHandler.topicName, l.multiFileHandler.dir, indexFileSuffixName, l.fileSeq)

	fp, err := os.OpenFile(fileName, os.O_RDONLY|os.O_CREATE, os.FileMode(0755))
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


func (l *consumerSegFileGroupMsgLoader) syncToDisk() error {
	if err := l.doneFileRWriter.fp.Sync(); err != nil {
		return err
	}
	if err := l.attemptFileRWriter.fp.Sync(); err != nil {
		return err
	}
	return nil
}

func (l *consumerSegFileGroupMsgLoader) load() ([]*fileMsgWrapper, error) {
	if err := l.loadDoneMsgOffsets(); err != nil {
		return nil, err
	}

	if err := l.loadMsgAttempts(); err != nil {
		return nil, err
	}

	msgItems, err := l.loadMsges(l.msgOffsetToAttemptMap, l.doneMsgOffsetSet)
	if err != nil {
		return nil, err
	}

	return msgItems, nil
}

func (l *consumerSegFileGroupMsgLoader) loadDoneMsgOffsets() error {
	buf := make([]byte, 10240 * doneMetadataBufSize, 10240 * doneMetadataBufSize)

	_, err := l.doneFileRWriter.fp.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	for {
		n, err := l.doneFileRWriter.fp.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if n % doneMetadataBufSize != 0 {
			return errdef.FileCorruptionErr
		}

		if n == 0 {
			continue
		}

		doneMetadataList, err := l.msgBufEncoder.decodeDoneMetadata(buf[:n])
		if err != nil {
			return err
		}

		for _, doneMetadata := range doneMetadataList {
			l.doneMsgOffsetSet.Put(doneMetadata.msgOffset)
		}
	}

	return nil
}

func (l *consumerSegFileGroupMsgLoader) loadMsgAttempts() error {
	buf := make([]byte, 10240 * doneMetadataBufSize, 10240 * doneMetadataBufSize)

	_, err := l.attemptFileRWriter.fp.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	for {
		var n int
		n, err = l.attemptFileRWriter.fp.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if n % doneMetadataBufSize != 0 {
			err = errdef.FileCorruptionErr
			return err
		}

		if n == 0 {
			continue
		}

		var attemptMetadataList []*attemptFileMsgMetadataWrapper
		attemptMetadataList, err = l.msgBufEncoder.decodeAttemptMetadata(buf[:n])
		if err != nil {
			return err
		}
		for _, attemptMetadata := range attemptMetadataList {
			l.msgOffsetToAttemptMap[attemptMetadata.msgOffset] = attemptMetadata
		}
	}

	return nil
}

func (l *consumerSegFileGroupMsgLoader) loadMsges(msgOffsetToAttemptMap map[uint64]*attemptFileMsgMetadataWrapper, doneMsgOffsetSet *structs.Uint64Set) ([]*fileMsgWrapper, error) {
	var (
		msgBuf = make([]byte, 1024 * 1024 * indexBufSize)
		allLoaded []*fileMsgWrapper
	)

	for {
		loadedIndexNum := uint32(l.indexFileReader.cursor / int64(indexBufSize))
		if loadedIndexNum >= l.indexNum {
			break
		}

		nextLoadMsgOffset := uint64(loadedIndexNum) + l.firstMsgOffset
		if doneMsgOffsetSet.Exist(nextLoadMsgOffset) {
			l.indexFileReader.cursor += indexBufSize
			continue
		}

		n, err := l.indexFileReader.fp.ReadAt(msgBuf, l.indexFileReader.cursor)
		if err != nil {
			return nil, err
		}

		if n % indexBufSize > 0 {
			return nil, errdef.FileCorruptionErr
		}

		msgItems, err := l.msgBufEncoder.decodeIndexes(msgBuf[:n])
		if err != nil {
			return nil, err
		}

		for _, msgItem := range msgItems {
			dataBuf := make([]byte, l.msgBufEncoder.calcMsgDataBufBytes([]*fileMsgWrapper{msgItem}))
			n, err = l.dataFileReader.fp.ReadAt(dataBuf, int64(msgItem.dataOffset))
			if err != nil {
				return nil, err
			}

			var msgData []byte
			msgData, err = l.msgBufEncoder.decodeData(dataBuf)
			if err != nil {
				return nil, err
			}

			var (
				msgMetadata = msgItem.msg.GetMetadata()
				msgRetryCnt, msgNextRetryAt uint32
			)

			if msgAttemptMetadata := msgOffsetToAttemptMap[msgMetadata.GetMsgOffset()]; msgAttemptMetadata.attemptCnt > 0 {
				msgRetryCnt = msgAttemptMetadata.attemptCnt - 1
				msgNextRetryAt = msgAttemptMetadata.nextAttemptedAt
			}

			newMsgReq := &msgstorages.NewMsgReq{
				Bucket: msgMetadata.GetBucket(),
				CreatedAt: msgMetadata.GetCreatedAt(),
				Priority: msgMetadata.GetPriority(),
				DelaySeconds: msgMetadata.GetDelaySeconds(),
				Id: msgMetadata.GetMsgId(),
				ExpireAt: msgMetadata.GetExpireAt(),
				MsgOffset: msgMetadata.GetMsgOffset(),
				RetryCnt: msgRetryCnt,
				ExpectRetryAt: msgNextRetryAt,
				Data: msgData,
			}
			msgItem.msg = msgstorages.NewMsg(newMsgReq)
		}

		allLoaded = append(allLoaded, msgItems...)
		l.cursor += int64(n)
	}

	return allLoaded, nil
}

func (l *consumerSegFileGroupMsgLoader) close(ctx context.Context) error {
	if err := l.indexFileReader.fp.Close(); err != nil {
		l.logger.Warn(ctx, err)
	}

	if err := l.dataFileReader.fp.Close(); err != nil {
		l.logger.Warn(ctx, err)
	}

	if err := l.doneFileRWriter.fp.Close(); err != nil {
		l.logger.Warn(ctx, err)
	}

	l.isClosed = true

	return nil
}

func (l *consumerSegFileGroupMsgLoader) areAllMsgesDone() bool {
	return l.doneMsgOffsetSet.Len() >= l.indexNum
}

type consumerMultiFileHandler struct {
	// 主体名称
	topicName string
	// 消费组名称
	consumerGroupName string
	// 每个文件序号的消息载入器
	consumerMsgLoaders []*consumerSegFileGroupMsgLoader
	// 消费组历史首次消费的开始位移检查文件读写器
	*startOffsetCheckFileRWriter
	// 预加载多少消息文件序号
	numOfPreloadMsgSegFileGroup uint32
	// 文件目录
	dir string
	// 消息编码器
	*msgBufEncoder
	// 文件污染
	hasFileCorruption bool
	// 是否初始化完成
	finishInit bool
}

type doneMsgReq struct {
	msgOffset uint32
}

type firstConsumeMsgOffsetSeek int
const (
	firstConsumeMsgOffsetSeekHistory firstConsumeMsgOffsetSeek = iota
	firstConsumeMsgOffsetSeekOne
	firstConsumeMsgOffsetSeekNewest
)

type consumerMsgCtrl struct {
	// topic名称
	topicName string
	// 消费组名称
	consumerGroupName string
	// 首次消费的偏移位置选择方式
	firstConsumeMsgOffsetSeekAt firstConsumeMsgOffsetSeek
	// 消费组历史首次启动开始消费消息的位置
	firstConsumeMsgOffset uint64
	// 是否需要重新定义开始消费的消息偏移
	needReSeekStartConsumeMsgOffset bool
	// 消息文件处理器
	multiFileHandler *consumerMultiFileHandler
	// 是否消息桶模式
	isBucketMode bool
	// 消息权重分配方式
	msgWeight msgstorages.MsgWeight
	// 是否串行模式
	isSerialMode bool
	// 每个桶的并发消费数
	maxConcurrentConsumptionPerBucket uint32
	// 全局最大并发消费数量
	globalMaxConcurrentConsumption uint32
	// 当前每个桶的正在消费消息数
	bucketToConcurrentConsumptionMap map[uint32]uint32
	// 桶模式就绪队列
	readyMsgQueue
	// 延时队列
	*delayMsgQueue
	// 出队又未确认状态被保留的消息
	msgOffsetToReservedMsgMap map[uint32]*msgstorages.Message
	// 确认完成消息请求的channel
	doneMsgReqCh chan *doneMsgReq
	// 是否已经初始化
	finishInit bool
}

func newConsumerMsgCtrl(
	topicName, consumerGroupName string,
	firstConsumeMsgOffsetSeekAt firstConsumeMsgOffsetSeek,
	needReSeekStartConsumeMsgOffset, isBucketMode, isSerialMode bool,
	msgWeight msgstorages.MsgWeight,
	) error {
	return nil
}
