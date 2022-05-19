package file

import (
	"git.pinquest.cn/qlb/extrpkg/github.com/huandu/skiplist"
	"github.com/995933447/bucketmq/core/log"
	"github.com/995933447/bucketmq/core/msgstorages"
	"github.com/995933447/bucketmq/core/utils/structs"
)

type consumerMsgLoaderPerFileSeq struct {
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
}

type consumerMultiFileHandler struct {
	// 每个文件序号的消息载入器
	consumerMsgLoaders []*consumerMsgLoaderPerFileSeq
	// 消费组历史首次消费的开始位移检查文件读写器
	*startOffsetCheckFileRWriter
	// 预加载多少消息文件序号
	NumOfPreloadMsgFileSeqs uint32
	// 文件目录
	dir string
	// 消息编码器
	*msgEncoder
	// 日志
	logger log.Logger
	// 是否初始化完成
	finishInit bool
}

type fileMsgWrapper struct {
	msg msgstorages.Message
	fileSeq uint32
}

type msgTable [msgstorages.MaxMsgPriority + 1][]*fileMsgWrapper

// 用于存放轮询方式获取消息的桶
type pollingBucket struct {
	msgTable
	nextOfBucketList *pollingBucket
	prevOfBucketList *pollingBucket
}

// 轮询方式获取消息的桶消息哈希表
type pollingBucketLinkedMap struct {
	bucketMap map[uint32]*pollingBucket
	lastPollingOfBucketList *pollingBucket
	headerOfBucketList *pollingBucket
	tailOfBucketList *pollingBucket
}

// 用于存放消息先入先出方式获取消息的桶
type fifoBucket struct {
	msgTable
}

//　消息先入先出方式的桶消息哈希表
type fifoBucketLinkedMap struct {
	bucketMap map[uint32]*fifoBucket
	bucketList *skiplist.SkipList
}

type readyMsgQueue struct {
	// 是否桶模式
	isBucketMode bool
	// 是否串行模式
	isSerialMode bool
	// 不分桶模式下的消息表
	notBucketMsgTable *msgTable
	// 轮询桶模式下的消息表
	*pollingBucketLinkedMap
	// 先进先出桶模式下的消息表
	*fifoBucketLinkedMap
	// 桶权重方式
	bucketWeight msgstorages.BucketWeight
	// 消息权重方式
	msgWeight msgstorages.MsgWeight
}

func (q *readyMsgQueue) pop() *fileMsgWrapper {
	return nil
}

type delayMsgQueue skiplist.SkipList

func (q *delayMsgQueue) migrateExpired() {
	return
}

func (q *delayMsgQueue) remove(msg *fileMsgWrapper) bool {
	return false
}

type doneMsgReq struct {
	msgOffset uint32
}

type consumerMsgController struct {
	// topic名称
	topicName string
	// 消费组名称
	consumerGroupName string
	// 消费组历史首次启动开始消费消息的位置
	firstConsumeMsgOffset uint64
	// 消息文件处理器
	multiFileHandler *consumerMultiFileHandler
	// 是否消息桶模式
	isBucketMode bool
	// 消息桶权重分配方式
	bucketWeight msgstorages.BucketWeight
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
	// 完成消费的位移
	doneOffsetSet *structs.Uint32Set
	// 桶模式就绪队列
	*readyMsgQueue
	// 延时队列
	*delayMsgQueue
	// 确认完成消息请求的channel
	doneMsgReqCh chan *doneMsgReq
	// 是否已经初始化
	finishInit bool
}