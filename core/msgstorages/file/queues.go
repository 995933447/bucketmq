package file

import (
	"github.com/995933447/bucketmq/core/msgstorages"
	"github.com/995933447/bucketmq/core/utils/structs"
	"github.com/huandu/skiplist"
)

// 就绪队列,根据不同模式使用不同的实现
type readyMsgQueue interface {
	// 初始化
	init(isSerialMode bool, msgWeight *msgstorages.MsgWeight)
	// 弹出消息
	pop() *fileMsgWrapper
	// 推进一个消息
	push(*fileMsgWrapper)
	// 返回队列长度
	len() uint32
}

// 优先队列消息表
type msgTable [msgstorages.MaxMsgPriority + 1][]*fileMsgWrapper

// 用于存放轮询方式获取消息的桶
type pollingBucket struct {
	msgTable
	nextOfBucketList *pollingBucket
	prevOfBucketList *pollingBucket
	*serialRetryMsgQueue
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
	*serialRetryMsgQueue
}

// 消息先入先出方式的桶消息哈希表
type fifoBucketLinkedMap struct {
	bucketMap map[uint32]*fifoBucket
	bucketList *skiplist.SkipList
}

// 不用桶模式存放的消息表
type notBucketMsgTable struct {
	msgTable
	*serialRetryMsgQueue
}

// 延迟队列
type delayMsgQueue skiplist.SkipList

// 迁移到期消息
func (q *delayMsgQueue) migrateExpired() {
	return
}

// 移除单个消息
func (q *delayMsgQueue) remove(msgItem *fileMsgWrapper) bool {
	return false
}

// 串行化模式重试消息队列
type serialRetryMsgQueue structs.MinHeap

// 弹出消息
func (q *serialRetryMsgQueue) pop() *fileMsgWrapper {
	return nil
}

// 存进一个消息
func (q *serialRetryMsgQueue) push(msgItem *fileMsgWrapper) {
	return
}