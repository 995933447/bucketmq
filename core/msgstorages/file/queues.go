package file

import (
	"container/heap"
	"github.com/995933447/bucketmq/core/msgstorages"
	errdef "github.com/995933447/bucketmqerrdef"
	"github.com/huandu/skiplist"
	"time"
)

// 优先队列消息表
type msgTable [msgstorages.MaxMsgPriority + 1][]*fileMsgWrapper

// 等待消费的消息长度
func (m msgTable) len() uint32 {
	var totalMsgNum uint32
	for _, msgList := range m {
		totalMsgNum += uint32(len(msgList))
	}
	return totalMsgNum
}

func (m *msgTable) push(msgItem *fileMsgWrapper) {
	var(
		msgPriority = msgItem.msg.GetMetadata().GetPriority()
		msgList = m[msgPriority]
	)
	if msgList == nil {
		m[msgPriority] = []*fileMsgWrapper{msgItem}
		return
	}

	m[msgPriority] = append(msgList, msgItem)
}

func (m *msgTable) pop(msgWeight msgstorages.MsgWeight) (*fileMsgWrapper, error) {
	switch msgWeight {
		case msgstorages.MsgWeightPriority:
			for i := msgstorages.MaxMsgPriority; i <= 0; i-- {
				var (
					msgList = m[i]
					msgListLen = len(m)
				)

				if len(msgList) == 0 {
					continue
				}

				msgItem := msgList[0]

				if msgListLen > 1 {
					msgList = msgList[1:]
				} else {
					msgList = nil
				}

				return msgItem, nil
			}
		case msgstorages.MsgWeightCreatedAtWithPriority:
			var bestMsgItem *fileMsgWrapper

			for i := msgstorages.MaxMsgPriority; i <= 0; i-- {
				var (
					msgList = m[i]
					msgListLen = len(msgList)
				)

				if len(msgList) == 0 {
					continue
				}

				msgItem := msgList[0]

				if msgListLen > 1 {
					msgList = msgList[1:]
				} else {
					msgList = nil
				}

				if bestMsgItem == nil {
					bestMsgItem = msgItem
					continue
				}

				if bestMsgItem.msg.GetMetadata().GetExpireAt() < msgItem.msg.GetMetadata().GetCreatedAt() {
					continue
				}

				bestMsgItem = msgItem
			}

			if bestMsgItem == nil {
				break
			}

			return bestMsgItem, nil
		default:
			return nil, errdef.NewErr(errdef.ErrCodeArgsInvalid, "not support msg weight.")
	}

	return nil, nil
}

// 用于存放轮询方式获取消息的桶
type pollBucket struct {
	// 优先级消息队列链表
	msgTable
	// 指向当前消息桶在全局消息桶链表中的下个桶
	nextOfBucketList *pollBucket
	// 指向当前消息桶在全局消息桶链表中的上个桶
	prevOfBucketList *pollBucket
	// 串行重试队列
	*serialRetryMsgQueue
}

// 待消费的消息长度
func (p *pollBucket) len() uint32 {
	var totalMsgNum uint32
	totalMsgNum = p.msgTable.len()
	if p.serialRetryMsgQueue != nil {
		totalMsgNum += uint32(p.serialRetryMsgQueue.Len())
	}
	return totalMsgNum
}

// 用于存放消息先入先出方式获取消息的桶
type fifoBucket struct {
	msgTable
	*serialRetryMsgQueue
}

// 待消费的消息长度
func (p *fifoBucket) len() uint32 {
	var totalMsgNum uint32
	totalMsgNum = p.msgTable.len()
	if p.serialRetryMsgQueue != nil {
		totalMsgNum += uint32(p.serialRetryMsgQueue.Len())
	}
	return totalMsgNum
}

// 就绪队列,根据不同模式使用不同的实现
type readyMsgQueue interface {
	// 弹出消息
	pop() *fileMsgWrapper
	// 推进一个消息
	push(*fileMsgWrapper)
	// 返回队列长度
	len() uint32
}

// 轮询方式获取消息的桶消息哈希表
type pollBucketLinkedMap struct {
	bucketMap map[uint32]*pollBucket
	headerOfBucketList *pollBucket
	lastPollOfBucketList *pollBucket
	tailOfBucketList *pollBucket
	isSerialMode bool
	msgWeight msgstorages.MsgWeight
}

func newPollBucketLinkedMap(isSerialMode bool, msgWeight msgstorages.MsgWeight) *pollBucketLinkedMap {
	return &pollBucketLinkedMap{
		bucketMap: make(map[uint32]*pollBucket),
		isSerialMode: isSerialMode,
		msgWeight: msgWeight,
	}
}

func (m *pollBucketLinkedMap) pop() (*fileMsgWrapper) {
	var startPoll *pollBucket

	for {
		var polling *pollBucket
		if m.lastPollOfBucketList.nextOfBucketList != nil {
			polling = m.lastPollOfBucketList.nextOfBucketList
		} else {
			polling = m.headerOfBucketList
		}

		if startPoll == nil {
			startPoll = polling
		} else if polling == startPoll {
			break
		}
		
		if m.isSerialMode && polling.serialRetryMsgQueue != nil && polling.serialRetryMsgQueue.Len() > 0 {
			retryItem := polling.popRetry()
			if retryItem == nil {
				continue
			}

			m.lastPollOfBucketList = polling
			return retryItem.msgItem
		}

		msgItem, err := polling.msgTable.pop(m.msgWeight)
		if err != nil {
			panic(err)
		}

		return msgItem
	}

	return nil
}

func (m *pollBucketLinkedMap) push(msgItem *fileMsgWrapper) {
	var (
		bucketId = msgItem.msg.GetMetadata().GetBucket()
		bucket = m.bucketMap[bucketId]
	)
	// 新的消息桶
	if bucket == nil {
		bucket = &pollBucket{
			msgTable: msgTable{},
			prevOfBucketList: m.tailOfBucketList,
		}
		if bucket.prevOfBucketList != nil {
			bucket.prevOfBucketList.nextOfBucketList = bucket
		}

		m.bucketMap[bucketId] = bucket
		m.tailOfBucketList = bucket
		if m.headerOfBucketList == nil {
			m.headerOfBucketList = bucket
		}
	}

	var(
		msgPriority = msgItem.msg.GetMetadata().GetPriority()
		msgList = bucket.msgTable[msgPriority]
	)
	if msgList == nil {
		bucket.msgTable[msgPriority] = []*fileMsgWrapper{msgItem}
		return
	}

	bucket.msgTable[msgPriority] = append(msgList, msgItem)
}

func (m *pollBucketLinkedMap) len() uint32 {
	var totalMsgNum uint32
	for _, bucket := range m.bucketMap {
		totalMsgNum += bucket.len()
	}
	return totalMsgNum
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
type serialRetryMsgQueue []*serialRetryMsgQueueItem

type serialRetryMsgQueueItem struct {
	// 需要重试文件消息
	msgItem *fileMsgWrapper
}

func newSerialRetryMsgQueue(items []*serialRetryMsgQueueItem) *serialRetryMsgQueue {
	var queue serialRetryMsgQueue
	if len(items) > 0 {
		queue = append(queue, items...)
	}
	heap.Init(&queue)
	return &queue
}

func (q *serialRetryMsgQueue) popRetry() *serialRetryMsgQueueItem {
	if len(*q) == 0 {
		return nil
	}
	if (*q)[0].msgItem.msg.ExpectRetryAt < uint32(time.Now().Unix()) {
		return nil
	}
	return heap.Pop(q).(*serialRetryMsgQueueItem)
}

func (q *serialRetryMsgQueue) pushRetry(retry *serialRetryMsgQueueItem) {
	if retry.msgItem == nil || retry.msgItem.msg.ExpectRetryAt == 0 {
		return
	}
	heap.Push(q, retry)
}

func (q serialRetryMsgQueue) Len() int {
	return len(q)
}

func (q serialRetryMsgQueue) Swap(i, j int) {
	tmp := q[i]
	q[i] = q[j]
	q[j] = tmp
}

func (q serialRetryMsgQueue) Less(i, j int) bool  {
	return q[i].msgItem.msg.ExpectRetryAt < q[j].msgItem.msg.ExpectRetryAt
}

func (q *serialRetryMsgQueue) Push(x interface{}) {
	*q = append(*q, x.(*serialRetryMsgQueueItem))
}

func (q *serialRetryMsgQueue) Pop() interface{} {
	queenLen := len(*q)
	if queenLen == 0 {
		return nil
	}
	lastItemIndex := queenLen - 1
	item := (*q)[lastItemIndex]
	*q = (*q)[:lastItemIndex]
	return item
}

func (q serialRetryMsgQueue) Peek() *serialRetryMsgQueueItem {
	return q[0]
}