package engine

import (
	"container/heap"
	"errors"
	"github.com/huandu/skiplist"
	"time"
)

const MaxMsgPriority = 3

type MsgWeight int

const (
	MsgWeightPriority MsgWeight = iota
	MsgWeightCreatedAtWithPriority
)

type msgTable [MaxMsgPriority + 1][]*FileMsg

// 等待消费的消息长度
func (m msgTable) len() uint32 {
	var totalMsgNum uint32
	for _, msgList := range m {
		totalMsgNum += uint32(len(msgList))
	}
	return totalMsgNum
}

func (m *msgTable) push(msgItem *FileMsg) {
	var (
		msgPriority = msgItem.priority
		msgList     = m[msgPriority]
	)
	if msgList == nil {
		m[msgPriority] = []*FileMsg{msgItem}
		return
	}

	m[msgPriority] = append(msgList, msgItem)
}

func (m *msgTable) pop(msgWeight MsgWeight) (*FileMsg, error) {
	switch msgWeight {
	case MsgWeightPriority:
		for i := MaxMsgPriority; i >= 0; i-- {
			var (
				msgList    = m[i]
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
			m[i] = msgList

			return msgItem, nil
		}
	case MsgWeightCreatedAtWithPriority:
		var bestMsgItem *FileMsg

		var poppedMsgListIdx int
		for i := MaxMsgPriority; i >= 0; i-- {
			msgList := m[i]

			if len(msgList) == 0 {
				continue
			}

			msgItem := msgList[0]

			if bestMsgItem != nil && bestMsgItem.createdAt < msgItem.createdAt {
				continue
			}

			poppedMsgListIdx = i
			bestMsgItem = msgItem
		}

		if bestMsgItem == nil {
			break
		}

		msgList := m[poppedMsgListIdx]
		if len(msgList) > 1 {
			msgList = msgList[1:]
		} else {
			msgList = nil
		}
		m[poppedMsgListIdx] = msgList

		return bestMsgItem, nil
	default:
		return nil, errors.New("not support msg weight")
	}

	return nil, nil
}

func newBucket(id uint32, queue *queue) *bucket {
	return &bucket{
		id:            id,
		queue:         queue,
		retryMsgQueue: newRetryMsgQueue(nil),
	}
}

type bucket struct {
	id uint32
	// 优先级消息队列链表
	msgTable
	// 指向当前消息桶在全局消息桶链表中的下个桶
	nextOfBuckets *bucket
	// 指向当前消息桶在全局消息桶链表中的上个桶
	prevOfBuckets *bucket
	// 串行重试队列
	*retryMsgQueue

	*queue
}

// 待消费的消息长度
func (b *bucket) len() uint32 {
	var totalMsgNum uint32
	totalMsgNum = b.msgTable.len()
	totalMsgNum += uint32(b.retryMsgQueue.Len())
	return totalMsgNum
}

func (b *bucket) pop(msgWeight MsgWeight) *FileMsg {
	if b.retryMsgQueue.Len() > 0 {
		msgItem := b.popRetry()

		if msgItem != nil {
			return msgItem
		}

		if b.queue.isSerial {
			return nil
		}

		if b.retryMsgQueue.Len() > 100 {
			return msgItem
		}
	}

	msgItem, err := b.msgTable.pop(msgWeight)
	if err != nil {
		panic(err)
	}

	return msgItem
}

func (b *bucket) push(msgItem *FileMsg) {
	if msgItem.retryAt > 0 {
		b.retryMsgQueue.pushRetry(msgItem)
		return
	}

	b.msgTable.push(msgItem)
}

type retryMsgQueue []*FileMsg

func newRetryMsgQueue(items []*FileMsg) *retryMsgQueue {
	var q retryMsgQueue
	for _, item := range items {
		q = append(q, item)
	}

	heap.Init(&q)
	return &q
}

func (q *retryMsgQueue) popRetry() *FileMsg {
	if len(*q) == 0 {
		return nil
	}
	if (*q)[0].retryAt > uint32(time.Now().Unix()) {
		return nil
	}
	return heap.Pop(q).(*FileMsg)
}

func (q *retryMsgQueue) pushRetry(retry *FileMsg) {
	heap.Push(q, retry)
}

func (q *retryMsgQueue) Len() int {
	return len(*q)
}

func (q retryMsgQueue) Swap(i, j int) {
	tmp := q[i]
	q[i] = q[j]
	q[j] = tmp
}

func (q retryMsgQueue) Less(i, j int) bool {
	return q[i].retryAt < q[j].retryAt
}

func (q *retryMsgQueue) Push(x interface{}) {
	*q = append(*q, x.(*FileMsg))
}

func (q *retryMsgQueue) Pop() interface{} {
	queenLen := len(*q)
	if queenLen == 0 {
		return nil
	}
	lastItemIndex := queenLen - 1
	item := (*q)[lastItemIndex]
	*q = (*q)[:lastItemIndex]
	return item
}

func (q retryMsgQueue) Peek() *FileMsg {
	return q[0]
}

func newDelayMsgQueue() *delayMsgQueue {
	return (*delayMsgQueue)(skiplist.New(&delayMsgQueueItemComparable{}))
}

// 延迟队列
type delayMsgQueue skiplist.SkipList

// 延迟队列消息比较处理器
type delayMsgQueueItemComparable struct{}

func (c *delayMsgQueueItemComparable) Compare(_, _ interface{}) int {
	return 1
}

func (c *delayMsgQueueItemComparable) CalcScore(key interface{}) float64 {
	return float64(key.(*FileMsg).delaySec) + float64(time.Now().Unix())
}

// 迁移到期消息,按到期时间降序排序
func (q *delayMsgQueue) migrateExpired() []*FileMsg {
	var (
		expired  []*FileMsg
		skipList = (*skiplist.SkipList)(q)
		now      = uint32(time.Now().Unix())
	)

	// 伪造一个当下下一秒过期的消息项
	expireAtNextSecFake := &FileMsg{
		delaySec: uint32(time.Now().Unix()),
	}
	// 找出离当下最近的未来过期的或者已经过期的消息项
	found := skipList.Find(expireAtNextSecFake)
	if found == nil {
		found = skipList.Back()
	}

	if found.Score() > float64(now) {
		found = found.Prev()
	}

	for found != nil {
		expired = append(expired, found.Key().(*FileMsg))
		found = found.Prev()
	}

	return expired
}

// 推入单个消息
func (q *delayMsgQueue) push(msgItem *FileMsg) {
	(*skiplist.SkipList)(q).Set(msgItem, struct{}{})
}

// 移除单个消息
func (q *delayMsgQueue) remove(msgItem *FileMsg) bool {
	(*skiplist.SkipList)(q).Remove(msgItem)
	return false
}

func newQueue(msgWeight MsgWeight, isSerial bool) *queue {
	return &queue{
		bucketHash:    map[uint32]*bucket{},
		msgWeight:     msgWeight,
		delayMsgQueue: newDelayMsgQueue(),
		isSerial:      isSerial,
	}
}

type queue struct {
	bucketHash      map[uint32]*bucket
	headerOfBuckets *bucket
	curReadBucket   *bucket
	tailOfBuckets   *bucket
	msgWeight       MsgWeight
	delayMsgQueue   *delayMsgQueue
	isSerial        bool
}

func (q *queue) push(msg *FileMsg, isFirstEnqueue bool) {
	if isFirstEnqueue && msg.delaySec > 0 {
		q.delayMsgQueue.push(msg)
		return
	}

	var (
		specBucket *bucket
		ok         bool
	)
	if len(q.bucketHash) == 0 {
		specBucket = newBucket(msg.bucketId, q)
		q.headerOfBuckets = specBucket
		q.tailOfBuckets = specBucket
		q.bucketHash[msg.bucketId] = specBucket
	} else if specBucket, ok = q.bucketHash[msg.bucketId]; !ok {
		specBucket = newBucket(msg.bucketId, q)
		specBucket.prevOfBuckets = q.tailOfBuckets
		q.tailOfBuckets.nextOfBuckets = specBucket
		q.tailOfBuckets = specBucket
		q.bucketHash[msg.bucketId] = specBucket
	}
	specBucket.push(msg)
}

func (q *queue) pop(bucketFilter func(*bucket) bool) *FileMsg {
	if q.headerOfBuckets == nil {
		return nil
	}
	if q.curReadBucket == nil {
		q.curReadBucket = q.headerOfBuckets
	}
	startBucket := q.curReadBucket
	for {
		q.curReadBucket = q.curReadBucket.nextOfBuckets
		if q.curReadBucket == nil {
			q.curReadBucket = q.headerOfBuckets
		}
		if bucketFilter != nil && !bucketFilter(q.curReadBucket) {
			continue
		}
		if msgItem := q.curReadBucket.pop(q.msgWeight); msgItem != nil {
			return msgItem
		}
		if startBucket == q.curReadBucket {
			break
		}
	}
	return nil
}

func (q *queue) migrateExpired() {
	for _, msgItem := range q.delayMsgQueue.migrateExpired() {
		q.push(msgItem, false)
	}
}
