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
	var (
		msgPriority = msgItem.msg.GetMetadata().GetPriority()
		msgList     = m[msgPriority]
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

			return msgItem, nil
		}
	case msgstorages.MsgWeightCreatedAtWithPriority:
		var bestMsgItem *fileMsgWrapper

		for i := msgstorages.MaxMsgPriority; i <= 0; i-- {
			var (
				msgList    = m[i]
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
type bucket struct {
	// 优先级消息队列链表
	msgTable
	// 指向当前消息桶在全局消息桶链表中的下个桶
	nextOfBucketList *bucket
	// 指向当前消息桶在全局消息桶链表中的上个桶
	prevOfBucketList *bucket
	// 串行重试队列
	*serialRetryMsgQueue
}

// 待消费的消息长度
func (b *bucket) len() uint32 {
	var totalMsgNum uint32
	totalMsgNum = b.msgTable.len()
	if b.serialRetryMsgQueue != nil {
		totalMsgNum += uint32(b.serialRetryMsgQueue.Len())
	}
	return totalMsgNum
}

func (b *bucket) pop(msgWeight msgstorages.MsgWeight) *fileMsgWrapper {
	if b.serialRetryMsgQueue != nil && b.serialRetryMsgQueue.Len() > 0 {
		return b.popRetry()
	}

	msgItem, err := b.msgTable.pop(msgWeight)
	if err != nil {
		panic(err)
	}

	return msgItem
}

func (b *bucket) push(msgItem *fileMsgWrapper) {
	if msgItem.msg.IsAttempted() && b.serialRetryMsgQueue != nil {
		b.serialRetryMsgQueue.pushRetry(msgItem)
		return
	}

	var (
		msgPriority = msgItem.msg.GetMetadata().GetPriority()
		msgList     = b.msgTable[msgPriority]
	)
	if msgList == nil {
		b.msgTable[msgPriority] = []*fileMsgWrapper{msgItem}
		return
	}
	b.msgTable[msgPriority] = append(msgList, msgItem)
}

// 就绪队列,根据不同模式使用不同的实现
type readyMsgQueue interface {
	// 弹出消息
	pop() *fileMsgWrapper
	// 推进一个消息
	push(*fileMsgWrapper)
	// 返回队列长度
	len() uint32
	// 消息完成时候回调
	onDoneMsg(*fileMsgWrapper)
}

// 轮询方式获取消息的桶消息哈希表
type bucketLinkedMap struct {
	bucketMap            map[uint32]*bucket
	headerOfBucketList   *bucket
	lastPollOfBucketList *bucket
	tailOfBucketList     *bucket
	isSerialMode         bool
	msgWeight            msgstorages.MsgWeight
	onPush               func(*fileMsgWrapper)
	onPop                func(*fileMsgWrapper)
	onDoneMsgDo          func(wrapper *fileMsgWrapper)
}

func newBucketLinkedMap(isSerialMode bool, msgWeight msgstorages.MsgWeight, onPush, onPop, onDoneMsgDo func(*fileMsgWrapper)) *bucketLinkedMap {
	return &bucketLinkedMap{
		bucketMap:    make(map[uint32]*bucket),
		isSerialMode: isSerialMode,
		msgWeight:    msgWeight,
		onPop:        onPop,
		onPush:       onPush,
		onDoneMsgDo:  onDoneMsgDo,
	}
}

func (m *bucketLinkedMap) onDoneMsg(msgItem *fileMsgWrapper) {
	if m.onDoneMsgDo == nil {
		return
	}

	m.onDoneMsgDo(msgItem)
}

func (m *bucketLinkedMap) pop() *fileMsgWrapper {
	var startPoll *bucket

	for {
		var polling *bucket
		if m.lastPollOfBucketList != nil && m.lastPollOfBucketList.nextOfBucketList != nil {
			polling = m.lastPollOfBucketList.nextOfBucketList
		} else {
			polling = m.headerOfBucketList
		}

		m.lastPollOfBucketList = polling

		if startPoll == nil {
			startPoll = polling
		} else if polling == startPoll {
			break
		}

		msgItem := polling.pop(m.msgWeight)
		if msgItem == nil {
			continue
		}

		m.onPop(msgItem)

		return msgItem
	}

	return nil
}

func (m *bucketLinkedMap) push(msgItem *fileMsgWrapper) {
	var (
		bucketId  = msgItem.msg.GetMetadata().GetBucket()
		theBucket = m.bucketMap[bucketId]
	)

	// 新的消息桶
	if theBucket == nil {
		theBucket = &bucket{
			msgTable:         msgTable{},
			prevOfBucketList: m.tailOfBucketList,
		}
		if theBucket.prevOfBucketList != nil {
			theBucket.prevOfBucketList.nextOfBucketList = theBucket
		}

		m.bucketMap[bucketId] = theBucket
		m.tailOfBucketList = theBucket
		if m.headerOfBucketList == nil {
			m.headerOfBucketList = theBucket
		}
	}

	doPush := func() {
		if m.isSerialMode && theBucket.serialRetryMsgQueue == nil && msgItem.msg.IsAttempted() {
			theBucket.serialRetryMsgQueue = newSerialRetryMsgQueue([]*fileMsgWrapper{msgItem})
			return
		}
		theBucket.push(msgItem)
	}
	doPush()

	m.onPush(msgItem)
}

func (m *bucketLinkedMap) len() uint32 {
	var totalMsgNum uint32
	for _, bucket := range m.bucketMap {
		totalMsgNum += bucket.len()
	}
	return totalMsgNum
}

// 不用桶模式存放的消息表
type notBucketMsgTable struct {
	msgTable
	*serialRetryMsgQueue
	isSerialMode bool
	msgWeight    msgstorages.MsgWeight
}

func newNotBucketMsgTable(isSerialMode bool, msgWeight msgstorages.MsgWeight) *notBucketMsgTable {
	return &notBucketMsgTable{
		msgTable:     msgTable{},
		isSerialMode: isSerialMode,
		msgWeight:    msgWeight,
	}
}

func (t *notBucketMsgTable) onMsgDone(_ *fileMsgWrapper) {
	return
}

func (t *notBucketMsgTable) len() uint32 {
	totalMsgNum := t.msgTable.len()
	if t.serialRetryMsgQueue == nil {
		return totalMsgNum
	}
	return totalMsgNum + uint32(t.serialRetryMsgQueue.Len())
}

func (t *notBucketMsgTable) pop() *fileMsgWrapper {
	if t.isSerialMode && t.serialRetryMsgQueue != nil && t.serialRetryMsgQueue.Len() > 0 {
		return t.serialRetryMsgQueue.popRetry()
	}

	msgItem, err := t.msgTable.pop(t.msgWeight)
	if err != nil {
		panic(err)
	}

	return msgItem
}

func (t *notBucketMsgTable) push(msgItem *fileMsgWrapper) {
	if msgItem.msg.IsAttempted() && t.isSerialMode {
		if t.serialRetryMsgQueue == nil {
			t.serialRetryMsgQueue = newSerialRetryMsgQueue([]*fileMsgWrapper{msgItem})
		} else {
			t.serialRetryMsgQueue.pushRetry(msgItem)
		}

		return
	}

	t.msgTable.push(msgItem)
}

// 延迟队列
type delayMsgQueue skiplist.SkipList

// 延迟队列消息比较处理器
type delayMsgQueueItemComparable struct{}

func (c *delayMsgQueueItemComparable) Compare(lhs, rhs interface{}) int {
	if lhs == rhs {
		return 0
	}

	lhsMsgMetadata := lhs.(*fileMsgWrapper).msg.GetMetadata()
	rhsMsgMetadata := rhs.(*fileMsgWrapper).msg.GetMetadata()
	if lhsMsgMetadata.GetMsgOffset() > rhsMsgMetadata.GetMsgOffset() {
		return 1
	} else if lhsMsgMetadata.GetMsgOffset() < rhsMsgMetadata.GetMsgOffset() {
		return -1
	}
	return 0
}

func (c *delayMsgQueueItemComparable) CalcScore(key interface{}) float64 {
	return float64(key.(*fileMsgWrapper).msg.ExpectAttemptAt())
}

func newDelayMsgQueue() *delayMsgQueue {
	return (*delayMsgQueue)(skiplist.New(&delayMsgQueueItemComparable{}))
}

// 迁移到期消息,按到期时间降序排序
func (q *delayMsgQueue) migrateExpired() []*fileMsgWrapper {
	var (
		expired  []*fileMsgWrapper
		skipList = (*skiplist.SkipList)(q)
		now      = uint32(time.Now().Unix())
		// 伪造一个当下下一秒过期的消息项
		expireAtNextSecFake = &fileMsgWrapper{
			msg: msgstorages.NewMsg(&msgstorages.NewMsgReq{
				CreatedAt:    now,
				DelaySeconds: 1,
			}),
		}
		// 找出离当下最近的未来过期的或者已经过期的消息项
		found = skipList.Find(expireAtNextSecFake)
	)

	if found.Key().(*fileMsgWrapper).msg.ExpectAttemptAt() > now {
		found = found.Prev()
	}

	for found != nil {
		expired = append(expired, found.Key().(*fileMsgWrapper))
		found = found.Prev()
	}

	return expired
}

// 推入单个消息
func (q *delayMsgQueue) push(msgItem *fileMsgWrapper) {
	(*skiplist.SkipList)(q).Set(msgItem, struct{}{})
}

// 移除单个消息
func (q *delayMsgQueue) remove(msgItem *fileMsgWrapper) bool {
	(*skiplist.SkipList)(q).Remove(msgItem)
	return false
}

// 串行化模式重试消息队列,以最小堆结构存储
type serialRetryMsgQueue []*fileMsgWrapper

func newSerialRetryMsgQueue(items []*fileMsgWrapper) *serialRetryMsgQueue {
	var queue serialRetryMsgQueue
	for _, item := range items {
		if !queue.isValidRetryItem(item) {
			continue
		}
		queue = append(queue, item)
	}

	heap.Init(&queue)
	return &queue
}

func (q *serialRetryMsgQueue) popRetry() *fileMsgWrapper {
	if len(*q) == 0 {
		return nil
	}
	if (*q)[0].msg.GetMetadata().GetExpectRetryAt() < uint32(time.Now().Unix()) {
		return nil
	}
	return heap.Pop(q).(*fileMsgWrapper)
}

func (q *serialRetryMsgQueue) pushRetry(retry *fileMsgWrapper) {
	if !q.isValidRetryItem(retry) {
		return
	}
	heap.Push(q, retry)
}

func (serialRetryMsgQueue) isValidRetryItem(retry *fileMsgWrapper) bool {
	if retry == nil || retry.msg.GetMetadata().GetExpectRetryAt() == 0 {
		return false
	}
	return true
}

func (q serialRetryMsgQueue) Len() int {
	return len(q)
}

func (q serialRetryMsgQueue) Swap(i, j int) {
	tmp := q[i]
	q[i] = q[j]
	q[j] = tmp
}

func (q serialRetryMsgQueue) Less(i, j int) bool {
	return q[i].msg.GetMetadata().GetExpectRetryAt() < q[j].msg.GetMetadata().GetExpectRetryAt()
}

func (q *serialRetryMsgQueue) Push(x interface{}) {
	*q = append(*q, x.(*fileMsgWrapper))
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

func (q serialRetryMsgQueue) Peek() *fileMsgWrapper {
	return q[0]
}
