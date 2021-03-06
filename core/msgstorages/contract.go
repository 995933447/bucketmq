package msgstorages

import (
	"context"
	"github.com/995933447/bucketmq/core/utils/uniqid"
	"math"
	"time"
)

const (
	MaxMsgPriority = 3
)

const (
	GlobalFirstMsgOffset = 0
)

const (
	MsgWeightPriority MsgWeight = iota
	MsgWeightCreatedAtWithPriority
)

type BucketWeight int8

type MsgWeight int8

type MsgMetadata struct {
	// 消息入的桶
	bucket uint32 `access:"r"`
	// 消息创建时间
	createdAt uint32 `access:"r"`
	// 消息优先级
	priority uint8 `access:"r"`
	// 延迟消费时间
	delaySeconds uint32 `access:"r"`
	// 消息过期时间
	expireAt uint32 `access:"r"`
	// 消息ID
	msgId string `access:"r"`
	// 全局消息位移
	msgOffset uint64 `access:"r"`
	// 重试次数
	retryCnt uint32 `access:"r"`
	// 期望重试时间
	expectRetryAt uint32 `access:"rw"`
}

func (m *MsgMetadata) GetMsgOffset() uint64 {
	return m.msgOffset
}

func (m *MsgMetadata) GetRetryCnt() uint32 {
	return m.retryCnt
}

func (m *MsgMetadata) AddRetryCnt() {
	m.retryCnt++
}

func (m *MsgMetadata) GetExpectRetryAt() uint32 {
	return m.expectRetryAt
}

func (m *MsgMetadata) SetExpectRetryAt(expectRetryAt uint32) {
	m.expectRetryAt = expectRetryAt
}

func (m *MsgMetadata) GetMsgId() string {
	return m.msgId
}

func (m *MsgMetadata) GetBucket() uint32 {
	return m.bucket
}

func (m *MsgMetadata) GetCreatedAt() uint32 {
	return m.createdAt
}

func (m *MsgMetadata) GetPriority() uint8 {
	return m.priority
}

func (m *MsgMetadata) GetDelaySeconds() uint32 {
	return m.delaySeconds
}

func (m *MsgMetadata) GetExpireAt() uint32 {
	return m.expireAt
}

type MsgDataPayload struct {
	// 消息内容
	data []byte `access:"r"`
}

func (m *MsgDataPayload) GetData() []byte {
	return m.data
}

type Message struct {
	metadata    *MsgMetadata    `access:"r"`
	dataPayload *MsgDataPayload `access:"r"`
}

type NewMsgReq struct {
	// 消息内容
	Data []byte
	// 消息入的桶
	Bucket uint32
	// 消息创建时间
	CreatedAt uint32
	// 消息优先级
	Priority uint8
	// 延迟消费时间
	DelaySeconds uint32
	// 消息id,需要保证唯一性
	Id string
	// 消息过期时间
	ExpireAt uint32
	// 全局消息位移
	MsgOffset uint64 `access:"r"`
	// 重试次数
	RetryCnt uint32 `access:"r"`
	// 期望重试时间
	ExpectRetryAt uint32 `access:"rw"`
}

func NewMsg(req *NewMsgReq) *Message {
	msgId := req.Id
	if msgId == "" {
		msgId = uniqid.GenUuid()
	} else if len(msgId) > 36 {
		msgId = msgId[:36]
	}

	return &Message{
		metadata: &MsgMetadata{
			bucket:       req.Bucket,
			createdAt:    req.CreatedAt,
			priority:     req.Priority,
			delaySeconds: req.DelaySeconds,
			expireAt:     req.ExpireAt,
			msgId:        msgId,
			msgOffset:    req.MsgOffset,
			retryCnt:     req.RetryCnt,
			expectRetryAt: req.ExpectRetryAt,
		},
		dataPayload: &MsgDataPayload{
			data: req.Data,
		},
	}
}

func (m *Message) CalcDefaultRetryAt() {
	m.metadata.expectRetryAt = uint32(math.Pow(float64(m.metadata.retryCnt), 2)) * 5 + uint32(time.Now().Unix())
}

func (m *Message) ExpectAttemptAt() uint32 {
	if m.IsAttempted() {
		return m.metadata.expectRetryAt
	}
	return m.metadata.createdAt + m.metadata.delaySeconds
}

func (m *Message) IsAttempted() bool {
	return m.metadata.expectRetryAt > 0
}

func (m *Message) GetMetadata() *MsgMetadata {
	return m.metadata
}

func (m *Message) GetDataPayload() *MsgDataPayload {
	return m.dataPayload
}

type ReleaseMsgReq struct {
	// 消息id
	MsgId string
	// 是否跳过本次消息尝试次数累加计算
	SkipIncrConsumedCnt bool
	// 重试时间
	RetryAt uint32
}

type MsgStorage interface {
	// Push 写入消息
	Push(ctx context.Context, topicName string, msg *Message) error
	// Pop 弹出消息
	Pop(ctx context.Context, topicName, consumerGroupName string) (*Message, error)
	// Done 确认消息消费完成状态
	Done(ctx context.Context, topicName, consumerGroupName string, msgId string) error
	// Release 确认消息重试状态
	Release(context.Context, *ReleaseMsgReq) error
	// Fail 缺认消息消费失败状态
	Fail(ctx context.Context, topicName, consumerGroupName string, msgId string) error
}
