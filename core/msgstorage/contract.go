package msgstorage

type MsgMetadata struct {
	// 消息入的桶
	bucket uint32 `access:"r"`
	// 消息创建时间
	createdAt uint32 `access:"r"`
	// 消息优先级
	priority uint8 `access:"r"`
	// 延迟消费时间
	delaySeconds uint32 `access:"r"`
	// 最大执行时间
	maxExecTimeLong uint32 `access:"r"`
	// 最大执行次数
	maxRetryCnt uint32 `access:"r"`
	// 消息过期时间
	expireAt uint32 `access:"r"`
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

func (m *MsgMetadata) GetMaxExecTimeLong() uint32 {
	return m.maxExecTimeLong
}

func (m *MsgMetadata) GetMaxRetryCnt() uint32 {
	return m.maxRetryCnt
}

func (m *MsgMetadata) GetExpireAt() uint32 {
	return m.expireAt
}

type MsgDataPayload struct {
	// 消息内容
	data []byte	`access:"r"`
	// 消息id,需要保证唯一性
	msgId string `access:"r"`
}

func (m *MsgDataPayload) GetData() []byte {
	return m.data
}

func (m *MsgDataPayload) GetMsgId() string {
	return m.msgId
}

type Message struct {
	metadata *MsgMetadata
	dataPayload *MsgDataPayload
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
	// 最大执行时间
	MaxExecTimeLong uint32
	// 最大执行次数
	MaxRetryCnt uint32
	// 消息id,需要保证唯一性
	Id string
	// 消息过期时间
	ExpireAt uint32
}

func NewMsg(req NewMsgReq) *Message {
	msgId := req.Id
	if msgId == "" {

	}
	return &Message{
		metadata: &MsgMetadata{
			bucket:          req.Bucket,
			createdAt:       req.CreatedAt,
			priority:        req.Priority,
			delaySeconds:    req.DelaySeconds,
			maxExecTimeLong: req.MaxExecTimeLong,
			maxRetryCnt:     req.MaxRetryCnt,
			expireAt:        req.ExpireAt,
		},
		dataPayload: &MsgDataPayload{
			data:			 req.Data,
			msgId: 			 msgId,
		},
	}
}

func (m *Message) GetMetadata() *MsgMetadata {
	return m.metadata
}

func (m *Message) GetDataPayload() *MsgDataPayload {
	return m.dataPayload
}

type RetryMsgReq struct {
	// 消息id
	msgId string
	// 是否跳过本次消息重试次数累加
	skipAddingRetryCnt bool
	// 重试间隔
	RetryInterval uint32
}

type MsgStorage interface {
	// PushMsg 写入消息
	PushMsg(*Message) error
	// PopMsg 弹出消息
	PopMsg() (*Message, error)
	// SetMsgConsumptionCompleted 确认消息消费完成
	SetMsgConsumptionCompleted(msgId string) error
	// SetMsgWillBeRetried 消息重试
	SetMsgWillBeRetried(*RetryMsgReq) error
}