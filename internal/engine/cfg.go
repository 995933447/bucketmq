package engine

type WriterCfg struct {
	BaseDir           string
	IdxFileMaxItemNum uint32
	DataFileMaxBytes  uint32
}

type SubscriberCfg struct {
	LoadMsgBootId                                  uint32
	Topic, Name, Consumer, BaseDir                 string
	ConcurConsumeNum, MaxConcurConsumeNumPerBucket uint32
	MsgWeight                                      MsgWeight
	IsSerial                                       bool
	LodeMode                                       LoadMsgMode
	StartMsgId                                     uint64
	MaxConsumeMs                                   uint32
}
