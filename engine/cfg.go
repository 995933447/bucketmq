package engine

type WriterCfg struct {
	BaseDir           string
	IdxFileMaxItemNum uint32
	DataFileMaxBytes  uint32
	DataFileMaxSize   string
}

type SubscriberCfg struct {
	LoadMsgBootId                        uint32
	BaseDir, Topic, ConsumerName         string
	ConsumerNum, MaxConsumerNumPerBucket uint32
	MsgWeight                            MsgWeight
	LodeMode                             LoadMsgMode
	StartMsgId                           uint64
}
