package mgr

import (
	"github.com/995933447/bucketmq/engine"
)

type TopicCfg struct {
	Name        string
	NodeGrpName string
	MaxMsgBytes uint32
}

type SubscriberCfg struct {
	LoadMsgBootId           uint32
	Name                    string
	ConsumerNum             uint32
	MaxConsumerNumPerBucket uint32
	MsgWeight               engine.MsgWeight
}
