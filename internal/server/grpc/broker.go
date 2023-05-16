package grpc

import (
	"context"
	"github.com/995933447/bucketmq/pkg/rpc"
)

var _ rpc.BrokerServer = (*Broker)(nil)

type Broker struct {
}

func (b Broker) RegTopic(c context.Context, req *rpc.RegTopicReq) (*rpc.RegTopicResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b Broker) RegSubscriber(c context.Context, req *rpc.RegSubscriberReq) (*rpc.RegSubscriberResp, error) {
	//TODO implement me
	panic("implement me")
}
