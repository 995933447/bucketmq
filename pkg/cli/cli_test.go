package cli

import (
	"context"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/bucketmq/pkg/rpc/broker"
	clientv3 "go.etcd.io/etcd/client/v3"
	"testing"
	"time"
)

func TestNewBucketMQ(t *testing.T) {
	ctx := context.Background()
	cli, err := NewBucketMQ(discover.SrvNameBroker, &clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: time.Duration(5000) * time.Millisecond,
	})
	if err != nil {
		util.Logger.Errorf(ctx, "error: %v", err)
		return
	}
	_, err = cli.RegTopic(ctx, &broker.RegTopicReq{
		Topic: &broker.Topic{
			Topic:       "test",
			NodeGrp:     "bucketmq_node_group1",
			MaxMsgBytes: 1024,
		},
	})
	if err != nil {
		util.Logger.Errorf(ctx, "error: %v", err)
		return
	}
}
