package cli

import (
	"context"
	"errors"
	"fmt"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/bucketmq/pkg/rpc/broker"
	"github.com/995933447/bucketmq/pkg/rpc/snrpc"
	"github.com/995933447/microgosuit/discovery"
	"github.com/995933447/microgosuit/discovery/impl/etcd"
	"github.com/995933447/microgosuit/grpcsuit"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"sync"
	"time"
)

func NewBucketMQ(svrName string, etcdCfg *clientv3.Config) (*Cli, error) {
	cli := &Cli{
		consumers: map[string]*Consumer{},
	}

	var err error
	cli.discovery, err = etcd.NewDiscovery(discover.SrvNamePrefix, time.Second*5, *etcdCfg)
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	service, err := cli.discovery.Discover(context.Background(), svrName)
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	if len(service.Nodes) == 0 {
		util.Logger.Error(nil, "not found service")
		return nil, errors.New("not found service")
	}

	// 需要随机选举一个节点
	node := service.Nodes[0]
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", node.Host, node.Port), grpcsuit.RoundRobinDialOpts...)
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	cli.BrokerClient = broker.NewBrokerClient(conn)

	return cli, nil
}

type Cli struct {
	discovery     discovery.Discovery
	consumers     map[string]*Consumer
	opConsumersMu sync.RWMutex
	broker.BrokerClient
}

func (c *Cli) RegConsumer(name string, onErr OnConsumerErr) (*Consumer, error) {
	c.opConsumersMu.RLock()
	if consumer, ok := c.consumers[name]; ok {
		c.opConsumersMu.RUnlock()

		consumer.onErr = onErr
		return consumer, nil
	}
	c.opConsumersMu.RUnlock()

	c.opConsumersMu.Lock()
	defer c.opConsumersMu.Unlock()

	consumer := &Consumer{
		cli:                c,
		name:               name,
		onErr:              onErr,
		topicToSNRPCCliMap: map[string]*snrpc.Cli{},
	}
	c.consumers[name] = consumer

	return consumer, nil
}
