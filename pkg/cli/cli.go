package cli

import (
	"github.com/995933447/bucketmq/pkg/rpc/broker"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/microgosuit/discovery"
	"github.com/995933447/microgosuit/discovery/impl/etcd"
	"github.com/995933447/microgosuit/grpcsuit"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"time"
)

func NewBucketMQ(cluster string, etcdCfg *clientv3.Config) (*Cli, error) {
	cli := &Cli{
		consumers: map[string]*Consumer{},
	}

	var err error
	cli.discovery, err = etcd.NewDiscovery(discover.GetDiscoverNamePrefix(cluster), time.Second*5, *etcdCfg)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(discover.GetGrpcResolveSchema(cluster)+":///"+discover.SrvNameBroker, grpcsuit.RoundRobinDialOpts...)
	if err != nil {
		return nil, err
	}

	cli.BrokerClient = broker.NewBrokerClient(conn)

	return cli, nil
}

type Cli struct {
	discovery discovery.Discovery
	consumers map[string]*Consumer
	broker.BrokerClient
}

func (c *Cli) AddConsumer(name, host string, port int) (*Consumer, error) {
	consumer := &Consumer{
		cli:   c,
		name:  name,
		host:  host,
		port:  port,
		procs: map[string]map[string]*proc{},
	}

	c.consumers[name] = consumer

	return consumer, nil
}
