package cli

import (
	"github.com/995933447/bucketmq/pkg/rpc"
	"github.com/995933447/microgosuit/discovery"
	"github.com/995933447/microgosuit/discovery/impl/etcd"
	"github.com/995933447/microgosuit/grpcsuit"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"time"
)

func NewCli(cluster string, etcdCfg *clientv3.Config) (*Cli, error) {
	cli := &Cli{
		consumers: map[string]*Consumer{},
	}

	var err error
	cli.discovery, err = etcd.NewDiscovery(time.Second*5, *etcdCfg)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(rpc.GrpcResolveSchema+":///"+cluster, grpcsuit.RoundRobinDialOpts...)
	if err != nil {
		return nil, err
	}

	cli.BucketMqClient = rpc.NewBucketMqClient(conn)

	return cli, nil
}

type Cli struct {
	discovery discovery.Discovery
	consumers map[string]*Consumer
	rpc.BucketMqClient
}

func (c *Cli) InitConsumer(name, host string, port int) (*Consumer, error) {
	consumer := &Consumer{
		cli:         c,
		name:        name,
		host:        host,
		port:        port,
		subscribers: map[string]map[string]*subscriber{},
	}

	c.consumers[name] = consumer

	return consumer, nil
}
