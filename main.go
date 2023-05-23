package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/995933447/bucketmq/internal/engine"
	"github.com/995933447/bucketmq/internal/mgr"
	"github.com/995933447/bucketmq/internal/server/grpc/handler"
	"github.com/995933447/bucketmq/internal/server/grpc/middleware"
	"github.com/995933447/bucketmq/internal/server/snrpc"
	snrpchandler "github.com/995933447/bucketmq/internal/server/snrpc/handler"
	"github.com/995933447/bucketmq/internal/syscfg"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/bucketmq/pkg/rpc/broker"
	"github.com/995933447/microgosuit"
	"github.com/995933447/microgosuit/discovery"
	"github.com/995933447/microgosuit/factory"
	"google.golang.org/grpc"
	"time"
)

func main() {
	var cfgFilePath string
	cfgFilePath = *flag.String("c", "", "server config file path")
	flag.Parse()

	if err := syscfg.Init(cfgFilePath); err != nil {
		panic(err)
	}

	disc, err := factory.GetOrMakeDiscovery(discover.SrvNamePrefix)
	if err != nil {
		panic(err)
	}

	go func() {
		if err = mgr.NewHealthChecker(disc, 200, 50000).Run(); err != nil {
			panic(err)
		}
	}()

	sysCfg := syscfg.MustCfg()

	snrpcSrv, err := snrpc.NewServer(sysCfg.Host, sysCfg.Port, 1000)
	consumerHandler := snrpchandler.NewConsumer(snrpcSrv, time.Second*5)
	snrpchandler.RegSNPRPCProto(consumerHandler, snrpcSrv)
	go func() {
		if err := snrpcSrv.Serve(); err != nil {
			panic(err)
		}
	}()

	err = microgosuit.ServeGrpc(context.Background(), &microgosuit.ServeGrpcReq{
		SrvName:              discover.SrvNameBroker,
		RegDiscoverKeyPrefix: discover.SrvNamePrefix,
		IpVar:                sysCfg.Host,
		Port:                 sysCfg.Port2,
		RegisterCustomServiceServerFunc: func(server *grpc.Server) error {
			etcdCli, err := util.GetOrNewEtcdCli()
			if err != nil {
				return err
			}
			topicMgr, err := mgr.NewTopicMgr(etcdCli, disc, func(topic, subscriber, consumer string, timeout time.Duration, msg *engine.FileMsg) error {
				if _, err := consumerHandler.Consume(consumer, topic, subscriber, timeout, msg); err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}
			broker.RegisterBrokerServer(server, handler.NewBroker(topicMgr))
			return nil
		},
		BeforeRegDiscover: func(_ discovery.Discovery, node *discovery.Node) error {
			nodeDesc, err := json.Marshal(&broker.Node{NodeGrp: sysCfg.NodeGrp})
			if err != nil {
				return err
			}
			node.Extra = string(nodeDesc)
			return nil
		},
		SrvOpts: []grpc.ServerOption{
			grpc.ChainUnaryInterceptor(middleware.Recover(), middleware.AutoValidate()),
		},
	})
	if err != nil {
		panic(err)
	}
}
