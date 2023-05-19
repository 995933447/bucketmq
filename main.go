package main

import (
	"context"
	"encoding/json"
	"github.com/995933447/bucketmq/internal/mgr"
	"github.com/995933447/bucketmq/internal/server/grpc/handler"
	"github.com/995933447/bucketmq/internal/server/grpc/middleware"
	"github.com/995933447/bucketmq/internal/syscfg"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/rpc/broker"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/gonetutil"
	"github.com/995933447/microgosuit"
	"github.com/995933447/microgosuit/discovery"
	"github.com/995933447/microgosuit/factory"
	"google.golang.org/grpc"
)

func main() {
	if err := syscfg.Init(""); err != nil {
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

	err = microgosuit.ServeGrpc(context.Background(), &microgosuit.ServeGrpcReq{
		SrvName:              discover.SrvNameBroker,
		RegDiscoverKeyPrefix: discover.SrvNamePrefix,
		IpVar:                gonetutil.InnerIp,
		RegisterCustomServiceServerFunc: func(server *grpc.Server) error {
			etcdCli, err := util.GetOrNewEtcdCli()
			if err != nil {
				return err
			}
			topicMgr, err := mgr.NewTopicMgr(etcdCli, disc)
			if err != nil {
				return err
			}
			broker.RegisterBrokerServer(server, handler.NewBroker(topicMgr))
			return nil
		},
		BeforeRegDiscover: func(_ discovery.Discovery, node *discovery.Node) error {
			nodeDesc, err := json.Marshal(&broker.Node{NodeGrp: syscfg.MustCfg().NodeGrp})
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
