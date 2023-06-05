package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/995933447/bucketmq/internal/engine"
	"github.com/995933447/bucketmq/internal/ha/synclog"
	"github.com/995933447/bucketmq/internal/mgr"
	"github.com/995933447/bucketmq/internal/server/grpc/handler"
	"github.com/995933447/bucketmq/internal/server/grpc/middleware"
	"github.com/995933447/bucketmq/internal/server/snrpc"
	snrpchandler "github.com/995933447/bucketmq/internal/server/snrpc/handler"
	"github.com/995933447/bucketmq/internal/syscfg"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/bucketmq/pkg/rpc/broker"
	"github.com/995933447/bucketmq/pkg/rpc/ha"
	"github.com/995933447/microgosuit"
	"github.com/995933447/microgosuit/discovery"
	"github.com/995933447/microgosuit/factory"
	"google.golang.org/grpc"
	"strings"
	"time"
)

func main() {
	if err := parseCmdToInitSysCfg(); err != nil {
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
	if err != nil {
		panic(err)
	}

	consumerService := snrpchandler.NewConsumer(snrpcSrv, time.Second*5)
	consumerService.RegSNRPCProto(snrpcSrv)
	go func() {
		if err := snrpcSrv.Serve(); err != nil {
			panic(err)
		}
	}()

	etcdCli, err := util.GetOrNewEtcdCli()
	if err != nil {
		panic(err)
	}

	if err = initHALogSync(); err != nil {
		panic(err)
	}

	topicMgr, err := mgr.NewTopicMgr(etcdCli, disc, func(topic, subscriber, consumer string, timeout time.Duration, msg *engine.FileMsg) error {
		if _, err := consumerService.Consume(consumer, topic, subscriber, timeout, msg); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	go func() {
		topicMgr.Start()
	}()

	err = microgosuit.ServeGrpc(context.Background(), &microgosuit.ServeGrpcReq{
		SrvName:              discover.SrvNameBroker,
		RegDiscoverKeyPrefix: discover.SrvNamePrefix,
		IpVar:                sysCfg.Host,
		Port:                 sysCfg.Port2,
		RegisterCustomServiceServerFunc: func(server *grpc.Server) error {
			broker.RegisterBrokerServer(server, handler.NewBroker(topicMgr))
			return nil
		},
		BeforeRegDiscover: func(_ discovery.Discovery, node *discovery.Node) error {
			nodeDesc, err := json.Marshal(&ha.Node{NodeGrp: sysCfg.NodeGrp})
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

func initHALogSync() error {
	hALogSync, err := synclog.NewSync(syscfg.MustCfg().DataDir)
	if err != nil {
		return err
	}

	engine.OnOutputFile = func(fileName string, buf []byte, fileOffset uint32, extra *engine.OutputExtra) error {
		var syncMsgLogItem *ha.SyncMsgFileLogItem

		if extra.Topic == "" && extra.Subscriber == "" {
			return nil
		}

		if extra.Subscriber == "" {
			syncMsgLogItem = &ha.SyncMsgFileLogItem{
				Topic:      extra.Topic,
				FileBuf:    buf,
				FileName:   fileName,
				CreatedAt:  extra.ContentCreatedAt,
				FileOffset: fileOffset,
			}
		} else {
			syncMsgLogItem = &ha.SyncMsgFileLogItem{
				Topic:      extra.Topic,
				Subscriber: extra.Subscriber,
				FileBuf:    buf,
				FileName:   fileName,
				CreatedAt:  extra.ContentCreatedAt,
				FileOffset: fileOffset,
			}
		}

		switch true {
		case strings.Contains(fileName, engine.IdxFileSuffix):
			syncMsgLogItem.MsgFileType = ha.MsgFileType_MsgFileTypeIdx
		case strings.Contains(fileName, engine.DataFileSuffix):
			syncMsgLogItem.MsgFileType = ha.MsgFileType_MsgFileTypeData
		case strings.Contains(fileName, engine.FinishFileSuffix):
			syncMsgLogItem.MsgFileType = ha.MsgFileType_MsgFileTypeFinish
		case strings.Contains(fileName, engine.LoadBootFileSuffix):
			syncMsgLogItem.MsgFileType = ha.MsgFileType_MsgFileTypeLoadBoot
		case strings.Contains(fileName, engine.MsgIdFileSuffix):
			syncMsgLogItem.MsgFileType = ha.MsgFileType_MsgFileTypeMsgId
		}

		if err := hALogSync.Write(syncMsgLogItem); err != nil {
			return err
		}

		return nil
	}

	return nil
}

func parseCmdToInitSysCfg() error {
	cfgFilePath := flag.String("c", "", "server config file path")
	flag.Parse()

	if err := syscfg.Init(*cfgFilePath); err != nil {
		return err
	}

	return nil
}
