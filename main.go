package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/995933447/bucketmq/internal/engine"
	"github.com/995933447/bucketmq/internal/ha/synclog"
	"github.com/995933447/bucketmq/internal/mgr"
	"github.com/995933447/bucketmq/internal/server/grpc/middleware"
	"github.com/995933447/bucketmq/internal/server/grpc/service"
	"github.com/995933447/bucketmq/internal/server/snrpc"
	snrpchandler "github.com/995933447/bucketmq/internal/server/snrpc/service"
	"github.com/995933447/bucketmq/internal/syscfg"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/bucketmq/pkg/rpc/broker"
	consumerrpc "github.com/995933447/bucketmq/pkg/rpc/consumer"
	"github.com/995933447/bucketmq/pkg/rpc/errs"
	"github.com/995933447/bucketmq/pkg/rpc/ha"
	snrpcx "github.com/995933447/bucketmq/pkg/rpc/snrpc"
	"github.com/995933447/gonetutil"
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

	etcdCli, err := util.GetOrNewEtcdCli()
	if err != nil {
		panic(err)
	}

	disc, err := factory.GetOrMakeDiscovery(discover.SrvNamePrefix)
	if err != nil {
		panic(err)
	}

	go func() {
		if err = mgr.NewHealthChecker(etcdCli, disc, 200, 50000).Run(); err != nil {
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

	hALogSync, err := synclog.NewSync(syscfg.MustCfg().DataDir, etcdCli)
	if err != nil {
		panic(err)
	}
	if err = regHALogSync(hALogSync); err != nil {
		panic(err)
	}

	topicMgr, err := mgr.NewTopicMgr(etcdCli, disc, func(topic, subscriber, consumer string, timeout time.Duration, msg *engine.FileMsg) error {
		consumeReq := &consumerrpc.ConsumeReq{
			Subscriber: subscriber,
			Topic:      topic,
			Consumer:   consumer,
			TimeoutMs:  uint32(timeout / time.Millisecond),
			Data:       msg.GetData(),
			RetriedCnt: msg.GetRetriedCnt(),
		}
		_, err := consumerService.CallbackConsumer(consumeReq)
		if err != nil {
			if rpcErr, ok := errs.ToRPCErr(err); ok && rpcErr.Code == errs.ErrCode_ErrCodeConsumerNotFound {
				brokerCfg, err := disc.Discover(context.Background(), discover.SrvNameBroker)
				if err != nil {
					return err
				}

				myHost, err := gonetutil.EvalVarToParseIp(sysCfg.Host)
				if err != nil {
					return err
				}

				for _, node := range brokerCfg.Nodes {
					if node.Host == myHost && node.Port == sysCfg.Port {
						continue
					}

					cli, err := snrpcx.NewCli(node.Host, node.Port, func(err error) {
						util.Logger.Error(nil, err)
					})
					if err != nil {
						return err
					}

					var consumeResp consumerrpc.ConsumeResp
					err = cli.Call(uint32(snrpcx.SNRPCProto_SNRPCProtoConsume), timeout, consumeReq, &consumeResp)
					if err != nil {
						if rpcErr, ok := errs.ToRPCErr(err); ok && rpcErr.Code == errs.ErrCode_ErrCodeConsumerNotFound {
							continue
						}
						return err
					}

					return nil
				}

				return errs.RPCErr(errs.ErrCode_ErrCodeConsumerNotFound, "")
			}
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	haService, err := service.NewHA(etcdCli, disc)
	if err != nil {
		panic(err)
	}

	err = microgosuit.ServeGrpc(context.Background(), &microgosuit.ServeGrpcReq{
		SrvName:              discover.SrvNameBroker,
		RegDiscoverKeyPrefix: discover.SrvNamePrefix,
		IpVar:                sysCfg.Host,
		Port:                 sysCfg.Port2,
		RegisterCustomServiceServerFunc: func(server *grpc.Server) error {
			ha.RegisterHAServer(server, haService)
			broker.RegisterBrokerServer(server, service.NewBroker(topicMgr))
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
		AfterRegDiscover: func(d discovery.Discovery, node *discovery.Node) error {
			go func() {
				if err = mgr.NewHA(etcdCli, disc, hALogSync, topicMgr, haService).Elect(); err != nil {
					panic(err)
				}
			}()
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

func regHALogSync(hALogSync *synclog.Sync) error {
	engine.LogMsgFileOp = func(fileName string, buf []byte, fileOffset uint32, extra *engine.OutputExtra) error {
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
