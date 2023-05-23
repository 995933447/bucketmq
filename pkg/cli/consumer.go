package cli

import (
	"context"
	"encoding/json"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/bucketmq/pkg/rpc/broker"
	"github.com/995933447/bucketmq/pkg/rpc/consumer"
	"github.com/995933447/bucketmq/pkg/rpc/health"
	"github.com/995933447/bucketmq/pkg/rpc/snrpc"
	"github.com/995933447/microgosuit/discovery"
	"github.com/golang/protobuf/proto"
	"sync"
	"time"
)

type OnConsumerErr func(err error)

type Consumer struct {
	cli                *Cli
	name               string
	topicToSNRPCCliMap map[string]*snrpc.Cli
	opSNRPCClisMu      sync.RWMutex
	onErr              OnConsumerErr
}

func (c *Consumer) Subscribe(cfg *broker.Subscriber, handleFunc snrpc.HandleMsgFunc) error {
	cfg.Consumer = c.name
	grpcReq := &broker.RegSubscriberReq{
		Subscriber: cfg,
	}
	if err := grpcReq.Validate(); err != nil {
		return err
	}

	c.opSNRPCClisMu.RLock()
	if _, ok := c.topicToSNRPCCliMap[cfg.Topic]; ok {
		c.opSNRPCClisMu.RUnlock()
	} else {
		c.opSNRPCClisMu.RUnlock()

		var connectSNPRPCSrv func() error
		connectSNPRPCSrv = func() error {
			getTopicResp, err := c.cli.BrokerClient.GetTopic(context.Background(), &broker.GetTopicReq{
				Topic: cfg.Topic,
			})
			if err != nil {
				return err
			}

			brokerCfg, err := c.cli.discovery.Discover(context.Background(), discover.SrvNameBroker)
			var destNode *discovery.Node
			for _, node := range brokerCfg.Nodes {
				if !node.Available() {
					continue
				}

				var nodeDesc broker.Node
				err = json.Unmarshal([]byte(node.Extra), &nodeDesc)
				if err != nil {
					return err
				}

				if nodeDesc.NodeGrp != getTopicResp.Topic.NodeGrp {
					continue
				}

				if nodeDesc.IsMaster {
					destNode = node
					break
				}
			}

			snrpcCli, err := snrpc.NewCli(destNode.Host, destNode.Port, func(err error) {
				c.opSNRPCClisMu.Lock()
				defer c.opSNRPCClisMu.Unlock()

				for i := 0; i < 3; i++ {
					if err = connectSNPRPCSrv(); err != nil {
						if i < 2 {
							util.Logger.Warnf(nil, "consumer:%s connect broker failed, retry after %d seconds", c.name, i*5)
							time.Sleep(time.Second * time.Duration(i*5))
							continue
						}
						delete(c.topicToSNRPCCliMap, cfg.Topic)
						c.onErr(err)
					}
				}
			})
			if err != nil {
				return err
			}

			err = snrpcCli.Call(uint32(snrpc.SNRPCProto_SNRPCProtoConsumerConnect), time.Second*5, &consumer.ConnSNSrvReq{
				SN: snrpc.GenSN(),
			}, &consumer.ConnSNSrvResp{})
			if err != nil {
				return err
			}

			snrpcCli.RegProto(uint32(snrpc.SNRPCProto_SNRPCProtoConsume), &consumer.ConsumeReq{}, handleFunc)
			snrpcCli.RegProto(uint32(snrpc.SNRPCProto_SNRPCProtoHeartBeat), &health.PingReq{}, func(req proto.Message) (proto.Message, error) {
				return &health.PingResp{}, nil
			})

			c.topicToSNRPCCliMap[cfg.Topic] = snrpcCli

			return nil
		}

		c.opSNRPCClisMu.Lock()
		if _, ok = c.topicToSNRPCCliMap[cfg.Topic]; !ok {
			err := connectSNPRPCSrv()
			if err != nil {
				c.opSNRPCClisMu.Unlock()
				return err
			}
		}
		c.opSNRPCClisMu.Unlock()
	}

	_, err := c.cli.BrokerClient.RegSubscriber(context.Background(), grpcReq)
	if err != nil {
		return err
	}

	return nil
}
