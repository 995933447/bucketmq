package cli

import (
	"context"
	"encoding/json"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/bucketmq/pkg/rpc/broker"
	"github.com/995933447/bucketmq/pkg/rpc/consumer"
	"github.com/995933447/bucketmq/pkg/rpc/health"
	"github.com/995933447/bucketmq/pkg/rpc/snrpc"
	"github.com/995933447/microgosuit/discovery"
	"github.com/golang/protobuf/proto"
	"time"
)

type Consumer struct {
	cli                *Cli
	name               string
	topicToSNRPCCliMap map[string]*snrpc.Cli
}

func (c *Consumer) Subscribe(cfg *broker.Subscriber, handleFunc snrpc.HandleMsgFunc) error {
	cfg.Consumer = c.name
	grpcReq := &broker.RegSubscriberReq{
		Subscriber: cfg,
	}
	if err := grpcReq.Validate(); err != nil {
		return err
	}

	if _, ok := c.topicToSNRPCCliMap[cfg.Topic]; !ok {
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

		snprpcCli, err := snrpc.NewCli(destNode.Host, destNode.Port)
		if err != nil {
			return err
		}

		err = snprpcCli.Call(uint32(snrpc.SNRPCProto_SNRPCProtoConsumerConnect), time.Second*5, &consumer.ConnSNSrvReq{
			SN: snrpc.GenSN(),
		}, &consumer.ConnSNSrvResp{})
		if err != nil {
			return err
		}

		snprpcCli.RegProto(uint32(snrpc.SNRPCProto_SNRPCProtoConsume), &consumer.ConsumeReq{}, handleFunc)
		snprpcCli.RegProto(uint32(snrpc.SNRPCProto_SNRPCProtoHeartBeat), &health.PingReq{}, func(req proto.Message) (proto.Message, error) {
			return &health.PingResp{}, nil
		})

		c.topicToSNRPCCliMap[cfg.Topic] = snprpcCli
	}

	_, err := c.cli.BrokerClient.RegSubscriber(context.Background(), grpcReq)
	if err != nil {
		return err
	}

	return nil
}
