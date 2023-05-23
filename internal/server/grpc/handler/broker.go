package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/995933447/bucketmq/internal/engine"
	"github.com/995933447/bucketmq/internal/mgr"
	"github.com/995933447/bucketmq/internal/syscfg"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/bucketmq/pkg/rpc/broker"
	"github.com/995933447/bucketmq/pkg/rpc/errs"
	"github.com/995933447/bucketmq/pkg/rpc/health"
	"github.com/995933447/microgosuit/grpcsuit"
	"google.golang.org/grpc"
	"time"
)

var _ broker.BrokerServer = (*Broker)(nil)
var _ health.HealthReporterServer = (*Broker)(nil)

func NewBroker(topicMgr *mgr.TopicMgr) *Broker {
	return &Broker{
		TopicMgr: topicMgr,
	}
}

type Broker struct {
	*mgr.TopicMgr
}

func (b Broker) GetTopic(ctx context.Context, req *broker.GetTopicReq) (*broker.GetTopicResp, error) {
	topicCfg, err := b.TopicMgr.GetTopicCfg(req.Topic)
	if err != nil {
		return nil, err
	}

	return &broker.GetTopicResp{
		Topic: &broker.Topic{
			Topic:       topicCfg.Name,
			NodeGrp:     topicCfg.NodeGrp,
			MaxMsgBytes: topicCfg.MaxMsgBytes,
		},
	}, nil
}

func (b Broker) Ping(ctx context.Context, req *health.PingReq) (*health.PingResp, error) {
	return &health.PingResp{}, nil
}

func (b Broker) Pub(ctx context.Context, req *broker.PubReq) (*broker.PubResp, error) {
	topicCfg, err := b.TopicMgr.GetTopicCfg(req.Topic)
	if err != nil {
		return nil, err
	}

	sysCfg := syscfg.MustCfg()
	if topicCfg.NodeGrp == sysCfg.NodeGrp {
		err = b.TopicMgr.Pub(req.Topic, &engine.Msg{
			Buf: req.Msg,
		})
		if err != nil {
			if err == mgr.ErrTopicNotFound {
				return nil, errs.GRPCErr(errs.ErrCode_ErrCodeTopicNotFound, "")
			}
			return nil, err
		}

		return &broker.PubResp{}, nil
	}

	brokerCfg, err := b.Discovery.Discover(ctx, discover.SrvNameBroker)
	if err != nil {
		return nil, err
	}

	for _, node := range brokerCfg.Nodes {
		var nodeDesc broker.Node
		err = json.Unmarshal([]byte(node.Extra), &nodeDesc)
		if err != nil {
			return nil, err
		}

		if nodeDesc.NodeGrp != sysCfg.NodeGrp {
			continue
		}

		return func() (*broker.PubResp, error) {
			conn, err := grpc.Dial(fmt.Sprintf("%s:%d", node.Host, node.Port), grpcsuit.NotRoundRobinDialOpts...)
			if err != nil {
				return nil, err
			}

			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()

			_, err = broker.NewBrokerClient(conn).Pub(ctx, &broker.PubReq{
				Topic: req.Topic,
				Msg:   req.Msg,
			})
			if err != nil {
				return nil, err
			}

			return &broker.PubResp{}, nil
		}()
	}

	return nil, errs.GRPCErr(errs.ErrCode_ErrCodeNotNodeGrpAvailable, "")
}

func (b Broker) RegTopic(_ context.Context, req *broker.RegTopicReq) (*broker.RegTopicResp, error) {
	err := b.TopicMgr.RegTopic(&mgr.TopicCfg{
		Name:        req.Topic.Topic,
		NodeGrp:     req.Topic.NodeGrp,
		MaxMsgBytes: req.Topic.MaxMsgBytes,
	})
	if err != nil {
		return nil, err
	}
	return &broker.RegTopicResp{}, nil
}

func (b Broker) RegSubscriber(_ context.Context, req *broker.RegSubscriberReq) (*broker.RegSubscriberResp, error) {
	err := b.TopicMgr.RegSubscriber(&mgr.SubscriberCfg{
		Topic:                        req.Subscriber.Topic,
		Name:                         req.Subscriber.Subscriber,
		Consumer:                     req.Subscriber.Consumer,
		StartMsgId:                   req.Subscriber.StartMsgId,
		LodeMode:                     engine.LoadMsgMode(req.Subscriber.LoadMode),
		LoadMsgBootId:                req.Subscriber.LoadMsgBootId,
		ConcurConsumeNum:             req.Subscriber.ConcurConsumeNum,
		MaxConcurConsumeNumPerBucket: req.Subscriber.MaxConcurConsumeNumPerBucket,
		MsgWeight:                    engine.MsgWeight(req.Subscriber.MsgWeight),
		IsSerial:                     req.Subscriber.IsSerial,
		MaxConsumeMs:                 req.Subscriber.MaxConsumeMs,
	})
	if err != nil {
		return nil, err
	}
	return &broker.RegSubscriberResp{}, nil
}
