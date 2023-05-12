package mgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/995933447/bucketmq/engine"
	"github.com/995933447/bucketmq/syscfg"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	ErrTopicCfgNotFound       = errors.New("topic conf not found")
	ErrTopicBoundOtherNodeGrp = errors.New("topic bound other node group")
)

type TopicCfg struct {
	Name        string
	NodeGrp     string
	MaxMsgBytes uint32
}

type SubscriberCfg struct {
	SubscriberName          string
	LoadMsgBootId           uint32
	Name                    string
	ConsumerNum             uint32
	MaxConsumerNumPerBucket uint32
	MsgWeight               engine.MsgWeight
	LodeMode                engine.LoadMsgMode
	StartMsgId              uint64
}

func newTopic(cfg *TopicCfg) *topic {
	return &topic{
		name: cfg.Name,
		cfg:  cfg,
	}
}

type topic struct {
	name           string
	cfg            *TopicCfg
	subscriberCfgs map[string]*SubscriberCfg
	subscribers    map[string]*engine.Subscriber
}

type TopicMgr struct {
	etcdCli *clientv3.Client
	topics  map[string]*topic
}

func (m *TopicMgr) Run() error {
	sysCfg := syscfg.MustCfg()
	for _, topic := range m.topics {
		for _, subscriberCfg := range topic.subscriberCfgs {
			subscriber, err := engine.NewSubscriber(&engine.SubscriberCfg{
				SubscriberName:          subscriberCfg.SubscriberName,
				LoadMsgBootId:           subscriberCfg.LoadMsgBootId,
				BaseDir:                 sysCfg.DataDir,
				Topic:                   topic.name,
				ConsumerNum:             subscriberCfg.ConsumerNum,
				MaxConsumerNumPerBucket: subscriberCfg.MaxConsumerNumPerBucket,
				MsgWeight:               subscriberCfg.MsgWeight,
				LodeMode:                subscriberCfg.LodeMode,
				StartMsgId:              subscriberCfg.StartMsgId,
			})
			if err != nil {
				return err
			}

			topic.subscribers[subscriberCfg.Name] = subscriber
		}
	}

	return nil
}

func (m *TopicMgr) checkTopicOrSubscriberName(name string) error {
	nameLen := len(name)
	if nameLen == 0 {
		return errors.New("name empty")
	}
	if nameLen > 64 {
		return errors.New("name too long")
	}
	for i := 0; i < nameLen; i++ {
		if name[i] == '.' || name[i] == '/' {
			return errors.New("name has invalid char")
		}
	}
	return nil
}

func (m *TopicMgr) RegTopic(cfg *TopicCfg) error {
	if err := m.checkTopicOrSubscriberName(cfg.Name); err != nil {
		return err
	}

	topic, ok := m.topics[cfg.Name]
	if ok {
		cfg.NodeGrp = syscfg.MustCfg().NodeGrp
		topic.cfg = cfg
	} else {
		origCfg, err := m.queryTopicCfgFromEtcd(cfg.Name)
		if err != nil {
			return err
		}

		if origCfg.NodeGrp != syscfg.MustCfg().NodeGrp {
			cfg.NodeGrp = origCfg.NodeGrp
		}

		topic = newTopic(cfg)
	}

	m.topics[cfg.Name] = topic

	return nil
}

func (m *TopicMgr) queryTopicCfgFromEtcd(topic string) (*TopicCfg, error) {
	getResp, err := m.etcdCli.Get(context.Background(), m.topicToEtcdKey(topic))
	if err != nil {
		return nil, err
	}

	if len(getResp.Kvs) == 0 {
		return nil, ErrTopicCfgNotFound
	}

	var cfg TopicCfg
	err = json.Unmarshal(getResp.Kvs[0].Value, &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (m *TopicMgr) subscriberToEtcdKey(topic, subscriber string) string {
	return fmt.Sprintf("%s_topic:%s_%s", "bucketmqsubscriber_", topic, subscriber)
}

func (m *TopicMgr) topicToEtcdKey(topic string) string {
	return fmt.Sprintf("%s_%s", "bucketmqtopic_", topic)
}

func (m *TopicMgr) atomicPersistTopic(ctx context.Context, topic string, version int64, cfg *TopicCfg) (bool, error) {
	cfgJ, err := json.Marshal(cfg)
	if err != nil {
		return false, err
	}

	key := m.topicToEtcdKey(topic)
	resp, err := m.etcdCli.
		Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", version)).
		Then(clientv3.OpPut(key, string(cfgJ))).
		Commit()
	if err != nil {
		return false, err
	}

	return resp.Succeeded, nil
}

func (m *TopicMgr) atomicPersistSubscriber(ctx context.Context, topic, subscriberName string, version int64, cfg *TopicCfg) (bool, error) {
	cfgJ, err := json.Marshal(cfg)
	if err != nil {
		return false, err
	}

	key := m.subscriberToEtcdKey(topic, subscriberName)
	resp, err := m.etcdCli.
		Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", version)).
		Then(clientv3.OpPut(key, string(cfgJ))).
		Commit()
	if err != nil {
		return false, err
	}

	return resp.Succeeded, nil
}
