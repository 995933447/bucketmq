package mgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/995933447/bucketmq/internal/engine"
	"github.com/995933447/bucketmq/internal/syscfg"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/microgosuit/discovery"
	"github.com/995933447/runtimeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sync"
	"time"
)

const (
	topicEtcdKeyPrefix      = "bucketmqtopic_"
	subscriberEtcdKeyPrefix = "bucketmqsubsriber_"
)

var (
	ErrTopicNotFound          = errors.New("topic conf not found")
	ErrTopicBoundOtherNodeGrp = errors.New("topic bound other node group")
)

type TopicCfg struct {
	Name        string
	NodeGrp     string
	MaxMsgBytes uint32
}

type SubscriberCfg struct {
	Topic                        string
	Name                         string
	Consumer                     string
	LoadMsgBootId                uint32
	ConcurConsumeNum             uint32
	MaxConcurConsumeNumPerBucket uint32
	MsgWeight                    engine.MsgWeight
	LodeMode                     engine.LoadMsgMode
	StartMsgId                   uint64
	IsSerial                     bool
	MaxConsumeMs                 uint32
}

var (
	topicMgr       *TopicMgr
	initTopicMgrMu sync.RWMutex
)

func InitTopicMgr(discover discovery.Discovery) error {
	initTopicMgrMu.RLock()
	if topicMgr != nil {
		initTopicMgrMu.RUnlock()
		return nil
	}
	initTopicMgrMu.RUnlock()

	initTopicMgrMu.Lock()
	defer initTopicMgrMu.Unlock()

	if topicMgr != nil {
		return nil
	}

	sysCfg := syscfg.MustCfg()

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   sysCfg.Etcd.Endpoints,
		DialTimeout: time.Duration(sysCfg.Etcd.ConnectTimeoutMs) * time.Millisecond,
	})
	if err != nil {
		return err
	}

	topicMgr, err = NewTopicMgr(etcdCli, discover)
	if err != nil {
		return err
	}

	return nil
}

type topic struct {
	name            string
	writer          *engine.Writer
	subscribers     map[string]*engine.Subscriber
	opSubscribersMu *runtimeutil.MulElemMuFactory
}

func (t *topic) close() {
	t.writer.Exit()
	for _, subscriber := range t.subscribers {
		subscriber.Exit()
	}
}

func NewTopicMgr(etcdCli *clientv3.Client, discover discovery.Discovery) (*TopicMgr, error) {
	mgr := &TopicMgr{
		Discovery: discover,
		topics:    map[string]*topic{},
		etcdCli:   etcdCli,
	}

	go mgr.loop()

	return mgr, nil
}

type TopicMgr struct {
	discovery.Discovery
	etcdCli *clientv3.Client
	topics  map[string]*topic
}

func (m *TopicMgr) loop() {
	sysCfg := syscfg.MustCfg()
	topicEvtCh := m.etcdCli.Watch(context.Background(), m.getTopicEtcdKeyPrefix())
	subscriberEvtCh := m.etcdCli.Watch(context.Background(), m.getSubscriberEtcdKeyPrefix())
	doLoop := func() {
		select {
		case evtResp := <-topicEvtCh:
			for _, evt := range evtResp.Events {
				var cfg TopicCfg
				err := json.Unmarshal(evt.Kv.Value, &cfg)
				if err != nil {
					util.Logger.Error(nil, err)
					continue
				}

				if cfg.NodeGrp != sysCfg.NodeGrp {
					continue
				}

				if evt.Type == clientv3.EventTypePut {
					var (
						topic *topic
						ok    bool
					)
					if topic, ok = m.topics[cfg.Name]; !ok {
						if topic, err = m.newTopic(&cfg); err != nil {
							util.Logger.Error(nil, err)
							continue
						}
					}
					m.topics[cfg.Name] = topic
					continue
				}

				if evt.Type == clientv3.EventTypeDelete {
					topic, ok := m.topics[cfg.Name]
					if ok {
						topic.close()
					}
				}
			}
		case evtResp := <-subscriberEvtCh:
			for _, evt := range evtResp.Events {
				var cfg SubscriberCfg
				err := json.Unmarshal(evt.Kv.Value, &cfg)
				if err != nil {
					util.Logger.Error(nil, err)
					continue
				}

				topic, ok := m.topics[cfg.Topic]
				if !ok {
					topicCfg, _, exist, err := m.queryTopicCfgFromEtcd(cfg.Topic)
					if err != nil {
						util.Logger.Error(nil, err)
						continue
					}

					if !exist {
						continue
					}

					if topicCfg.NodeGrp != sysCfg.NodeGrp {
						continue
					}

					m.topics[cfg.Topic], err = m.newTopic(topicCfg)
					if err != nil {
						util.Logger.Error(nil, err)
						continue
					}
				}

				mu := topic.opSubscribersMu.MakeOrGetSpecElemMu(cfg.Name)
				mu.Lock()
				if evt.Type == clientv3.EventTypePut {
					if subscriber, ok := topic.subscribers[cfg.Name]; ok {
						subscriber.Exit()
					}

					subscriber, err := engine.NewSubscriber(m.Discovery, &engine.SubscriberCfg{
						LodeMode:                     cfg.LodeMode,
						LoadMsgBootId:                cfg.LoadMsgBootId,
						StartMsgId:                   cfg.StartMsgId,
						BaseDir:                      sysCfg.DataDir,
						Topic:                        cfg.Topic,
						Name:                         cfg.Name,
						Consumer:                     cfg.Consumer,
						MsgWeight:                    cfg.MsgWeight,
						ConcurConsumeNum:             cfg.ConcurConsumeNum,
						MaxConcurConsumeNumPerBucket: cfg.MaxConcurConsumeNumPerBucket,
						IsSerial:                     cfg.IsSerial,
						MaxConsumeMs:                 cfg.MaxConsumeMs,
					})
					if err != nil {
						mu.Unlock()
						util.Logger.Error(nil, err)
						continue
					}

					topic.subscribers[cfg.Name] = subscriber
					mu.Unlock()
					continue
				}

				if evt.Type == clientv3.EventTypeDelete {
					if subscriber, ok := topic.subscribers[cfg.Name]; ok {
						subscriber.Exit()
						delete(topic.subscribers, cfg.Name)
					}
				}
				mu.Unlock()
			}
		}
	}
	for {
		doLoop()
	}
}

func (m *TopicMgr) newTopic(cfg *TopicCfg) (*topic, error) {
	topic := &topic{
		name:            cfg.Name,
		subscribers:     map[string]*engine.Subscriber{},
		opSubscribersMu: runtimeutil.NewMulElemMuFactory(),
	}

	sysCfg := syscfg.MustCfg()
	writerCfg := &engine.WriterCfg{
		BaseDir:           sysCfg.DataDir,
		IdxFileMaxItemNum: sysCfg.IdxFileMaxItemNum,
	}

	var err error
	writerCfg.DataFileMaxBytes, err = util.ParseMemSizeStrToBytes(sysCfg.DataFileMaxSize)
	if err != nil {
		return nil, err
	}

	topic.writer = engine.NewWriter(writerCfg)

	subscriberCfgs, err := m.listSubscriberCfgsFromEtcd(cfg.Name)
	if err != nil {
		return nil, err
	}

	for _, subscriberCfg := range subscriberCfgs {
		mu := topic.opSubscribersMu.MakeOrGetSpecElemMu(subscriberCfg.Name)
		mu.Lock()
		topic.subscribers[subscriberCfg.Name], err = engine.NewSubscriber(m.Discovery, &engine.SubscriberCfg{
			LoadMsgBootId:                subscriberCfg.LoadMsgBootId,
			LodeMode:                     subscriberCfg.LodeMode,
			StartMsgId:                   subscriberCfg.StartMsgId,
			Name:                         subscriberCfg.Name,
			Topic:                        subscriberCfg.Topic,
			Consumer:                     subscriberCfg.Consumer,
			BaseDir:                      sysCfg.DataDir,
			ConcurConsumeNum:             subscriberCfg.ConcurConsumeNum,
			MaxConcurConsumeNumPerBucket: subscriberCfg.MaxConcurConsumeNumPerBucket,
			MsgWeight:                    subscriberCfg.MsgWeight,
			IsSerial:                     subscriberCfg.IsSerial,
			MaxConsumeMs:                 subscriberCfg.MaxConsumeMs,
		})
		if err != nil {
			mu.Unlock()
			return nil, err
		}
		mu.Unlock()
	}

	return topic, nil
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

func (m *TopicMgr) RegSubscriber(cfg *SubscriberCfg) error {
	if err := m.checkTopicOrSubscriberName(cfg.Topic); err != nil {
		return err
	}

	if err := m.checkTopicOrSubscriberName(cfg.Name); err != nil {
		return err
	}

	_, _, exist, err := m.queryTopicCfgFromEtcd(cfg.Topic)
	if err != nil {
		return err
	}

	if !exist {
		return ErrTopicNotFound
	}

	origCfg, version, exist, err := m.querySubscriberCfgFromEtcd(cfg.Topic, cfg.Name)
	if err != nil {
		return err
	}

	var needUpdate bool
	if !exist {
		needUpdate = true
	} else if origCfg.Name != cfg.Name ||
		origCfg.LoadMsgBootId != cfg.LoadMsgBootId ||
		cfg.StartMsgId != origCfg.StartMsgId ||
		cfg.LodeMode != origCfg.LodeMode ||
		cfg.MsgWeight != origCfg.MsgWeight ||
		cfg.MaxConcurConsumeNumPerBucket != cfg.MaxConcurConsumeNumPerBucket ||
		cfg.ConcurConsumeNum != origCfg.ConcurConsumeNum {
		needUpdate = true
	}

	if needUpdate {
		_, err = m.atomicPersistSubscriber(version, cfg)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *TopicMgr) RegTopic(cfg *TopicCfg) error {
	if err := m.checkTopicOrSubscriberName(cfg.Name); err != nil {
		return err
	}

	origCfg, version, exist, err := m.queryTopicCfgFromEtcd(cfg.Name)
	if err != nil {
		return err
	}

	var needUpdate bool
	if exist {
		if cfg.NodeGrp != "" && cfg.NodeGrp != origCfg.NodeGrp {
			return ErrTopicBoundOtherNodeGrp
		}
		if cfg.MaxMsgBytes != origCfg.MaxMsgBytes {
			needUpdate = true
		}
	} else {
		cfg.NodeGrp = syscfg.MustCfg().NodeGrp
		needUpdate = true
	}

	if !needUpdate {
		return nil
	}

	_, err = m.atomicPersistTopic(version, cfg)
	if err != nil {
		return err
	}

	return nil
}

func (m *TopicMgr) UnRegTopic(cfg *TopicCfg) error {
	if err := m.checkTopicOrSubscriberName(cfg.Name); err != nil {
		return err
	}

	if _, err := m.etcdCli.Delete(context.Background(), m.topicToEtcdKey(cfg.Name)); err != nil {
		return err
	}

	return nil
}

func (m *TopicMgr) UnRegSubscriber(cfg *SubscriberCfg) error {
	if err := m.checkTopicOrSubscriberName(cfg.Topic); err != nil {
		return err
	}

	if err := m.checkTopicOrSubscriberName(cfg.Name); err != nil {
		return err
	}

	if _, err := m.etcdCli.Delete(context.Background(), m.subscriberToEtcdKey(cfg.Topic, cfg.Name)); err != nil {
		return err
	}

	return nil
}

func (m *TopicMgr) GetTopicCfg(topic string) (*TopicCfg, error) {
	if err := m.checkTopicOrSubscriberName(topic); err != nil {
		return nil, err
	}

	cfg, _, exist, err := m.queryTopicCfgFromEtcd(topic)
	if err != nil {
		return nil, err
	}

	if !exist {
		return nil, ErrTopicNotFound
	}

	return cfg, nil
}

func (m *TopicMgr) listSubscriberCfgsFromEtcd(topic string) ([]*SubscriberCfg, error) {
	key := m.getTopicSubscriberEtcdKeyPrefix(topic)
	getResp, err := m.etcdCli.Get(context.Background(), key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var cfgs []*SubscriberCfg
	for _, kv := range getResp.Kvs {
		var cfg SubscriberCfg
		err = json.Unmarshal(kv.Value, &cfg)
		if err != nil {
			return nil, err
		}

		cfgs = append(cfgs, &cfg)
	}

	return cfgs, nil
}

func (m *TopicMgr) querySubscriberCfgFromEtcd(topic, subscriber string) (*SubscriberCfg, int64, bool, error) {
	getResp, err := m.etcdCli.Get(context.Background(), m.subscriberToEtcdKey(topic, subscriber))
	if err != nil {
		return nil, 0, false, err
	}

	if len(getResp.Kvs) == 0 {
		return nil, 0, false, nil
	}

	var cfg SubscriberCfg
	err = json.Unmarshal(getResp.Kvs[0].Value, &cfg)
	if err != nil {
		return nil, 0, false, err
	}

	return &cfg, getResp.Kvs[0].Version, true, nil
}

func (m *TopicMgr) queryTopicCfgFromEtcd(topic string) (*TopicCfg, int64, bool, error) {
	getResp, err := m.etcdCli.Get(context.Background(), m.topicToEtcdKey(topic))
	if err != nil {
		return nil, 0, false, err
	}

	if len(getResp.Kvs) == 0 {
		return nil, 0, false, nil
	}

	var cfg TopicCfg
	err = json.Unmarshal(getResp.Kvs[0].Value, &cfg)
	if err != nil {
		return nil, 0, false, err
	}

	return &cfg, getResp.Kvs[0].Version, true, nil
}

func (m *TopicMgr) getSubscriberEtcdKeyPrefix() string {
	return fmt.Sprintf(subscriberEtcdKeyPrefix + "cluster_" + syscfg.MustCfg().Cluster + "_")
}

func (m *TopicMgr) getTopicSubscriberEtcdKeyPrefix(topic string) string {
	return m.getTopicEtcdKeyPrefix() + "topic:" + topic + "_"
}

func (m *TopicMgr) subscriberToEtcdKey(topic, subscriber string) string {
	return m.getTopicSubscriberEtcdKeyPrefix(topic) + subscriber
}

func (m *TopicMgr) getTopicEtcdKeyPrefix() string {
	return fmt.Sprintf(topicEtcdKeyPrefix + "cluster_" + syscfg.MustCfg().Cluster + "_")
}

func (m *TopicMgr) topicToEtcdKey(topic string) string {
	return m.getTopicEtcdKeyPrefix() + topic
}

func (m *TopicMgr) atomicPersistTopic(version int64, cfg *TopicCfg) (bool, error) {
	cfgJ, err := json.Marshal(cfg)
	if err != nil {
		return false, err
	}

	key := m.topicToEtcdKey(cfg.Name)
	resp, err := m.etcdCli.
		Txn(context.Background()).
		If(clientv3.Compare(clientv3.Version(key), "=", version)).
		Then(clientv3.OpPut(key, string(cfgJ))).
		Commit()
	if err != nil {
		return false, err
	}

	return resp.Succeeded, nil
}

func (m *TopicMgr) atomicPersistSubscriber(version int64, cfg *SubscriberCfg) (bool, error) {
	cfgJ, err := json.Marshal(cfg)
	if err != nil {
		return false, err
	}

	key := m.subscriberToEtcdKey(cfg.Topic, cfg.Name)
	resp, err := m.etcdCli.
		Txn(context.Background()).
		If(clientv3.Compare(clientv3.Version(key), "=", version)).
		Then(clientv3.OpPut(key, string(cfgJ))).
		Commit()
	if err != nil {
		return false, err
	}

	return resp.Succeeded, nil
}
