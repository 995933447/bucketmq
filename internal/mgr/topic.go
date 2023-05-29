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
	"sync/atomic"
)

const (
	topicEtcdKeyPrefix      = "bucketmqtopic_"
	subscriberEtcdKeyPrefix = "bucketmqsubsriber_"
)

var (
	ErrTopicNotFound          = errors.New("topic not found")
	ErrTopicBoundOtherNodeGrp = errors.New("topic bound other node group")
)

type TopicCfg struct {
	Name        string
	NodeGrp     string
	MaxMsgBytes uint32

	version int64
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

	version int64
}

type topic struct {
	name                  string
	writer                *engine.Writer
	cfg                   *TopicCfg
	subscribers           map[string]*engine.Subscriber
	SubscriberCfgs        map[string]*SubscriberCfg
	opSubscriberMuFactory *runtimeutil.MulElemMuFactory
	opSubscribersMu       sync.RWMutex
}

func (t *topic) close() {
	t.writer.Exit()
	for _, subscriber := range t.subscribers {
		subscriber.Exit()
	}
}

func NewTopicMgr(etcdCli *clientv3.Client, disc discovery.Discovery, consumeFunc engine.ConsumeFunc) (*TopicMgr, error) {
	mgr := &TopicMgr{
		Discovery:        disc,
		topics:           map[string]*topic{},
		etcdCli:          etcdCli,
		consumeFunc:      consumeFunc,
		opTopicMuFactory: runtimeutil.NewMulElemMuFactory(),
	}

	topicCfgs, err := mgr.listTopicCfgsFromEtcd()
	if err != nil {
		return nil, err
	}

	go mgr.loop()

	for _, cfg := range topicCfgs {
		opTopicMu := mgr.opTopicMuFactory.MakeOrGetSpecElemMu(cfg.Name)
		opTopicMu.Lock()

		mgr.opTopicsMu.RLock()
		if topic, ok := mgr.topics[cfg.Name]; ok {
			mgr.opTopicsMu.RUnlock()
			if topic.cfg.version > cfg.version {
				opTopicMu.RUnlock()
				continue
			}
		}
		mgr.opTopicsMu.RUnlock()

		topic, err := mgr.newTopic(cfg)
		if err != nil {
			opTopicMu.Unlock()
			mgr.exitLoopCh <- struct{}{}
			mgr.Exit()
			return nil, err
		}

		mgr.topics[cfg.Name] = topic
		opTopicMu.Unlock()
	}

	return mgr, nil
}

type TopicMgr struct {
	discovery.Discovery
	etcdCli          *clientv3.Client
	topics           map[string]*topic
	opTopicsMu       sync.RWMutex
	opTopicMuFactory *runtimeutil.MulElemMuFactory
	consumeFunc      engine.ConsumeFunc
	isRunning        atomic.Bool
	exitLoopCh       chan struct{}
}

func (m *TopicMgr) onTopicCfgUpdated(cfg *TopicCfg) error {
	opTopicMu := m.opTopicMuFactory.MakeOrGetSpecElemMu(cfg.Name)
	opTopicMu.Lock()
	defer opTopicMu.Unlock()

	var (
		topic *topic
		err   error
		ok    bool
	)
	m.opTopicsMu.Lock()
	if topic, ok = m.topics[cfg.Name]; ok {
		m.opTopicsMu.RUnlock()
		if cfg.version <= topic.cfg.version {
			return nil
		}
		topic.cfg.version = cfg.version
	} else {
		m.opTopicsMu.RUnlock()
		if topic, err = m.newTopic(cfg); err != nil {
			return err
		}
	}

	m.opTopicsMu.Lock()
	m.topics[cfg.Name] = topic
	m.opTopicsMu.Unlock()

	return nil
}

func (m *TopicMgr) onTopicCfgDel(cfg *TopicCfg) error {
	opTopicMu := m.opTopicMuFactory.MakeOrGetSpecElemMu(cfg.Name)
	opTopicMu.Lock()
	defer opTopicMu.Unlock()

	m.opTopicsMu.RLock()
	topic, ok := m.topics[cfg.Name]
	if !ok {
		m.opTopicsMu.RUnlock()
		return nil
	}

	if topic.cfg.version > cfg.version {
		m.opTopicsMu.RUnlock()
		return nil
	}

	topic.close()

	m.opTopicsMu.Lock()
	defer m.opTopicsMu.Unlock()

	delete(m.topics, cfg.Name)

	return nil
}

func (m *TopicMgr) onSubscriberCfgDel(cfg *SubscriberCfg) error {
	m.opTopicsMu.RLock()

	topic, ok := m.topics[cfg.Topic]
	if !ok {
		m.opTopicsMu.RUnlock()
		return nil
	}

	opTopicMu := m.opTopicMuFactory.MakeOrGetSpecElemMu(cfg.Topic)
	opTopicMu.Lock()
	defer opTopicMu.Unlock()

	m.opTopicsMu.RUnlock()

	if origCfg, ok := topic.SubscriberCfgs[cfg.Name]; ok && origCfg.version > cfg.version {
		return nil
	}

	if subscriber, ok := topic.subscribers[cfg.Name]; ok {
		subscriber.Exit()
	}

	delete(topic.subscribers, cfg.Name)
	delete(topic.SubscriberCfgs, cfg.Name)

	return nil
}

func (m *TopicMgr) onSubscriberCfgUpdated(cfg *SubscriberCfg) error {
	m.opTopicsMu.RLock()

	topic, ok := m.topics[cfg.Topic]
	if !ok {
		m.opTopicsMu.RUnlock()
		return nil
	}

	opTopicMu := m.opTopicMuFactory.MakeOrGetSpecElemMu(cfg.Topic)
	opTopicMu.Lock()
	defer opTopicMu.Unlock()

	m.opTopicsMu.RUnlock()

	if origCfg, ok := topic.SubscriberCfgs[cfg.Name]; ok && origCfg.version >= cfg.version {
		return nil
	}

	if subscriber, ok := topic.subscribers[cfg.Name]; ok {
		subscriber.Exit()
	}

	watchWrittenCh := make(chan uint64)
	subscriber, err := engine.NewSubscriber(m.Discovery, &engine.SubscriberCfg{
		LodeMode:                     cfg.LodeMode,
		LoadMsgBootId:                cfg.LoadMsgBootId,
		StartMsgId:                   cfg.StartMsgId,
		BaseDir:                      syscfg.MustCfg().DataDir,
		Topic:                        cfg.Topic,
		Name:                         cfg.Name,
		Consumer:                     cfg.Consumer,
		MsgWeight:                    cfg.MsgWeight,
		ConcurConsumeNum:             cfg.ConcurConsumeNum,
		MaxConcurConsumeNumPerBucket: cfg.MaxConcurConsumeNumPerBucket,
		IsSerial:                     cfg.IsSerial,
		MaxConsumeMs:                 cfg.MaxConsumeMs,
		WatchWrittenCh:               watchWrittenCh,
	}, m.consumeFunc)
	if err != nil {
		util.Logger.Error(nil, err)
		return nil
	}

	topic.writer.AddAfterWriteCh(watchWrittenCh)

	topic.subscribers[cfg.Name] = subscriber
	topic.SubscriberCfgs[cfg.Name] = cfg

	return nil
}

func (m *TopicMgr) loop() {
	sysCfg := syscfg.MustCfg()
	topicEvtCh := m.etcdCli.Watch(context.Background(), m.getTopicEtcdKeyPrefix())
	subscriberEvtCh := m.etcdCli.Watch(context.Background(), m.getSubscriberEtcdKeyPrefix())
	for {
		select {
		case <-m.exitLoopCh:
			goto out
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
					if err = m.onTopicCfgUpdated(&cfg); err != nil {
						util.Logger.Error(nil, err)
					}
					continue
				}

				if evt.Type == clientv3.EventTypeDelete {
					if err = m.onTopicCfgDel(&cfg); err != nil {
						util.Logger.Error(nil, err)
					}
					continue
				}
			}
		case evtResp := <-subscriberEvtCh:
			for _, evt := range evtResp.Events {
				var cfg SubscriberCfg
				if err := json.Unmarshal(evt.Kv.Value, &cfg); err != nil {
					util.Logger.Error(nil, err)
					continue
				}

				if evt.Type == clientv3.EventTypePut {
					if err := m.onSubscriberCfgUpdated(&cfg); err != nil {
						util.Logger.Error(nil, err)
						continue
					}
				}

				if evt.Type == clientv3.EventTypeDelete {
					if err := m.onSubscriberCfgDel(&cfg); err != nil {
						util.Logger.Error(nil, err)
						continue
					}
				}
			}
		}
	}
out:
	return
}

func (m *TopicMgr) newTopic(cfg *TopicCfg) (*topic, error) {
	topic := &topic{
		name:           cfg.Name,
		cfg:            cfg,
		subscribers:    map[string]*engine.Subscriber{},
		SubscriberCfgs: map[string]*SubscriberCfg{},
	}

	sysCfg := syscfg.MustCfg()
	writerCfg := &engine.WriterCfg{
		Topic:             cfg.Name,
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
		watchWrittenCh := make(chan uint64)
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
			WatchWrittenCh:               watchWrittenCh,
		}, m.consumeFunc)
		if err != nil {
			return nil, err
		}

		topic.writer.AddAfterWriteCh(watchWrittenCh)

		topic.SubscriberCfgs[subscriberCfg.Name] = subscriberCfg
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

func (m *TopicMgr) Start() {
	if m.isRunning.Load() {
		return
	}

	m.isRunning.Store(true)

	for _, topic := range m.topics {
		for _, subscriber := range topic.subscribers {
			go subscriber.Start()
		}
	}
}

func (m *TopicMgr) Exit() {
	if !m.isRunning.Load() {
		return
	}

	m.isRunning.Store(false)

	for _, topic := range m.topics {
		for _, subscriber := range topic.subscribers {
			subscriber.Exit()
		}
	}
}

func (m *TopicMgr) RegSubscriber(cfg *SubscriberCfg) error {
	if err := m.checkTopicOrSubscriberName(cfg.Topic); err != nil {
		return err
	}

	if err := m.checkTopicOrSubscriberName(cfg.Name); err != nil {
		return err
	}

	origCfg, exist, err := m.querySubscriberCfgFromEtcd(cfg.Topic, cfg.Name)
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
		cfg.ConcurConsumeNum != origCfg.ConcurConsumeNum ||
		cfg.MaxConsumeMs != origCfg.MaxConsumeMs ||
		cfg.Consumer != origCfg.Consumer ||
		cfg.IsSerial != origCfg.IsSerial {
		needUpdate = true
	}

	if needUpdate {
		_, err = m.atomicPersistSubscriber(origCfg.version, cfg)
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

	origCfg, exist, err := m.queryTopicCfgFromEtcd(cfg.Name)
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

	opTopicMu := m.opTopicMuFactory.MakeOrGetSpecElemMu(cfg.Name)
	opTopicMu.Lock()
	defer opTopicMu.Unlock()

	succ, err := m.atomicPersistTopic(cfg.version, cfg)
	if err != nil {
		return err
	}

	if !succ {
		return nil
	}

	m.opTopicsMu.Lock()
	defer m.opTopicsMu.Unlock()

	if topic, ok := m.topics[cfg.Name]; ok {
		if topic.cfg.version >= cfg.version {
			return nil
		}
		topic.cfg = cfg
		return nil
	}

	m.topics[cfg.Name], err = m.newTopic(cfg)
	if err != nil {
		return err
	}

	return nil
}

func (m *TopicMgr) UnRegTopic(cfg *TopicCfg) error {
	if err := m.checkTopicOrSubscriberName(cfg.Name); err != nil {
		return err
	}

	origCfg, exist, err := m.queryTopicCfgFromEtcd(cfg.Name)
	if err != nil {
		return err
	}

	if !exist {
		return nil
	}

	opTopicMu := m.opTopicMuFactory.MakeOrGetSpecElemMu(cfg.Name)
	opTopicMu.Lock()
	defer opTopicMu.Unlock()

	succ, err := m.atomicDelTopic(origCfg.version, cfg)
	if err != nil {
		return err
	}

	if !succ {
		return nil
	}

	m.opTopicsMu.RLock()
	if topic, ok := m.topics[cfg.Name]; !ok {
		m.opTopicsMu.RUnlock()
	} else {
		m.opTopicsMu.RUnlock()
		topic.close()
	}

	m.opTopicsMu.Lock()
	defer m.opTopicsMu.Unlock()

	delete(m.topics, cfg.Name)

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

func (m *TopicMgr) Pub(topic string, msg *engine.Msg) error {
	t, ok := m.topics[topic]
	if !ok {
		return ErrTopicNotFound
	}

	if msg.WriteMsgResWait != nil {
		msg.WriteMsgResWait.Wg.Add(1)
	}

	t.writer.Write(msg)

	if msg.WriteMsgResWait != nil {
		msg.WriteMsgResWait.Wg.Wait()
		if msg.WriteMsgResWait.Err != nil {
			return msg.WriteMsgResWait.Err
		}
	}

	return nil
}

func (m *TopicMgr) GetTopicCfg(topic string) (*TopicCfg, error) {
	if err := m.checkTopicOrSubscriberName(topic); err != nil {
		return nil, err
	}

	cfg, exist, err := m.queryTopicCfgFromEtcd(topic)
	if err != nil {
		return nil, err
	}

	if !exist {
		return nil, ErrTopicNotFound
	}

	return cfg, nil
}

func (m *TopicMgr) listTopicCfgsFromEtcd() ([]*TopicCfg, error) {
	getResp, err := m.etcdCli.Get(context.Background(), m.getTopicEtcdKeyPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var cfgs []*TopicCfg
	for _, kv := range getResp.Kvs {
		var cfg TopicCfg

		err = json.Unmarshal(kv.Value, &cfg)
		if err != nil {
			return nil, err
		}

		cfg.version = kv.Version

		cfgs = append(cfgs, &cfg)
	}

	return cfgs, nil
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

		cfg.version = kv.Version

		cfgs = append(cfgs, &cfg)
	}

	return cfgs, nil
}

func (m *TopicMgr) querySubscriberCfgFromEtcd(topic, subscriber string) (*SubscriberCfg, bool, error) {
	getResp, err := m.etcdCli.Get(context.Background(), m.subscriberToEtcdKey(topic, subscriber))
	if err != nil {
		return nil, false, err
	}

	if len(getResp.Kvs) == 0 {
		return nil, false, nil
	}

	var cfg SubscriberCfg
	err = json.Unmarshal(getResp.Kvs[0].Value, &cfg)
	if err != nil {
		return nil, false, err
	}

	cfg.version = getResp.Kvs[0].Version

	return &cfg, true, nil
}

func (m *TopicMgr) queryTopicCfgFromEtcd(topic string) (*TopicCfg, bool, error) {
	getResp, err := m.etcdCli.Get(context.Background(), m.topicToEtcdKey(topic))
	if err != nil {
		return nil, false, err
	}

	if len(getResp.Kvs) == 0 {
		return nil, false, nil
	}

	var cfg TopicCfg
	err = json.Unmarshal(getResp.Kvs[0].Value, &cfg)
	if err != nil {
		return nil, false, err
	}

	cfg.version = getResp.Kvs[0].ModRevision

	return &cfg, true, nil
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

func (m *TopicMgr) atomicDelTopic(version int64, cfg *TopicCfg) (bool, error) {
	key := m.topicToEtcdKey(cfg.Name)
	resp, err := m.etcdCli.
		Txn(context.Background()).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", version)).
		Then(clientv3.OpDelete(key)).
		Commit()
	if err != nil {
		return false, err
	}

	if resp.Succeeded {
		cfg.version = resp.Responses[1].GetResponseRange().Kvs[0].ModRevision
	}

	return resp.Succeeded, nil
}

func (m *TopicMgr) atomicPersistTopic(version int64, cfg *TopicCfg) (bool, error) {
	cfgJ, err := json.Marshal(cfg)
	if err != nil {
		return false, err
	}

	key := m.topicToEtcdKey(cfg.Name)
	resp, err := m.etcdCli.
		Txn(context.Background()).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", version)).
		Then(clientv3.OpPut(key, string(cfgJ))).
		Commit()
	if err != nil {
		return false, err
	}

	if resp.Succeeded {
		cfg.version = resp.Responses[1].GetResponseRange().Kvs[0].ModRevision
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
		If(clientv3.Compare(clientv3.ModRevision(key), "=", version)).
		Then(clientv3.OpPut(key, string(cfgJ))).
		Commit()
	if err != nil {
		return false, err
	}

	if resp.Succeeded {
		cfg.version = resp.Responses[1].GetResponseRange().Kvs[0].ModRevision
	}

	return resp.Succeeded, nil
}
