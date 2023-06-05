package util

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/995933447/bucketmq/internal/syscfg"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/bucketmq/pkg/rpc/ha"
	"github.com/995933447/gonetutil"
	"github.com/995933447/microgosuit/discovery"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"sync"
	"time"
)

var (
	newEtcdCliMu sync.RWMutex
	etcdCli      *clientv3.Client
)

func GetOrNewEtcdCli() (*clientv3.Client, error) {
	newEtcdCliMu.RLock()
	if etcdCli != nil {
		newEtcdCliMu.RUnlock()
		return etcdCli, nil
	}
	newEtcdCliMu.RUnlock()

	newEtcdCliMu.Lock()
	defer newEtcdCliMu.Unlock()

	if etcdCli != nil {
		return etcdCli, nil
	}

	sysCfg := syscfg.MustCfg()
	var err error
	etcdCli, err = clientv3.New(clientv3.Config{
		Endpoints:   sysCfg.Endpoints,
		DialTimeout: time.Duration(sysCfg.ConnectTimeoutMs) * time.Millisecond,
	})
	if err != nil {
		return nil, err
	}

	return etcdCli, nil
}

func DistributeLockForUpdateDiscovery(etcdCli *clientv3.Client) (func(), error) {
	concurSess, err := concurrency.NewSession(etcdCli, concurrency.WithTTL(5))
	if err != nil {
		return nil, err
	}

	sysCfg := syscfg.MustCfg()
	muCli := concurrency.NewMutex(concurSess, fmt.Sprintf("bucketMQ_%s", sysCfg.Cluster))

	if err = muCli.Lock(context.Background()); err != nil {
		return nil, err
	}

	return func() {
		if err = muCli.Unlock(context.Background()); err != nil {
			Logger.Error(nil, err)
		}
	}, nil
}

func DistributeLockNodeGrpForUpdateDiscovery(etcdCli *clientv3.Client) (func(), error) {
	concurSess, err := concurrency.NewSession(etcdCli, concurrency.WithTTL(5))
	if err != nil {
		return nil, err
	}

	sysCfg := syscfg.MustCfg()
	muCli := concurrency.NewMutex(concurSess, fmt.Sprintf("bucketMQ_%s_%s", sysCfg.Cluster, sysCfg.NodeGrp))

	if err = muCli.Lock(context.Background()); err != nil {
		return nil, err
	}

	return func() {
		if err = muCli.Unlock(context.Background()); err != nil {
			Logger.Error(nil, err)
		}
	}, nil
}

func DiscoverMyNode(disc discovery.Discovery) (*discovery.Node, bool, error) {
	sysCfg := syscfg.MustCfg()
	curHost, err := gonetutil.EvalVarToParseIp(sysCfg.Host)
	if err != nil {
		Logger.Error(context.Background(), err)
		return nil, false, err
	}

	brokerCfg, err := disc.Discover(context.Background(), discover.SrvNameBroker)
	if err != nil {
		Logger.Error(nil, err)
		return nil, false, err
	}

	for _, node := range brokerCfg.Nodes {
		var nodeDesc ha.Node
		err = json.Unmarshal([]byte(node.Extra), &nodeDesc)
		if err != nil {
			Logger.Error(context.Background(), err)
			return nil, false, err
		}

		nodeDesc.IsMaster = false

		if node.Host == curHost && node.Port == sysCfg.Port2 {
			return node, true, nil
		}
	}

	return nil, false, nil
}
