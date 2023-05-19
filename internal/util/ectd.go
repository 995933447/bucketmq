package util

import (
	"github.com/995933447/bucketmq/internal/syscfg"
	clientv3 "go.etcd.io/etcd/client/v3"
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
