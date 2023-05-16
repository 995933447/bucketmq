package mgr

import (
	"github.com/995933447/bucketmq/internal/syscfg"
	"github.com/995933447/microgosuit/discovery"
	"github.com/995933447/microgosuit/discovery/impl/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sync"
	"time"
)

var (
	discover        discovery.Discovery
	initDiscoveryMu sync.RWMutex
)

func InitDiscovery() error {
	initDiscoveryMu.RLock()
	if discover != nil {
		initDiscoveryMu.RUnlock()
		return nil
	}
	initDiscoveryMu.RUnlock()

	initDiscoveryMu.Lock()
	defer initDiscoveryMu.Unlock()

	if discover != nil {
		return nil
	}

	sysCfg := syscfg.MustCfg()
	var err error
	discover, err = etcd.NewDiscovery(time.Second*5, clientv3.Config{
		Endpoints:   sysCfg.Etcd.Endpoints,
		DialTimeout: time.Duration(sysCfg.Etcd.ConnectTimeoutMs) * time.Millisecond,
	})
	if err != nil {
		return err
	}
	return nil
}

func MustDiscovery() discovery.Discovery {
	if discover == nil {
		panic("discovery not init")
	}
	return discover
}
