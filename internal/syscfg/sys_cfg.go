package syscfg

import (
	"github.com/995933447/confloader"
	"github.com/gzjjyz/micro/env"
	"sync"
	"time"
)

type EtcdCfg struct {
	ConnectTimeoutMs int32    `json:"connect_timeout_ms"`
	Endpoints        []string `json:"endpoints"`
}

type Cfg struct {
	*env.Meta
	DataDir           string
	NodeGrp           string
	Cluster           string
	IdxFileMaxItemNum uint32
	DataFileMaxSize   string
}

var (
	cfg       *Cfg
	initCfgMu sync.RWMutex
)

func Init(cfgFilePath string) error {
	initCfgMu.RLock()
	if cfg != nil {
		initCfgMu.RUnlock()
		return nil
	}

	initCfgMu.Lock()
	defer initCfgMu.Unlock()

	if cfg != nil {
		return nil
	}

	if cfgFilePath == "" {
		cfgFilePath = defaultCfgFilePath
	}

	var err error
	cfgLoader := confloader.NewLoader(cfgFilePath, time.Second*10, cfg)
	if err = cfgLoader.Load(); err != nil {
		return err
	}

	if err = env.InitMeta(cfgFilePath); err != nil {
		return err
	}

	cfg.Meta = env.MustMeta()

	return nil
}

func MustCfg() *Cfg {
	if cfg == nil {
		panic("sysCfg not init")
	}
	return cfg
}
