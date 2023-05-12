package syscfg

import (
	"github.com/995933447/confloader"
	"sync"
	"time"
)

type Cfg struct {
	DataDir string
	NodeGrp string
	Cluster string
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

	return nil
}

func MustCfg() *Cfg {
	if cfg == nil {
		panic("sysCfg not init")
	}
	return cfg
}
