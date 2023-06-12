package syscfg

import (
	"github.com/995933447/confloader"
	"github.com/995933447/microgosuit/env"
	"sync"
	"time"
)

type Cfg struct {
	*env.Meta
	DataDir           string `json:"data_dir"`
	NodeGrp           string `json:"node_grp"`
	Cluster           string `json:"cluster"`
	IdxFileMaxItemNum uint32 `json:"idx_file_max_item_num"`
	DataFileMaxSize   string `json:"data_file_max_size"`
	Host              string `json:"host"`
	Port              int    `json:"port"`
	Port2             int    `json:"port_2"`
}

var (
	cfg       *Cfg
	initCfgMu sync.RWMutex
)

func Init(cfgFilePath string) error {
	initCfgMu.Lock()
	defer initCfgMu.Unlock()
	if cfg != nil {
		return nil
	}

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
