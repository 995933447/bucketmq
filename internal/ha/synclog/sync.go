package synclog

import (
	"context"
	"encoding/json"
	nodegrpha "github.com/995933447/bucketmq/internal/ha"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/bucketmq/pkg/rpc/ha"
	"github.com/995933447/microgosuit/discovery"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

type Sync struct {
	*Writer
	*Consumer
	discovery.Discovery
	etcdCli *clientv3.Client
	baseDir string
}

func (s *Sync) BackupMasterLogMeta() error {
	if !s.needBackupMasterLogMeta.Load() {
		return nil
	}

	meta, err := nodegrpha.GetNodeGrpHAMeta(s.etcdCli)
	if err != nil {
		return err
	}

	defer func() {
		s.needBackupMasterLogMeta.Store(false)
	}()

	meta.MaxSyncLogId = s.Writer.output.msgIdGen.curMaxMsgId
	meta.TermOfMaxSyncLog = nodegrpha.GetCurElectTerm()

	err = nodegrpha.SaveNodeGrpHAMetaByMasterRole(s.etcdCli, meta)
	if err != nil {
		return err
	}

	nodegrpha.MaxSyncedLogId = meta.MaxSyncLogId
	nodegrpha.TermOfMaxSyncedLog = meta.TermOfMaxSyncLog
	nodegrpha.LastSyncedLogAt = uint32(time.Now().Unix())

	unlock, err := util.DistributeLockForUpdateDiscovery(s.etcdCli)
	if err != nil {
		return err
	}

	defer unlock()

	node, exist, err := util.DiscoverMyNode(s.Discovery)
	if err != nil {
		return err
	}

	if !exist {
		return nodegrpha.ErrMasterNodeNotFound
	}

	var nodeDesc ha.Node
	if err = json.Unmarshal([]byte(node.Extra), &nodeDesc); err != nil {
		return err
	}

	nodeDesc.MaxSyncedLogId = meta.MaxSyncLogId
	nodeDesc.TermOfMaxSyncedLog = meta.TermOfMaxSyncLog

	nodeDescJ, err := json.Marshal(nodeDesc)
	if err != nil {
		return err
	}

	node.Extra = string(nodeDescJ)
	if err = s.Discovery.Register(context.Background(), discover.SrvNameBroker, node); err != nil {
		return err
	}

	return nil
}

func NewSync(baseDir string, etcdCli *clientv3.Client) (*Sync, error) {
	sync := &Sync{
		baseDir: baseDir,
		etcdCli: etcdCli,
	}

	var err error
	sync.Writer, err = NewWriter(sync)
	if err != nil {
		return nil, err
	}

	sync.Consumer, err = newConsumer(sync)
	if err != nil {
		return nil, err
	}

	return sync, nil
}
