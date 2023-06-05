package ha

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/995933447/bucketmq/internal/syscfg"
	"github.com/995933447/bucketmq/internal/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sync/atomic"
)

const (
	NodeGrpHAMetaEtcdKeyPrefix = "bucketMQ_nodeGrp_meta_"
	NodeGrpElectEtcdKeyPrefix  = "bucketMQ_nodeGrp_elect_"
)

var (
	NodeGrpMasterElectEtcdKey              string
	NodeGrpMasterElectEtcdKeyCreateVersion int64
)

type NodeHAMeta struct {
	ElectTerm        uint64
	MaxSyncLogId     uint64
	TermOfMaxSyncLog uint64
}

func GetNodeGrpHAMetaEtcdKey() string {
	return NodeGrpHAMetaEtcdKeyPrefix + syscfg.MustCfg().Cluster + "_" + syscfg.MustCfg().NodeGrp
}

func GetNodeGrpElectEtcdKey() string {
	return NodeGrpElectEtcdKeyPrefix + syscfg.MustCfg().Cluster + "_" + syscfg.MustCfg().NodeGrp
}

func GetNodeGrpHAMeta(etcdCli *clientv3.Client) (*NodeHAMeta, error) {
	key := GetNodeGrpHAMetaEtcdKey()
	getResp, err := etcdCli.Get(context.Background(), key)
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	var meta NodeHAMeta
	if len(getResp.Kvs) == 0 {
		return &meta, nil
	}

	err = json.Unmarshal(getResp.Kvs[0].Value, &meta)
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	return &meta, nil
}

func SaveNodeGrpHAMetaByMasterRole(etcdCli *clientv3.Client, meta *NodeHAMeta) (bool, error) {
	metaJ, err := json.Marshal(meta)
	if err != nil {
		util.Logger.Error(nil, err)
		return false, err
	}

	putResp, err := etcdCli.
		Txn(context.Background()).
		If(clientv3.Compare(clientv3.CreateRevision(NodeGrpMasterElectEtcdKey), "=", NodeGrpMasterElectEtcdKeyCreateVersion)).
		Then(clientv3.OpPut(GetNodeGrpHAMetaEtcdKey(), string(metaJ))).
		Commit()
	if err != nil {
		util.Logger.Error(nil, err)
		return false, err
	}

	if !putResp.Succeeded {
		RevokeMasterRole()
		return false, nil
	}

	return true, nil
}

var (
	curElectTerm uint64
	isMasterRole atomic.Bool
)

func GetCurElectTerm() uint64 {
	return curElectTerm
}

func SwitchMasterRole(term uint64) {
	curElectTerm = term
	isMasterRole.Store(true)
}

func RevokeMasterRole() {
	isMasterRole.Store(false)
}

func IsMasterRole() bool {
	return isMasterRole.Load()
}

func RefreshIsMasterRole(etcdCli *clientv3.Client) (bool, error) {
	getResp, err := etcdCli.Get(context.Background(), NodeGrpMasterElectEtcdKey)
	if err != nil {
		util.Logger.Error(nil, err)
		return false, err
	}

	if len(getResp.Kvs) == 0 {
		return false, nil
	}

	return getResp.Kvs[0].CreateRevision == NodeGrpMasterElectEtcdKeyCreateVersion, nil
}

const MaxSyncLogIdEtcdKeyPrefix = ""

func GetMaxSyncLogIdEtcdKey() string {
	return fmt.Sprintf(MaxSyncLogIdEtcdKeyPrefix + "cluster_" + syscfg.MustCfg().Cluster + "_sync_log_msg_id")
}
