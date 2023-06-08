package ha

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/995933447/bucketmq/internal/syscfg"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/microgosuit/discovery"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sync/atomic"
)

var (
	ErrLostMaster         = errors.New("current node lost master role")
	ErrMasterNodeNotFound = errors.New("master node not found")
)

var (
	MaxSyncedLogId     uint64
	TermOfMaxSyncedLog uint64
	LastSyncedLogAt    uint32
)

const (
	NodeGrpHAMetaEtcdKeyPrefix          = "bucketMQ_nodeGrp_meta_"
	NodeGrpElectEtcdKeyPrefix           = "bucketMQ_nodeGrp_elect_"
	NodeGrpMasterDiscoveryEtcdKeyPrefix = "bucketMQ_nodeGrp_elect_"
)

var MeAsMasterDiscoverEtcdLeaseCancelFunc context.CancelFunc

var (
	NodeGrpMasterElectEtcdKey              string
	NodeGrpMasterElectEtcdKeyCreateVersion int64
)

type NodeHAMeta struct {
	ElectTerm        uint64
	MaxSyncLogId     uint64
	TermOfMaxSyncLog uint64
}

func GetNodeGrpMasterNodeEtcdKey() string {
	return NodeGrpMasterDiscoveryEtcdKeyPrefix + syscfg.MustCfg().Cluster + "_" + syscfg.MustCfg().NodeGrp
}

func SaveNodeGrpMasterDiscovery(etcdCli *clientv3.Client, node *discovery.Node) error {
	nodeJ, err := json.Marshal(node)
	if err != nil {
		util.Logger.Error(nil, err)
		return err
	}

	grantReleaseResp, err := etcdCli.Grant(context.Background(), 5)
	if err != nil {
		return err
	}

	releaseId := grantReleaseResp.ID

	ctx, cancel := context.WithCancel(etcdCli.Ctx())
	keepAlive, err := etcdCli.KeepAlive(ctx, releaseId)
	if err != nil || keepAlive == nil {
		cancel()
		return err
	}

	putResp, err := etcdCli.
		Txn(context.Background()).
		If(clientv3.Compare(clientv3.CreateRevision(NodeGrpMasterElectEtcdKey), "=", NodeGrpMasterElectEtcdKeyCreateVersion)).
		Then(clientv3.OpPut(GetNodeGrpMasterNodeEtcdKey(), string(nodeJ), clientv3.WithLease(releaseId))).
		Commit()
	if err != nil {
		cancel()
		util.Logger.Error(nil, err)
		return err
	}

	if !putResp.Succeeded {
		cancel()
		RevokeMasterRole()
		return ErrLostMaster
	}

	MeAsMasterDiscoverEtcdLeaseCancelFunc = cancel

	return nil
}

func GetNodeGrpMasterDiscovery(etcdCli *clientv3.Client) (*discovery.Node, error) {
	getResp, err := etcdCli.Get(context.Background(), GetNodeGrpMasterNodeEtcdKey())
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	if len(getResp.Kvs) == 0 {
		return nil, ErrMasterNodeNotFound
	}

	var node discovery.Node
	err = json.Unmarshal(getResp.Kvs[0].Value, &node)
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	return &node, nil
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

func SaveNodeGrpHAMetaByMasterRole(etcdCli *clientv3.Client, meta *NodeHAMeta) error {
	metaJ, err := json.Marshal(meta)
	if err != nil {
		util.Logger.Error(nil, err)
		return err
	}

	putResp, err := etcdCli.
		Txn(context.Background()).
		If(clientv3.Compare(clientv3.CreateRevision(NodeGrpMasterElectEtcdKey), "=", NodeGrpMasterElectEtcdKeyCreateVersion)).
		Then(clientv3.OpPut(GetNodeGrpHAMetaEtcdKey(), string(metaJ))).
		Commit()
	if err != nil {
		util.Logger.Error(nil, err)
		return err
	}

	if !putResp.Succeeded {
		RevokeMasterRole()
		return nil
	}

	return ErrLostMaster
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
	MeAsMasterDiscoverEtcdLeaseCancelFunc()
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
