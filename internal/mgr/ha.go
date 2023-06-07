package mgr

import (
	"context"
	"encoding/json"
	"github.com/995933447/autoelectv2"
	"github.com/995933447/autoelectv2/factory"
	nodegrpha "github.com/995933447/bucketmq/internal/ha"
	"github.com/995933447/bucketmq/internal/ha/synclog"
	"github.com/995933447/bucketmq/internal/syscfg"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/discover"
	"github.com/995933447/bucketmq/pkg/rpc/ha"
	"github.com/995933447/gonetutil"
	"github.com/995933447/microgosuit/discovery"
	"github.com/995933447/microgosuit/grpcsuit"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"time"
)

func NewHA(etcdCli *clientv3.Client, disc discovery.Discovery, hALogSync *synclog.Sync, topicMgr *TopicMgr, haService ha.HAServer) *HA {
	hA := &HA{
		etcdCli:   etcdCli,
		Discovery: disc,
		hALogSync: hALogSync,
		topicMgr:  topicMgr,
		hAService: haService,
	}

	hA.init()

	return hA
}

type HA struct {
	discovery.Discovery
	hALogSync *synclog.Sync
	topicMgr  *TopicMgr
	etcdCli   *clientv3.Client
	elect     autoelectv2.AutoElection
	hAGRPC    ha.HAClient
	hAService ha.HAServer
}

func (h *HA) init() {
	go func() {
		for {
			if nodegrpha.IsMasterRole() {
				continue
			}

			if uint32(time.Now().Unix())-nodegrpha.LastSyncedLogAt <= 30 {
				continue
			}

			meta, err := nodegrpha.GetNodeGrpHAMeta(h.etcdCli)
			if err != nil {
				util.Logger.Error(nil, err)
				continue
			}

			if meta.MaxSyncLogId <= nodegrpha.MaxSyncedLogId && meta.TermOfMaxSyncLog <= nodegrpha.TermOfMaxSyncedLog {
				continue
			}

			unlock, err := util.DistributeLockNodeGrpForUpdateDiscovery(h.etcdCli)
			if err != nil {
				util.Logger.Error(nil, err)
				continue
			}

			brokerCfg, err := h.Discovery.Discover(context.Background(), discover.SrvNameBroker)
			if err != nil {
				unlock()
				util.Logger.Error(nil, err)
				continue
			}

			var masterNode *discovery.Node
			for _, node := range brokerCfg.Nodes {
				var nodeDesc ha.Node
				err = json.Unmarshal([]byte(node.Extra), &nodeDesc)
				if err != nil {
					unlock()
					util.Logger.Error(nil, err)
					continue
				}

				if nodeDesc.IsMaster {
					break
				}
			}

			unlock()

			if masterNode == nil {
				continue
			}

			conn, err := grpc.Dial(discover.GetGrpcResolveSchema(syscfg.MustCfg().Cluster)+":///"+discover.SrvNameBroker, grpcsuit.NotRoundRobinDialOpts...)
			if err != nil {
				util.Logger.Error(nil, err)
				continue
			}

			resp, err := ha.NewHAClient(conn).PullRemoteReplica(context.Background(), &ha.PullRemoteReplicaReq{
				LastSyncedLogId:     nodegrpha.MaxSyncedLogId,
				TermOfLastSyncedLog: nodegrpha.TermOfMaxSyncedLog,
			})
			if err != nil {
				util.Logger.Error(nil, err)
				continue
			}

			_, err = h.hAService.SyncRemoteReplica(context.Background(), &ha.SyncRemoteReplicaReq{
				LogItems:            resp.LogItems,
				LastSyncedLogId:     nodegrpha.MaxSyncedLogId,
				TermOfLastSyncedLog: nodegrpha.TermOfMaxSyncedLog,
			})
			if err != nil {
				util.Logger.Error(nil, err)
				continue
			}
		}
	}()
}

func (h *HA) Elect() (err error) {
	electKey := nodegrpha.GetNodeGrpElectEtcdKey()

	h.elect, err = factory.NewAutoElection(factory.ElectDriverDistribMuEtcdv3, factory.NewDistribMuEtcdv3Cfg(electKey, h.etcdCli, 5))
	if err != nil {
		return err
	}

	h.elect.OnBeMaster(func() bool {
		meta, err := nodegrpha.GetNodeGrpHAMeta(h.etcdCli)
		if err != nil {
			util.Logger.Error(nil, err)
			return false
		}

		if nodegrpha.MaxSyncedLogId < meta.MaxSyncLogId || nodegrpha.TermOfMaxSyncedLog < meta.TermOfMaxSyncLog {
			return false
		}

		resp, err := h.etcdCli.Get(context.Background(), electKey, clientv3.WithFirstCreate()...)
		if err != nil {
			util.Logger.Error(nil, err)
			return false
		}

		if len(resp.Kvs) == 0 {
			util.Logger.Error(nil, "not master")
			return false
		}

		nodegrpha.NodeGrpMasterElectEtcdKey = string(resp.Kvs[0].Key)
		nodegrpha.NodeGrpMasterElectEtcdKeyCreateVersion = resp.Kvs[0].CreateRevision

		meta.ElectTerm++
		succ, err := nodegrpha.SaveNodeGrpHAMetaByMasterRole(h.etcdCli, meta)
		if err != nil {
			util.Logger.Error(nil, err)
			return false
		}

		if !succ {
			return false
		}

		nodegrpha.SwitchMasterRole(meta.ElectTerm)

		h.topicMgr.Start()

		unlock, err := util.DistributeLockNodeGrpForUpdateDiscovery(h.etcdCli)
		if err != nil {
			return false
		}

		defer unlock()

		if ok, err := nodegrpha.RefreshIsMasterRole(h.etcdCli); err != nil {
			util.Logger.Error(nil, err)
			return false
		} else if !ok {
			return false
		}

		brokerCfg, err := h.Discovery.Discover(context.Background(), discover.SrvNameBroker)
		if err != nil {
			util.Logger.Error(nil, err)
			return false
		}

		sysCfg := syscfg.MustCfg()
		curHost, err := gonetutil.EvalVarToParseIp(sysCfg.Host)
		if err != nil {
			util.Logger.Error(nil, err)
			return false
		}

		for _, node := range brokerCfg.Nodes {
			var nodeDesc ha.Node
			err = json.Unmarshal([]byte(node.Extra), &nodeDesc)
			if err != nil {
				util.Logger.Error(nil, err)
				return false
			}

			nodeDesc.IsMaster = false

			if node.Host == curHost && node.Port == sysCfg.Port2 {
				nodeDesc.IsMaster = true
			}

			nodeDescJ, err := json.Marshal(nodeDesc)
			if err != nil {
				util.Logger.Error(nil, err)
				return false
			}

			node.Extra = string(nodeDescJ)

			err = h.Discovery.Register(context.Background(), discover.SrvNameBroker, node)
			if err != nil {
				util.Logger.Error(nil, err)
				return false
			}
		}

		for {
			ok, err := nodegrpha.RefreshIsMasterRole(h.etcdCli)
			if err != nil {
				util.Logger.Error(nil, err)
				return false
			}

			if !ok {
				break
			}

			if h.hALogSync.Consumer.IsExiting() {
				time.Sleep(time.Second)
				continue
			}

			go func() {
				if err = h.hALogSync.Consumer.Start(); err != nil {
					util.Logger.Error(nil, err)
				}
			}()

			break
		}

		return true
	})

	h.elect.OnLostMaster(func() {
		nodegrpha.RevokeMasterRole()
		h.hALogSync.Writer.Stop()
		h.topicMgr.Exit()
		time.Sleep(time.Millisecond * 5)
		h.hALogSync.Writer.Resume()
		h.hALogSync.Consumer.Exit()
	})

	electErrCh := make(chan error)
	go func() {
		for {
			err := <-electErrCh
			util.Logger.Error(nil, err)
		}
	}()
	h.elect.LoopInElect(context.Background(), electErrCh)

	return nil
}
