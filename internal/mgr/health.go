package mgr

import (
	"context"
	"fmt"
	"github.com/995933447/autoelectv2/factory"
	"github.com/995933447/bucketmq/internal/syscfg"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/microgosuit/discovery"
	"github.com/995933447/microgosuit/grpcsuit"
	"github.com/995933447/microgosuit/grpcsuit/handler/health"
	"github.com/995933447/microgosuit/log"
	"google.golang.org/grpc"
	"sync/atomic"
	"time"
)

const checkWorkerPoolSize = 100

type Node struct {
	srvName string
	detail  *discovery.Node
}

func NewHealthChecker(disc discovery.Discovery, checkWorkerPoolSize, checkIntervalMs uint32) *HealthChecker {
	return &HealthChecker{
		Discovery:           disc,
		checkWorkerPoolSize: checkWorkerPoolSize,
		checkIntervalMs:     checkIntervalMs,
	}
}

type HealthChecker struct {
	discovery.Discovery
	checkWorkerPoolSize uint32
	checkIntervalMs     uint32
	isPaused            atomic.Bool
	isExited            atomic.Bool
}

func (h *HealthChecker) ResetCheckWorkerPoolSize(size uint32) {
	h.checkWorkerPoolSize = size
}

func (h *HealthChecker) ResetCheckIntervalMs(ms uint32) {
	h.checkIntervalMs = ms
}

func (h *HealthChecker) Exit() {
	h.isExited.Store(true)
}

func (h *HealthChecker) Pause() {
	h.isPaused.Store(true)
}

func (h *HealthChecker) Resume() {
	h.isPaused.Store(false)
}

func (h *HealthChecker) Run() error {
	sysCfg := syscfg.MustCfg()

	etcdCli, err := util.GetOrNewEtcdCli()
	if err != nil {
		return err
	}

	elect, err := factory.NewAutoElection(factory.ElectDriverDistribMuEtcdv3, factory.NewDistribMuEtcdv3Cfg(sysCfg.Cluster, etcdCli, 10))
	if err != nil {
		return err
	}

	electErrCh := make(chan error)
	go func() {
		for {
			log.Logger.Error(nil, <-electErrCh)
		}
	}()

	go elect.LoopInElect(context.Background(), electErrCh)

	nodeCh := make(chan *Node)
	exitCh := make(chan struct{})
	go func() {
		for {
			var oldWorkerPoolSize uint32
			for {
				if h.isExited.Load() {
					return
				}

				workerPoolSize := h.checkWorkerPoolSize
				if workerPoolSize == 0 {
					workerPoolSize = checkWorkerPoolSize
				}

				if workerPoolSize == oldWorkerPoolSize {
					continue
				}

				expandWorkerNum := int32(workerPoolSize) - int32(oldWorkerPoolSize)
				if expandWorkerNum > 0 {
					for i := int32(0); i < expandWorkerNum; i++ {
						h.work(nodeCh, exitCh)
					}
				}

				if expandWorkerNum < 0 {
					for i := expandWorkerNum; i < 0; i++ {
						exitCh <- struct{}{}
					}
				}

				oldWorkerPoolSize = workerPoolSize

				time.Sleep(time.Millisecond * time.Duration(h.checkIntervalMs))
			}
		}
	}()

	for {
		if !elect.IsMaster() {
			time.Sleep(time.Millisecond * time.Duration(h.checkIntervalMs))
			continue
		}

		if h.isExited.Load() {
			return nil
		}

		if !h.isPaused.Load() {
			sleepMs := 10000
			if h.checkIntervalMs < 1000 {
				sleepMs = int(h.checkIntervalMs)
			}
			time.Sleep(time.Millisecond * time.Duration(sleepMs))
			continue
		}

		services, err := h.Discovery.LoadAll(context.Background())
		if err != nil {
			time.Sleep(time.Second * 3)
			continue
		}

		for _, srv := range services {
			for _, node := range srv.Nodes {
				nodeCh <- &Node{
					srvName: srv.SrvName,
					detail:  node,
				}
			}
		}

		time.Sleep(time.Millisecond * time.Duration(h.checkIntervalMs))
	}
}

func (h *HealthChecker) work(nodeCh chan *Node, exitCh chan struct{}) {
	go func() {
		for {
			select {
			case <-exitCh:
				goto out
			case node := <-nodeCh:
				if err := h.check(node); err != nil {
					log.Logger.Error(nil, err)
				}
			}
		}
	out:
		return
	}()
}

func (h *HealthChecker) check(node *Node) error {
	doCheck := func() error {
		conn, err := grpc.Dial(fmt.Sprintf("%s:%d", node.detail.Host, node.detail.Port), grpcsuit.NotRoundRobinDialOpts...)
		if err != nil {
			return err
		}

		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		_, err = health.NewHealthReporterClient(conn).Ping(ctx, &health.PingReq{})
		if err != nil {
			return err
		}

		return nil
	}

	var alive bool
	for retry := 0; retry < 3; retry++ {
		if err := doCheck(); err != nil {
			time.Sleep(5 * time.Second)
			continue
		}

		alive = true
		break
	}

	if alive {
		return nil
	}

	err := h.Discovery.Unregister(context.Background(), node.srvName, node.detail, false)
	if err != nil {
		return err
	}

	return nil
}
