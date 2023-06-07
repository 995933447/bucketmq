package service

import (
	"github.com/995933447/bucketmq/internal/engine"
	"github.com/995933447/bucketmq/internal/server/snrpc"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/rpc/consumer"
	consumerrpc "github.com/995933447/bucketmq/pkg/rpc/consumer"
	"github.com/995933447/bucketmq/pkg/rpc/errs"
	"github.com/995933447/bucketmq/pkg/rpc/health"
	snrpcx "github.com/995933447/bucketmq/pkg/rpc/snrpc"
	"github.com/golang/protobuf/proto"
	"math/rand"
	"net"
	"sync"
	"time"
)

type ConsumeConn struct {
	name string
	conn net.Conn
}

func NewConsumer(srv *snrpc.Server, checkHeartBeatInterval time.Duration) *Consumer {
	handler := &Consumer{
		srv:                    srv,
		connToConsumerMap:      map[net.Conn]string{},
		consumerToConnSetMap:   map[string]map[net.Conn]struct{}{},
		heartBeatCh:            make(chan net.Conn),
		checkHeartBeatInterval: checkHeartBeatInterval,
	}
	go handler.loop()
	return handler
}

type Consumer struct {
	srv                    *snrpc.Server
	opConnsMu              sync.RWMutex
	connToConsumerMap      map[net.Conn]string
	consumerToConnSetMap   map[string]map[net.Conn]struct{}
	connHeartBeatAts       sync.Map // map[net.Conn]uint32
	heartBeatCh            chan net.Conn
	checkHeartBeatInterval time.Duration
}

func (c *Consumer) loop() {
	go c.createHeartBeatWorkers()
	go c.schedCheckHeartBeats()
	go c.delExpiredConns()
}

func (c *Consumer) delExpiredConns() {
	for {
		c.connHeartBeatAts.Range(func(key, value any) bool {
			conn := key.(net.Conn)
			heartBeatAt := value.(uint32)
			if time.Second*time.Duration(uint32(time.Now().Unix())-heartBeatAt) < c.checkHeartBeatInterval*3 {
				return true
			}

			c.opConnsMu.Lock()
			defer c.opConnsMu.Unlock()

			consumer, ok := c.connToConsumerMap[conn]
			if ok {
				if conns, ok := c.consumerToConnSetMap[consumer]; ok {
					delete(conns, conn)
				}
			}
			delete(c.connToConsumerMap, conn)
			c.connHeartBeatAts.Delete(conn)

			return true
		})
		time.Sleep(time.Second)
	}
}

func (c *Consumer) schedCheckHeartBeats() {
	heartBeatTk := time.NewTicker(c.checkHeartBeatInterval)
	defer heartBeatTk.Stop()
	for {
		<-heartBeatTk.C
		for _, conn := range c.srv.GetConns() {
			c.heartBeatCh <- conn
		}
	}
}

func (c *Consumer) createHeartBeatWorkers() {
	for i := 0; i < 200; i++ {
		go func() {
			for {
				conn := <-c.heartBeatCh
				err := c.srv.Callback(conn, time.Second*5, uint32(snrpcx.SNRPCProto_SNRPCProtoHeartBeat), &health.PingReq{}, &health.PingResp{})
				if err != nil {
					util.Logger.Warn(nil, err)
					continue
				}
				c.opConnsMu.Lock()
				c.connHeartBeatAts.Store(conn, uint32(time.Now().Unix()))
				c.opConnsMu.Unlock()
			}
		}()
	}
}

func (c *Consumer) RegConsumer(conn net.Conn, req *consumer.ConnSNSrvReq) error {
	c.opConnsMu.Lock()
	defer c.opConnsMu.Unlock()

	conns, ok := c.consumerToConnSetMap[req.Consumer]
	if !ok {
		conns = map[net.Conn]struct{}{}
		c.consumerToConnSetMap[req.Consumer] = conns
	}
	c.consumerToConnSetMap[req.Consumer][conn] = struct{}{}
	c.connToConsumerMap[conn] = req.Consumer
	c.connHeartBeatAts.Store(conn, uint32(time.Now().Unix()))

	return nil
}

func (c *Consumer) Consume(consumer, topic, subscriber string, timeout time.Duration, msg *engine.FileMsg) (*consumer.ConsumeResp, error) {
	c.opConnsMu.RLock()
	conns, ok := c.consumerToConnSetMap[consumer]
	if !ok {
		c.opConnsMu.RUnlock()
		return nil, errs.RPCErr(errs.ErrCode_ErrCodeConsumerNotFound, "")
	}
	c.opConnsMu.RUnlock()

	randN := rand.Int()
	destIdx := randN % len(conns)
	var (
		curIdx   int
		destConn net.Conn
	)
	for conn := range conns {
		if curIdx == destIdx {
			destConn = conn
			break
		}
		curIdx++
	}

	var resp consumerrpc.ConsumeResp
	err := c.srv.Callback(destConn, timeout, uint32(snrpcx.SNRPCProto_SNRPCProtoConsume), &consumerrpc.ConsumeReq{
		Topic:      topic,
		Subscriber: subscriber,
		Data:       msg.GetData(),
		RetriedCnt: msg.GetRetriedCnt(),
	}, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (c *Consumer) RegSNRPCProto(srv *snrpc.Server) {
	srv.RegProto(uint32(snrpcx.SNRPCProto_SNRPCProtoConsumerConnect), &consumer.ConsumeReq{}, func(conn net.Conn, req proto.Message) (proto.Message, error) {
		specReq := req.(*consumer.ConnSNSrvReq)
		if err := c.RegConsumer(conn, specReq); err != nil {
			return nil, err
		}
		return &consumer.ConsumeResp{}, nil
	})
}
