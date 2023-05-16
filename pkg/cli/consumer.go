package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/995933447/bucketmq/pkg/rpc"
	"github.com/995933447/microgosuit/discovery"
	"google.golang.org/grpc"
	"net"
	"reflect"
	"sync"
)

type ConsumerProcFunc func(ctx context.Context, req *rpc.ConsumeReq, data interface{}) (*rpc.ConsumeResp, error)

var _ rpc.ConsumerServer = (*Consumer)(nil)

type subscriber struct {
	proc    ConsumerProcFunc
	msgType reflect.Type
}

type Consumer struct {
	cli         *Cli
	name        string
	host        string
	port        int
	opProcMu    sync.RWMutex
	subscribers map[string]map[string]*subscriber
}

func (c *Consumer) Ping(ctx context.Context, req *rpc.PingReq) (*rpc.PingResp, error) {
	return &rpc.PingResp{}, nil
}

func (c *Consumer) Consume(ctx context.Context, req *rpc.ConsumeReq) (*rpc.ConsumeResp, error) {
	var resp rpc.ConsumeResp
	subscribers, ok := c.subscribers[req.Topic]
	if !ok {
		return &resp, nil
	}

	subscriber, ok := subscribers[req.Subscriber]
	if !ok {
		return &resp, nil
	}

	data := reflect.New(subscriber.msgType).Interface()
	err := json.Unmarshal(req.Data, data)
	if err != nil {
		return nil, err
	}

	return subscriber.proc(ctx, req, data)
}

func (c *Consumer) Subscribe(cfg *rpc.Subscriber, msg interface{}, procFunc ConsumerProcFunc) error {
	c.opProcMu.Lock()
	defer c.opProcMu.Unlock()

	_, err := c.cli.RegSubscriber(context.Background(), &rpc.RegSubscriberReq{
		Subscriber: cfg,
	})
	if err != nil {
		return err
	}

	subscribers, ok := c.subscribers[cfg.Topic]
	if !ok {
		subscribers = map[string]*subscriber{}
		c.subscribers[cfg.Topic] = subscribers
	}

	subscribers[cfg.Subscriber] = &subscriber{
		proc:    procFunc,
		msgType: reflect.TypeOf(msg),
	}

	return nil
}

func (c *Consumer) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", c.host, c.port))
	if err != nil {
		return err
	}

	node := discovery.NewNode(c.host, c.port)
	err = c.cli.discovery.Register(context.Background(), c.name, node)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	rpc.RegisterConsumerServer(grpcServer, c)

	defer func() {
		_ = c.cli.discovery.Unregister(context.Background(), c.name, node, true)
	}()

	err = grpcServer.Serve(listener)
	if err != nil {
		return err
	}

	return nil
}
