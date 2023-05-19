package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/995933447/bucketmq/pkg/rpc/consumer"
	"github.com/995933447/bucketmq/pkg/rpc/health"
	"github.com/995933447/gonetutil"
	"github.com/995933447/microgosuit/discovery"
	"google.golang.org/grpc"
	"net"
	"reflect"
	"sync"
)

type ConsumerProcFunc func(ctx context.Context, req *consumer.ConsumeReq, data interface{}) (*consumer.ConsumeResp, error)

var (
	_ consumer.ConsumerServer     = (*Consumer)(nil)
	_ health.HealthReporterServer = (*Consumer)(nil)
)

type proc struct {
	handler ConsumerProcFunc
	msgType reflect.Type
}

type Consumer struct {
	cli      *Cli
	name     string
	host     string
	port     int
	opProcMu sync.RWMutex
	procs    map[string]map[string]*proc
}

func (c *Consumer) Ping(_ context.Context, _ *health.PingReq) (*health.PingResp, error) {
	return &health.PingResp{}, nil
}

func (c *Consumer) Consume(ctx context.Context, req *consumer.ConsumeReq) (*consumer.ConsumeResp, error) {
	var resp consumer.ConsumeResp
	procs, ok := c.procs[req.Topic]
	if !ok {
		return &resp, nil
	}

	proc, ok := procs[req.Subscriber]
	if !ok {
		return &resp, nil
	}

	data := reflect.New(proc.msgType).Interface()
	err := json.Unmarshal(req.Data, data)
	if err != nil {
		return nil, err
	}

	return proc.handler(ctx, req, data)
}

func (c *Consumer) Proc(topic, subscriber string, msg interface{}, procFunc ConsumerProcFunc) error {
	c.opProcMu.Lock()
	defer c.opProcMu.Unlock()

	procs, ok := c.procs[topic]
	if !ok {
		procs = map[string]*proc{}
		c.procs[topic] = procs
	}

	procs[subscriber] = &proc{
		handler: procFunc,
		msgType: reflect.TypeOf(msg),
	}

	return nil
}

func (c *Consumer) Start() error {
	host, err := gonetutil.EvalVarToParseIp(c.host)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, c.port))
	if err != nil {
		return err
	}

	node := discovery.NewNode(c.host, c.port)
	err = c.cli.discovery.Register(context.Background(), c.name, node)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	consumer.RegisterConsumerServer(grpcServer, c)

	defer func() {
		_ = c.cli.discovery.Unregister(context.Background(), c.name, node, true)
	}()

	err = grpcServer.Serve(listener)
	if err != nil {
		return err
	}

	return nil
}
