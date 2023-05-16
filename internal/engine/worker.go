package engine

import (
	"context"
	"fmt"
	"github.com/995933447/bucketmq/pkg/rpc"
	"github.com/995933447/microgosuit/discovery"
	"github.com/995933447/microgosuit/discovery/util"
	"github.com/995933447/microgosuit/grpcsuit"
	"google.golang.org/grpc"
	"time"
)

func newWorker(subscriber *Subscriber) *worker {
	return &worker{
		Subscriber: subscriber,
		exitCh:     make(chan struct{}),
	}
}

type worker struct {
	*Subscriber
	exitCh       chan struct{}
	consumeMaxMs uint32
}

func (w *worker) run() {
	consumeCh := make(chan *FileMsg)
	for {
		select {
		case w.readyWorkerCh <- consumeCh:
		case <-w.exitCh:
			goto out
		}

		msg := <-consumeCh

		if err := w.consume(msg); err != nil {
			if msg.retriedCnt < msg.maxRetryCnt {
				msg.retriedCnt++
				switch msg.retriedCnt {
				case 1, 2:
					msg.retryAt = uint32(time.Now().Unix()) + msg.retriedCnt
				case 3:
					msg.retryAt = uint32(time.Now().Unix()) + 5
				case 4:
					msg.retryAt = uint32(time.Now().Unix()) + 10
				default:
					msg.retryAt = uint32(time.Now().Unix()) + 15
				}
				w.queue.push(msg, false)
				continue
			}
		}

		w.confirmMsgCh <- &confirmMsgReq{
			seq:       msg.seq,
			idxOffset: msg.offset,
			bucketId:  msg.bucketId,
		}
	}
out:
	return
}

func (w *worker) consume(msg *FileMsg) error {
	consumerNode, err := util.Route(context.Background(), w.Subscriber.Discovery, w.Subscriber.consumer)
	if err != nil {
		if err == discovery.ErrSrvNotFound {
			return nil
		}
		return err
	}

	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", consumerNode.Host, consumerNode.Port), grpcsuit.NotRoundRobinDialOpts...)
	if err != nil {
		return err
	}

	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(w.Subscriber.maxConsumeMs)*time.Millisecond)
	defer cancel()
	_, err = rpc.NewConsumerClient(conn).Consume(ctx, &rpc.ConsumeReq{
		Topic:      w.topic,
		Subscriber: w.Subscriber.name,
		Data:       msg.data,
		RetriedCnt: msg.retriedCnt,
	})
	if err != nil {
		return err
	}

	return nil
}

func (w *worker) exit() {
	w.exitCh <- struct{}{}
}
