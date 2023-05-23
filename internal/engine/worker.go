package engine

import (
	"github.com/995933447/bucketmq/pkg/rpc/errs"
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

		if err := w.consumeFunc(w.topic, w.Subscriber.name, w.consumer, time.Millisecond*time.Duration(w.consumeMaxMs), msg); err != nil {
			if rpcErr, ok := errs.ToRPCErr(err); ok && rpcErr.Code == errs.ErrCode_ErrCodeConsumerNotFound {
				msg.retryAt = uint32(time.Now().Unix()) + 5
				continue
			}

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

func (w *worker) exit() {
	w.exitCh <- struct{}{}
}
