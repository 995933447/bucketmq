package engine

import (
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/microgosuit/discovery"
	"time"
)

const (
	maxOpenReaderNum             = 1000
	concurConsumeNum             = 1000
	maxConcurConsumeNumPerBucket = 1
	maxConsumeMs                 = 300 * 10000
)

type confirmMsgReq struct {
	seq       uint64
	idxOffset uint32
	bucketId  uint32
}

type bucketPendingNumRec struct {
	bucketPendingNumMap map[uint32]uint32
}

func (r *bucketPendingNumRec) addPending(bucketId uint32) {
	pendingNum, _ := r.bucketPendingNumMap[bucketId]
	pendingNum++
	r.bucketPendingNumMap[bucketId] = pendingNum
}

func (r *bucketPendingNumRec) subPending(bucketId uint32) {
	pendingNum, _ := r.bucketPendingNumMap[bucketId]
	if pendingNum == 0 {
		return
	}
	pendingNum--
	r.bucketPendingNumMap[bucketId] = pendingNum
}

func NewSubscriber(discover discovery.Discovery, cfg *SubscriberCfg, consumeFunc ConsumeFunc) (*Subscriber, error) {
	subscriber := &Subscriber{
		baseDir:                      cfg.BaseDir,
		topic:                        cfg.Topic,
		name:                         cfg.Name,
		concurConsumeNum:             cfg.ConcurConsumeNum,
		maxConcurConsumeNumPerBucket: cfg.MaxConcurConsumeNumPerBucket,
		queue:                        newQueue(cfg.MsgWeight, cfg.IsSerial),
		Discovery:                    discover,
		exitCh:                       make(chan struct{}),
		readyWorkerCh:                make(chan chan *FileMsg),
		watchWrittenCh:               cfg.WatchWrittenCh,
		confirmMsgCh:                 make(chan *confirmMsgReq),
		consumer:                     cfg.Consumer,
		maxConsumeMs:                 cfg.MaxConsumeMs,
		consumeFunc:                  consumeFunc,
	}

	var err error
	subscriber.readerGrp, err = newReaderGrp(subscriber, cfg.LodeMode, cfg.StartMsgId, maxOpenReaderNum, cfg.LoadMsgBootId)
	if err != nil {
		return nil, err
	}

	if subscriber.concurConsumeNum == 0 {
		subscriber.concurConsumeNum = concurConsumeNum
		subscriber.maxConcurConsumeNumPerBucket = maxConcurConsumeNumPerBucket
	}

	if subscriber.maxConsumeMs == 0 {
		subscriber.maxConsumeMs = maxConsumeMs
	}

	return subscriber, nil
}

type ConsumeFunc func(topic, subscriber, consumer string, timeout time.Duration, msg *FileMsg) error

type Subscriber struct {
	*queue
	*readerGrp
	discovery.Discovery
	baseDir                      string
	topic                        string
	consumer                     string
	name                         string
	concurConsumeNum             uint32
	maxConcurConsumeNumPerBucket uint32
	bucketPendingNumRec          *bucketPendingNumRec
	exitCh                       chan struct{}
	readyWorkerCh                chan chan *FileMsg
	watchWrittenCh               chan uint64
	confirmMsgCh                 chan *confirmMsgReq
	workers                      []*worker
	maxConsumeMs                 uint32
	consumeFunc                  ConsumeFunc
}

func (s *Subscriber) Start() {
	for i := uint32(0); i < s.concurConsumeNum; i++ {
		worker := newWorker(s)
		s.workers = append(s.workers, worker)
		worker.run()
	}
	s.loop()
}

func (s *Subscriber) Exit() {
	s.exitCh <- struct{}{}
}

func (s *Subscriber) loop() {
	migrateDelayMsgTk := time.NewTimer(time.Second)
	var waitConsumeChs []chan *FileMsg
	isBucketPendingNotFull := func(bucket *bucket) bool {
		pendingNum, _ := s.bucketPendingNumRec.bucketPendingNumMap[bucket.id]
		return pendingNum < s.maxConcurConsumeNumPerBucket
	}
	for {
		select {
		case <-migrateDelayMsgTk.C:
			s.queue.migrateExpired()
		case <-s.exitCh:
			for _, worker := range s.workers {
				worker.exit()
			}
			s.readerGrp.close()
			goto out
		case confirmReq := <-s.confirmMsgCh:
			s.bucketPendingNumRec.subPending(confirmReq.bucketId)
			reader, ok := s.readerGrp.seqToReaderMap[confirmReq.seq]
			if !ok {
				break
			}
			reader.confirmMsgCh <- &confirmedMsgIdx{
				idxOffset: confirmReq.idxOffset,
			}
		case consumeCh := <-s.readyWorkerCh:
			if msg := s.queue.pop(isBucketPendingNotFull); msg != nil {
				s.bucketPendingNumRec.addPending(msg.bucketId)
				consumeCh <- msg
				break
			}
			waitConsumeChs = append(waitConsumeChs, consumeCh)
		case seq := <-s.watchWrittenCh:
			if len(s.readerGrp.seqToReaderMap) > 0 {
				if _, ok := s.readerGrp.seqToReaderMap[seq]; !ok {
					break
				}
			}

			reader, err := s.readerGrp.loadSpec(seq)
			if err != nil {
				util.Logger.Error(nil, err)
				break
			}

			s.readerGrp.seqToReaderMap[seq] = reader

			if len(waitConsumeChs) == 0 {
				break
			}

			msg := s.queue.pop(isBucketPendingNotFull)
			if msg == nil {
				break
			}

			readyConsumeCh := waitConsumeChs[0]
			waitConsumeChs = waitConsumeChs[1:]
			s.bucketPendingNumRec.addPending(msg.bucketId)
			readyConsumeCh <- msg
		}
	}
out:
	return
}
