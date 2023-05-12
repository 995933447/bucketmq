package engine

import (
	"github.com/995933447/bucketmq/util"
	"time"
)

const (
	maxOpenReaderNum = 1000
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

func NewSubscriber(cfg *SubscriberCfg) (*Subscriber, error) {
	subscriber := &Subscriber{
		baseDir:                 cfg.BaseDir,
		topic:                   cfg.Topic,
		consumerName:            cfg.ConsumerName,
		consumerNum:             cfg.ConsumerNum,
		maxConsumerNumPerBucket: cfg.MaxConsumerNumPerBucket,
		queue:                   newQueue(cfg.MsgWeight),
		exitCh:                  make(chan struct{}),
		readyConsumerCh:         make(chan chan *FileMsg),
		watchWrittenCh:          make(chan uint64),
		confirmMsgCh:            make(chan *confirmMsgReq),
	}

	var err error
	subscriber.readerGrp, err = newReaderGrp(subscriber, cfg.LodeMode, cfg.StartMsgId, maxOpenReaderNum, cfg.LoadMsgBootId)
	if err != nil {
		return nil, err
	}

	return subscriber, nil
}

type Subscriber struct {
	baseDir                 string
	topic                   string
	consumerName            string
	consumerNum             uint32
	maxConsumerNumPerBucket uint32
	bucketPendingNumRec     *bucketPendingNumRec
	*queue
	*readerGrp
	exitCh          chan struct{}
	readyConsumerCh chan chan *FileMsg
	watchWrittenCh  chan uint64
	confirmMsgCh    chan *confirmMsgReq
	consumers       []*consumer
}

func (s *Subscriber) Start() {
	for i := uint32(0); i < s.consumerNum; i++ {
		consumer := newConsumer(s)
		s.consumers = append(s.consumers, consumer)
		consumer.run()
	}
	s.loop()
}

func (s *Subscriber) loop() {
	migrateDelayMsgTk := time.NewTimer(time.Second)
	var waitConsumeChs []chan *FileMsg
	isBucketPendingNotFull := func(bucket *bucket) bool {
		pendingNum, _ := s.bucketPendingNumRec.bucketPendingNumMap[bucket.id]
		return pendingNum < s.maxConsumerNumPerBucket
	}
	for {
		select {
		case <-migrateDelayMsgTk.C:
			s.queue.migrateExpired()
		case <-s.exitCh:
			for _, consumer := range s.consumers {
				consumer.exit()
			}
			s.readerGrp.close()
		case confirmReq := <-s.confirmMsgCh:
			s.bucketPendingNumRec.subPending(confirmReq.bucketId)
			reader, ok := s.readerGrp.seqToReaderMap[confirmReq.seq]
			if !ok {
				break
			}
			reader.confirmMsgCh <- &confirmedMsgIdx{
				idxOffset: confirmReq.idxOffset,
			}
		case consumeCh := <-s.readyConsumerCh:
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
}
