package engine

import (
	"github.com/995933447/bucketmq/util"
	"time"
)

const (
	maxOpenFileName = 1000
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

func NewSubscriber(baseDir, topic, consumerName string, globalMaxConcurWorkerNum, bucketMaxConcurWorkerNum uint32, weight MsgWeight) (*Subscriber, error) {
	subscriber := &Subscriber{
		baseDir:                  baseDir,
		topic:                    topic,
		consumerName:             consumerName,
		globalMaxConcurWorkerNum: globalMaxConcurWorkerNum,
		bucketMaxConcurWorkerNum: bucketMaxConcurWorkerNum,
		queue:                    newQueue(weight),
		exitCh:                   make(chan struct{}),
	}

	var err error
	subscriber.readerGrp, err = newReaderGroup(subscriber, maxOpenFileName)
	if err != nil {
		return nil, err
	}

	return subscriber, nil
}

type Subscriber struct {
	baseDir                  string
	topic                    string
	consumerName             string
	globalMaxConcurWorkerNum uint32
	bucketMaxConcurWorkerNum uint32
	bucketPendingNumRec      *bucketPendingNumRec
	*queue
	*readerGrp
	exitCh          chan struct{}
	readyConsumerCh chan chan *FileMsg
	watchWrittenCh  chan uint64
	confirmMsgCh    chan *confirmMsgReq
}

func (s *Subscriber) Loop() {
	migrateDelayMsgTk := time.NewTimer(time.Second)
	var waitConsumeChs []chan *FileMsg
	isBucketPendingNotFull := func(bucket *bucket) bool {
		pendingNum, _ := s.bucketPendingNumRec.bucketPendingNumMap[bucket.id]
		return pendingNum < s.bucketMaxConcurWorkerNum
	}
	for {
		select {
		case <-migrateDelayMsgTk.C:
			s.queue.migrateExpired()
		case <-s.exitCh:
			s.readerGrp.close()
		case confirmReq := <-s.confirmMsgCh:
			s.bucketPendingNumRec.subPending(confirmReq.bucketId)
			read, ok := s.readerGrp.seqToReaderMap[confirmReq.seq]
			if !ok {
				break
			}
			read.confirmMsgCh <- &confirmedMsgIdx{
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
			if _, ok := s.readerGrp.seqToReaderMap[seq]; !ok {
				break
			}

			if _, err := s.readerGrp.loadReader(seq); err != nil {
				util.Logger.Error(nil, err)
				break
			}

			if len(waitConsumeChs) == 0 {
				continue
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
