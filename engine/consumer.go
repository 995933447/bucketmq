package engine

func newConsumer(subscriber *Subscriber) *consumer {
	return &consumer{
		Subscriber: subscriber,
		exitCh:     make(chan struct{}),
	}
}

type consumer struct {
	*Subscriber
	exitCh chan struct{}
}

func (c *consumer) run() {
	consumeCh := make(chan *FileMsg)
	for {
		select {
		case c.readyConsumerCh <- consumeCh:
		case <-c.exitCh:
			goto out
		}

		msg := <-consumeCh

		// TODO

		c.confirmMsgCh <- &confirmMsgReq{
			seq:       msg.seq,
			idxOffset: msg.offset,
			bucketId:  msg.bucketId,
		}
	}
out:
	return
}

func (c *consumer) exit() {
	c.exitCh <- struct{}{}
}
