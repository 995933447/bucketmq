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
	}
out:
	return
}

func (c *consumer) exit() {
	c.exitCh <- struct{}{}
}
