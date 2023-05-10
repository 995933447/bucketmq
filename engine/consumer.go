package engine

type consumer struct {
	*Subscriber
}

func (c *consumer) run() {
	consumeCh := make(chan *FileMsg)
	go func() {
		c.readyConsumerCh <- consumeCh
	}()
}
