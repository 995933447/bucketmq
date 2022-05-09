package file

import (
	"context"
	"fmt"
	"github.com/995933447/bucketmq/core/log"
	"github.com/995933447/bucketmq/core/msgstorages"
	"testing"
	"time"
)

func TestInitTopicMsgWriter(t *testing.T) {
	_, err := mockTopicMsgWriter()
	if err != nil {
		t.Error(err)
	}
}

func mockTopicMsgWriter() (*topicMsgWriter, error) {
	return newTopicMsgWriter(
		context.TODO(),
		"test_topic",
		"/data/bucketmqtest",
		2,
		100,
		5,
		log.DefaultLogger,
	)
}

func TestWriteTopicMsg(t *testing.T) {
	w, err := mockTopicMsgWriter()
	if err != nil {
		t.Error(err)
		return
	}
	go func() {
		if err := w.loop(context.Background()); err != nil {
			t.Error(err)
			return
		}
	}()

	for i := 0; i < 10; i++ {
		w.msgCh <- msgstorages.NewMsg(&msgstorages.NewMsgReq{
			Data: []byte(fmt.Sprintf("Hello world, %d", i)),
			Bucket: 1,
			CreatedAt: uint32(time.Now().Unix()),
		})
	}

	time.Sleep(time.Second * 10)
}