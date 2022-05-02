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
		"/data/bucketmqtest/index",
		"/data/bucketmqtest/data",
		1,
		2,
		100,
		0,
		log.DefaultLogger,
	)
}

func TestWriteTopicMsg(t *testing.T) {
	w, _ := mockTopicMsgWriter()
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
			MaxExecTimeLong: uint32(10),
			MaxRetryCnt: 5,
		})
	}

	time.Sleep(time.Second * 10)
}