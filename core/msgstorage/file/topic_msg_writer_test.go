package file

import (
	"github.com/995933447/bucketmq/core/msgstorage"
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
		"test_topic",
		"/data/bucketmq/index",
		"/data/bucketmq/data",
		1,
		100,
		100,
	)
}

func TestWriteTopicMsg(t *testing.T) {
	w, _ := mockTopicMsgWriter()
	go func() {
		if err := w.loop(); err != nil {
			t.Error(err)
			return
		}
	}()

	w.msgCh <- msgstorage.NewMsg(&msgstorage.NewMsgReq{
		Data: []byte("Hello world"),
		Bucket: 1,
		CreatedAt: uint32(time.Now().Unix()),
		MaxExecTimeLong: uint32(10),
		MaxRetryCnt: 5,
	})
	time.Sleep(time.Second * 5)
}