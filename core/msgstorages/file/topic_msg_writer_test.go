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
		"test_topic",
		"/data/bucketmqtest13",
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

	var (
		writtenResultChs []chan *WrittenMsgEvent
		msgs             []*msgstorages.Message
	)
	for i := 0; i < 5; i++ {
		writtenMsgEventCh := make(chan *WrittenMsgEvent, 1)
		msg := msgstorages.NewMsg(&msgstorages.NewMsgReq{
			Data:      []byte(fmt.Sprintf("Hello world, %d", i)),
			Bucket:    1,
			CreatedAt: uint32(time.Now().Unix()),
		})
		w.writeMsgReqChan <- &WriteMsgReq{
			msg:              msg,
			writtenEventChan: writtenMsgEventCh,
		}
		writtenResultChs = append(writtenResultChs, writtenMsgEventCh)
		msgs = append(msgs, msg)
	}

	for n, ch := range writtenResultChs {
		t.Logf("loop:%d", n+1)
		res := <-ch
		t.Logf("%+v", res)
	}

	for _, msg := range msgs {
		t.Logf("msg.MsgOffset:%d", msg.GetMetadata().GetMsgOffset())
	}
}
