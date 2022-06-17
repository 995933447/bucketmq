package msgstorages

import (
	"testing"
	"time"
)

func TestCalcDefaultRetryAt(t *testing.T) {
	msg := NewMsg(&NewMsgReq{})
	msg.GetMetadata().AddRetryCnt()
	msg.CalcDefaultRetryAt()
	t.Logf("retry at %d", msg.GetMetadata().GetExpectRetryAt() - uint32(time.Now().Unix()))
}
