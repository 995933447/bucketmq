package engine

import (
	"testing"
	"time"
)

func TestMsgTable(t *testing.T) {
	var msgQ msgTable
	msgQ.push(&FileMsg{
		priority:  MaxMsgPriority,
		createdAt: uint32(time.Now().Unix()),
		bucketId:  1,
	})
	msgQ.push(&FileMsg{
		priority:  MaxMsgPriority,
		createdAt: uint32(time.Now().Unix()),
	})
	msgQ.push(&FileMsg{
		priority:  2,
		createdAt: uint32(time.Now().Unix()),
	})
	msgQ.push(&FileMsg{
		priority:  1,
		createdAt: uint32(time.Now().Unix() - 1),
	})
	msgQ.push(&FileMsg{
		priority:  1,
		createdAt: uint32(time.Now().Unix()),
	})
	msgQ.push(&FileMsg{
		priority:  0,
		createdAt: uint32(time.Now().Unix()),
	})
	t.Log(msgQ)

	msg, _ := msgQ.pop(MsgWeightPriority)
	t.Logf("%+v", msg)
	t.Log(msgQ)

	msg, _ = msgQ.pop(MsgWeightCreatedAtWithPriority)
	t.Logf("%+v", msg)

	t.Log(msgQ)
	t.Log(msgQ.len())
}

func TestRetryMsgQueue(t *testing.T) {
	retryQ := newRetryMsgQueue(nil)
	retryQ.pushRetry(&FileMsg{
		retryAt:  uint32(time.Now().Unix() - 1),
		bucketId: 1,
	})
	retryQ.pushRetry(&FileMsg{
		retryAt:  uint32(time.Now().Unix() - 2),
		bucketId: 2,
	})
	retryQ.pushRetry(&FileMsg{
		retryAt:  uint32(time.Now().Unix() + 10),
		bucketId: 3,
	})
	t.Log(retryQ.Peek())
	t.Log(retryQ.popRetry())
	t.Log(retryQ.popRetry())
	t.Log(retryQ.popRetry())
}

func TestBucket(t *testing.T) {
	buck := newBucket(1, newQueue(MsgWeightPriority, true))
	buck.push(&FileMsg{
		priority:  MaxMsgPriority,
		createdAt: uint32(time.Now().Unix()),
		bucketId:  1,
	})
	buck.push(&FileMsg{
		priority:  MaxMsgPriority,
		createdAt: uint32(time.Now().Unix()),
	})
	buck.push(&FileMsg{
		priority:  2,
		createdAt: uint32(time.Now().Unix()),
	})
	buck.push(&FileMsg{
		priority:  1,
		createdAt: uint32(time.Now().Unix() - 1),
	})
	buck.push(&FileMsg{
		priority:  1,
		createdAt: uint32(time.Now().Unix()),
	})
	buck.push(&FileMsg{
		priority:  0,
		createdAt: uint32(time.Now().Unix()),
	})

	buck.push(&FileMsg{
		retryAt:  uint32(time.Now().Unix() - 1),
		bucketId: 1,
	})
	buck.push(&FileMsg{
		retryAt:  uint32(time.Now().Unix() - 2),
		bucketId: 1,
	})
	//buck.push(&FileMsg{
	//	retryAt:  uint32(time.Now().Unix() + 10),
	//	bucketId: 3,
	//})

	t.Log(buck.pop(MsgWeightPriority))
	t.Log(buck.pop(MsgWeightPriority))
	t.Log(buck.pop(MsgWeightPriority))
}

func TestDelayMsgQueue(t *testing.T) {
	delayQ := newDelayMsgQueue()
	delayQ.push(&FileMsg{
		delaySec: 10,
		bucketId: 1,
	})
	delayQ.push(&FileMsg{
		delaySec: 2,
		bucketId: 2,
	})
	delayQ.push(&FileMsg{
		delaySec: 1,
		bucketId: 3,
	})
	delayQ.push(&FileMsg{
		delaySec: 1,
		bucketId: 2,
	})
	time.Sleep(time.Second * 3)
	for _, expired := range delayQ.migrateExpired() {
		t.Logf("%+v", expired)
	}
}

func TestQueue(t *testing.T) {
	q := newQueue(MsgWeightPriority, true)
	q.push(&FileMsg{
		delaySec: 1,
		bucketId: 3,
	}, true)

	q.push(&FileMsg{
		bucketId: 2,
		priority: 2,
	}, true)
	q.push(&FileMsg{
		bucketId:  2,
		priority:  MaxMsgPriority,
		createdAt: uint32(time.Now().Unix()),
	}, true)
	q.push(&FileMsg{
		bucketId:  3,
		priority:  1,
		createdAt: uint32(time.Now().Unix()),
	}, true)
	q.push(&FileMsg{
		bucketId:  3,
		priority:  2,
		createdAt: uint32(time.Now().Unix() - 1),
	}, true)
	q.push(&FileMsg{
		bucketId:  1,
		priority:  1,
		createdAt: uint32(time.Now().Unix()),
	}, true)

	t.Logf("%+v", q.pop(nil))
	t.Logf("%+v", q.pop(nil))
	t.Logf("%+v", q.pop(nil))

	q.push(&FileMsg{
		delaySec: 10,
		bucketId: 1,
	}, true)
	q.push(&FileMsg{
		delaySec: 2,
		bucketId: 2,
	}, true)
	q.push(&FileMsg{
		delaySec: 1,
		bucketId: 3,
	}, true)
	q.push(&FileMsg{
		delaySec: 1,
		bucketId: 2,
	}, true)
	q.migrateExpired()
}
