package file

import "github.com/995933447/bucketmq/core/msgstorage"

const (
	DataPayloadBoundarySize uint32 = 2
	IndexBoundarySize uint32 = 2
)

type MsgEncode struct {
	msgs []*msgstorage.Message
}

func newMsgEncoder(msgs []*msgstorage.Message) *MsgEncode {
	return &MsgEncode{
		msgs: msgs,
	}
}

func (*MsgEncode) encodeBuf() (indexesBuf []byte, dataBuf []byte, err error) {
	return indexesBuf, dataBuf, err
}