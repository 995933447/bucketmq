package file

import (
	"github.com/995933447/bucketmq/core/msgstorage"
	"sync"
)

const (
	bufBeginBoundary = 0x12
	bufEndBoundary = 0x34
)

const (
	bufBoundarySize = 2
	indexBufSize = 25
)

var (
	newDefaultMsgEncoderLocker sync.Mutex
	defaultMsgEncoder *MsgEncoder
)

type MsgEncoder struct {
	indexesBuf []byte
	dataBuf []byte
}

func getDefaultMsgEncoder() *MsgEncoder {
	if defaultMsgEncoder == nil {
		newDefaultMsgEncoderLocker.Lock()
		if defaultMsgEncoder == nil {
			defaultMsgEncoder = &MsgEncoder{}
		}
		newDefaultMsgEncoderLocker.Unlock()
	}

	return defaultMsgEncoder
}

func (e *MsgEncoder) getMsgsDataBufBytes(msgs []*msgstorage.Message) uint32 {
	var totalBytes uint32
	for _, msg := range msgs {
		msgDataPayload := msg.GetDataPayload()
		dataBufBytes :=  uint32(len(msgDataPayload.GetData()) + len(msgDataPayload.GetMsgId())) + bufBoundarySize
		totalBytes =+ dataBufBytes
	}
	return totalBytes
}

func (e *MsgEncoder) encodeBuf(msgs []*msgstorage.Message) (indexesBuf []byte, dataBuf []byte, err error) {
	// indexes buf len: Bucket(4 bytes) + CreatedAt(4 bytes) + Priority(1 byte)
	//	+ DelaySeconds(4 bytes) + MaxExecTimeLong(4 bytes) + MaxRetryCnt(4 bytes) + ExpireAt(4 bytes) = 25 bytes
	indexesBufBytes := len(msgs) * indexBufSize
	dataBufSizeBytes := e.getMsgsDataBufBytes(msgs)
	if indexesBufBytes > cap(e.indexesBuf) {
		e.indexesBuf = make([]byte, indexesBufBytes)
	}
	if dataBufSizeBytes > uint32(cap(e.dataBuf)) {
		e.dataBuf = make([]byte, dataBufSizeBytes)
	}

	//littleEndian := binary.LittleEndian
	//for i, msg := range msgs {
		//writableIndexesBuf := e.indexesBuf[i * indexBufSize:]
		//writableIndexesBuf[0] = bufBeginBoundary
		//writableIndexesBuf[3] = byte(msg.GetMetadata().GetCreatedAt())
		//littleEndian.PutUint16(writableIndexesBuf, bufBeginBoundary)
		//littleEndian.PutUint32(writableIndexesBuf, msg.GetMetadata().GetCreatedAt())
		//littleEndian.PutUint32(writableIndexesBuf, msg.GetMetadata().GetBucket())
		//littleEndian.PutUint32(writableIndexesBuf, msg.GetMetadata().GetExpireAt())
		//littleEndian.PutUint16(writableIndexesBuf, msg.GetMetadata().GetPriority())
	//}

	indexesBuf = e.indexesBuf[:indexesBufBytes]
	dataBuf = e.dataBuf[:dataBufSizeBytes]

	return
}