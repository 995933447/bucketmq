package file

import (
	"encoding/binary"
	"github.com/995933447/bucketmq/core/msgstorage"
	"sync"
)

const (
	bufBeginBoundary = 0x12
	bufEndBoundary = 0x34
)

const (
	// 0x12 + 0x34 = 2 bytes
	bufBoundarySize = 2
	// indexes buf len: Bucket(4 bytes) + CreatedAt(4 bytes) + Priority(2 byte)
	//	+ DelaySeconds(4 bytes) + MaxExecTimeLong(4 bytes) + MaxRetryCnt(4 bytes) + ExpireAt(4 bytes)
	//	+ DataLen(4 bytes) + MsgIdLen(4 bytes) + bufBoundarySize(2 bytes) = 34 bytes
	indexBufSize = 68
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
		totalBytes =+ uint32(len(msg.GetDataPayload().GetData())) + bufBoundarySize
	}
	return totalBytes
}

func (e *MsgEncoder) encodeBuf(msgs []*msgstorage.Message) (indexesBuf []byte, dataBuf []byte) {
	indexesBufBytes := len(msgs) * indexBufSize
	dataBufSizeBytes := e.getMsgsDataBufBytes(msgs)
	if indexesBufBytes > cap(e.indexesBuf) {
		e.indexesBuf = make([]byte, indexesBufBytes)
	}
	if dataBufSizeBytes > uint32(cap(e.dataBuf)) {
		e.dataBuf = make([]byte, dataBufSizeBytes)
	}

	var (
		endian = binary.LittleEndian
		writtenDataBufLen uint32
	)
	for i, msg := range msgs {
		var (
			metadata = msg.GetMetadata()
			dataPayload = msg.GetDataPayload()
			dataLen = len(dataPayload.GetData())
		)

		writableIndexesBuf := e.indexesBuf[i * indexBufSize:]
		writableIndexesBuf[0] = bufBeginBoundary
		endian.PutUint32(writableIndexesBuf[1:5], metadata.GetCreatedAt())
		endian.PutUint32(writableIndexesBuf[5:9], metadata.GetBucket())
		endian.PutUint32(writableIndexesBuf[9:13], metadata.GetExpireAt())
		endian.PutUint16(writableIndexesBuf[13:15], metadata.GetPriority())
		endian.PutUint32(writableIndexesBuf[15:19], metadata.GetDelaySeconds())
		endian.PutUint32(writableIndexesBuf[19:23], metadata.GetMaxExecTimeLong())
		endian.PutUint32(writableIndexesBuf[23:27], metadata.GetMaxRetryCnt())
		endian.PutUint32(writableIndexesBuf[27:31], uint32(dataLen))
		copy(writableIndexesBuf[31:67], metadata.GetMsgId())
		writableIndexesBuf[67] = bufEndBoundary

		writableDataBuf := e.dataBuf[writtenDataBufLen:]
		writableDataBuf[0] = bufBeginBoundary
		copy(writableDataBuf[1:], dataPayload.GetData())
		writableDataBuf[dataLen + 1] = bufEndBoundary
		writtenDataBufLen += e.getMsgsDataBufBytes([]*msgstorage.Message{msg})
	}

	indexesBuf = e.indexesBuf[:indexesBufBytes]
	dataBuf = e.dataBuf[:dataBufSizeBytes]

	return
}