package file

import (
	"encoding/binary"
	"github.com/995933447/bucketmq/core/msgstorages"
)

const (
	bufBeginBoundary = 0x12
	bufEndBoundary = 0x34
)

const (
	// 0x12 + 0x34 = 2 bytes
	bufBoundarySize = 2
	// indexes buf len: Bucket(4 bytes) + CreatedAt(4 bytes) + Priority(1 byte)
	//	+ DelaySeconds(4 bytes) + ExpireAt(4 bytes)
	//	+ DataLen(4 bytes) + MsgIdLen(16 bytes) + MsgOffset(8 bytes) + bufBoundarySize(2 bytes) = 47 bytes
	indexBufSize = 47
	offsetBufSize = 4
)

type msgEncoder struct {
	indexesBuf []byte
	dataBuf []byte
}

func (e *msgEncoder) getMsgsDataBufBytes(msgs []*msgstorages.Message) uint32 {
	var totalBytes uint32
	for _, msg := range msgs {
		totalBytes += uint32(len(msg.GetDataPayload().GetData())) + bufBoundarySize
	}
	return totalBytes
}

func (e *msgEncoder) encodeBuf(msgs []*msgstorages.Message) (indexesBuf []byte, dataBuf []byte) {
	indexesBufBytes := len(msgs) * indexBufSize
	dataBufSizeBytes := e.getMsgsDataBufBytes(msgs)
	if indexesBufBytes > len(e.indexesBuf) {
		e.indexesBuf = make([]byte, indexesBufBytes)
	}
	if dataBufSizeBytes > uint32(len(e.dataBuf)) {
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
		writableIndexesBuf[13] = metadata.GetPriority()
		endian.PutUint32(writableIndexesBuf[14:18], metadata.GetDelaySeconds())
		endian.PutUint64(writableIndexesBuf[18:26], metadata.GetMsgOffset())
		endian.PutUint32(writableIndexesBuf[26:30], uint32(dataLen))
		copy(writableIndexesBuf[30:46], metadata.GetMsgId())
		writableIndexesBuf[46] = bufEndBoundary

		writableDataBuf := e.dataBuf[writtenDataBufLen:]
		writableDataBuf[0] = bufBeginBoundary
		copy(writableDataBuf[1:], dataPayload.GetData())
		writableDataBuf[dataLen + 1] = bufEndBoundary
		writtenDataBufLen += e.getMsgsDataBufBytes([]*msgstorages.Message{msg})
	}

	indexesBuf = e.indexesBuf[:indexesBufBytes]
	dataBuf = e.dataBuf[:dataBufSizeBytes]

	return
}

func (e *msgEncoder) decodeOffsets(buf []byte) ([]uint32, error) {
	offsetNum := len(buf) / offsetBufSize
	offsets := make([]uint32, 0, offsetNum)
	endian := binary.LittleEndian
	for i := 0; i < len(buf); i += 4 {
		offsets = append(offsets, endian.Uint32(buf[i:i + offsetBufSize]))
	}
	return offsets, nil
}