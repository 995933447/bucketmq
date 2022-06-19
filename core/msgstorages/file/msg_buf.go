package file

import (
	"encoding/binary"
	errdef "github.com/995933447/bucketmqerrdef"
)

const (
	bufBeginBoundary byte = 0x12
	bufEndBoundary byte = 0x34
)

const (
	// 0x12 + 0x34 = 2 bytes
	bufBoundarySize = 2
	// Bucket(4 bytes) + CreatedAt(4 bytes) + Priority(1 byte)
	// + DelaySeconds(4 bytes) + ExpireAt(4 bytes) + DataOffset(4 bytes)
	// + DataLen(4 bytes) + MsgIdLen(16 bytes) + MsgOffset(8 bytes) + bufBoundarySize(2 bytes) = 51 bytes
	indexBufSize  = 51
	// MsgOffset(8 bytes) + DoneAt(4 bytes) + bufBoundarySize(2 bytes)
	doneMetadataBufSize = 14
	// MsgOffset(8 bytes)
	attemptCntMetadataBufSize = 13
)

type msgBufEncoder struct {
	indexesBuf []byte
	dataBuf    []byte
	doneMetadataBuf []byte
}

func (e *msgBufEncoder) calcMsgDataBufBytes(msgItems []*fileMsgWrapper) uint32 {
	var totalBytes uint32
	for _, msgItem := range msgItems {
		totalBytes += uint32(len(msgItem.msg.GetDataPayload().GetData())) + bufBoundarySize
	}
	return totalBytes
}

func (e *msgBufEncoder) encodeMsges(msgItems []*fileMsgWrapper) (indexesBuf []byte, dataBuf []byte) {
	indexesBufBytes := len(msgItems) * indexBufSize
	dataBufSizeBytes := e.calcMsgDataBufBytes(msgItems)
	if indexesBufBytes > len(e.indexesBuf) {
		e.indexesBuf = make([]byte, indexesBufBytes)
	}
	if dataBufSizeBytes > uint32(len(e.dataBuf)) {
		e.dataBuf = make([]byte, dataBufSizeBytes)
	}

	var (
		endian            = binary.LittleEndian
		writtenDataBufLen uint32
	)
	for i, msgItem := range msgItems {
		var (
			msg 		= msgItem.msg
			metadata    = msg.GetMetadata()
			dataPayload = msg.GetDataPayload()
			dataLen     = len(dataPayload.GetData())
		)

		writableIndexesBuf := e.indexesBuf[i*indexBufSize:]
		writableIndexesBuf[0] = bufBeginBoundary
		endian.PutUint32(writableIndexesBuf[1:5], metadata.GetCreatedAt())
		endian.PutUint32(writableIndexesBuf[5:9], metadata.GetBucket())
		endian.PutUint32(writableIndexesBuf[9:13], metadata.GetExpireAt())
		writableIndexesBuf[13] = metadata.GetPriority()
		endian.PutUint32(writableIndexesBuf[14:18], metadata.GetDelaySeconds())
		endian.PutUint64(writableIndexesBuf[18:26], metadata.GetMsgOffset())
		endian.PutUint32(writableIndexesBuf[26:30], msgItem.dataOffset)
		endian.PutUint32(writableIndexesBuf[30:34], uint32(dataLen))
		copy(writableIndexesBuf[34:50], metadata.GetMsgId())
		writableIndexesBuf[50] = bufEndBoundary

		writableDataBuf := e.dataBuf[writtenDataBufLen:]
		writableDataBuf[0] = bufBeginBoundary
		copy(writableDataBuf[1:], dataPayload.GetData())
		writableDataBuf[dataLen + 1] = bufEndBoundary
		writtenDataBufLen += e.calcMsgDataBufBytes([]*fileMsgWrapper{msgItem})
	}

	indexesBuf = e.indexesBuf[:indexesBufBytes]
	dataBuf = e.dataBuf[:dataBufSizeBytes]

	return
}

func (e *msgBufEncoder) decodeIndexes(buf []byte) ([]*fileMsgWrapper, error) {
	//var (
	//	msgItems []*fileMsgWrapper
	//	completeBufLen = len(buf)
	//)
	//for i := 0; i < completeBufLen; i += indexBufSize {
	//	var(
	//		msgItem = &fileMsgWrapper{}
	//		buf = buf[i:]
	//	)
	//}
	return nil, nil
}

func (e *msgBufEncoder) decodeData(buf []byte) ([]byte, error) {
	return nil, nil
}

func (e *msgBufEncoder) decodeDoneMetadata(buf []byte) ([]*doneFileMsgMetadataWrapper, error) {
	metadataNum := len(buf) / doneMetadataBufSize
	doneMetadataList := make([]*doneFileMsgMetadataWrapper, 0, metadataNum)
	endian := binary.LittleEndian
	completeBufLen := len(buf)
	for i := 0; i < completeBufLen; i += doneMetadataBufSize {
		buf = buf[i:]
		beginBoundary := buf[0]
		if beginBoundary != bufBeginBoundary {
			return nil, errdef.BufCorruptionErr
		}
		endBoundary := buf[13]
		if endBoundary != bufEndBoundary {
			return nil, errdef.BufCorruptionErr
		}

		doneMetadata := &doneFileMsgMetadataWrapper{}
		doneMetadata.msgOffset = endian.Uint64(buf[1:9])
		doneMetadata.doneAt = endian.Uint32(buf[9:13])
		doneMetadataList = append(doneMetadataList, doneMetadata)
	}
	return doneMetadataList, nil
}

func (e *msgBufEncoder) decodeAttemptMetadata(buf []byte) ([]*attemptFileMsgMetadataWrapper, error) {
	return nil, nil
}
