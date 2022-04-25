package file

import (
	"github.com/995933447/bucketmq/core/msgstorage"
	"os"
	"strconv"
	"strings"
)

type indexFileWriter struct {
	fp *os.File
	maxWritableIndexNum uint32
	writtenIndexNum uint32
}

type dataFileWriter struct {
	fp *os.File
	maxWritableDataBytes uint32
	writtenDataBytes uint32
}

type filesWriter struct {
	*indexFileWriter
	*dataFileWriter
}

type NewFilesOpenedSignal struct {
	fileSeq uint32 `access:"r"`
}

func (s *NewFilesOpenedSignal) getFileSeq() uint32 {
	return s.fileSeq
}

type topicMsgWriter struct {
	finishInit bool
	DirOfIndexFiles string
	DirOfDataFiles string
	TopicName string
	FileSeq uint32
	*filesWriter
	msgCh chan *msgstorage.Message
	stopWritingMsgLoopCh chan struct{}
	notifyNewFilesOpenedCh chan *NewFilesOpenedSignal
}

func (s *NewFilesOpenedSignal) getSeq() uint32 {
	return s.fileSeq
}

func (w *topicMsgWriter) getIndexFp() (*os.File, error) {
	fileSeqStr := strconv.FormatUint(uint64(w.FileSeq), 10)
	indexFileName := strings.TrimRight(w.DirOfDataFiles, "/") + w.TopicName + "." + fileSeqStr + ".idx"
	indexFp, err := os.OpenFile(indexFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {

	}
	return indexFp, nil
}

func (w *topicMsgWriter) getDataFp() (*os.File, error) {
	fileSeqStr := strconv.FormatUint(uint64(w.FileSeq), 10)
	dataFileName := strings.TrimRight(w.DirOfDataFiles, "/") + w.TopicName + "." + fileSeqStr + ".dat"
	dataFp, err := os.OpenFile(dataFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {

	}
	return dataFp, nil
}

func (w *topicMsgWriter) init(totalWritableMsgNum, totalWritableMsgBytes uint32) error {
	if w.finishInit {
		return nil
	}
	
	dataFp, err := w.getDataFp()
	if err != nil {
		return err
	}

	indexFp, err := w.getIndexFp()
	if err != nil {
		return err
	}

	w.dataFileWriter = &dataFileWriter{
		maxWritableDataBytes: totalWritableMsgBytes,
		fp: dataFp,
	}
	w.indexFileWriter = &indexFileWriter{
		maxWritableIndexNum: totalWritableMsgNum,
		fp: indexFp,
	}
	w.msgCh = make(chan *msgstorage.Message, 10000)
	w.stopWritingMsgLoopCh = make(chan struct{})
	w.notifyNewFilesOpenedCh = make(chan *NewFilesOpenedSignal)
	w.finishInit = true

	return nil
}

func (w *topicMsgWriter) openNewMsgFiles() error {
	w.FileSeq++
	fileSeqStr := strconv.FormatUint(uint64(w.FileSeq), 10)

	dataFileName := strings.TrimRight(w.DirOfDataFiles, "/") + w.TopicName + "." + fileSeqStr + ".dat"
	dataFp, err := os.OpenFile(dataFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		return err
	}

	indexFileName := strings.TrimRight(w.DirOfDataFiles, "/") + w.TopicName + "." + fileSeqStr + ".idx"
	indexFp, err := os.OpenFile(indexFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		return err
	}
	w.dataFileWriter = &dataFileWriter{
		fp: dataFp,
	}
	w.indexFileWriter = &indexFileWriter{
		fp: indexFp,
	}
	w.notifyNewFilesOpenedCh <- &NewFilesOpenedSignal{
		fileSeq: w.FileSeq,
	}
	
	return nil
}

func (w *topicMsgWriter) writeMsgs(msgs []*msgstorage.Message) error {
	for len(msgs) > 0 {
		var (
			batchWritableMsgs []*msgstorage.Message
			batchWritableMsgDataPayloadBytes uint32
			areMsgFilesFull bool
		)

		for _, msg := range msgs {
			msgDataPayload := msg.GetDataPayload()
			encodedMsgDataPayloadBytes :=  uint32(len(msgDataPayload.GetData()) + len(msgDataPayload.GetMsgId())) + DataPayloadBoundarySize
			batchWritableMsgDataPayloadBytes =+ encodedMsgDataPayloadBytes

			if w.indexFileWriter.writtenIndexNum + uint32(len(batchWritableMsgs)) >= w.indexFileWriter.maxWritableIndexNum - 1 ||
				w.dataFileWriter.writtenDataBytes + batchWritableMsgDataPayloadBytes >= w.dataFileWriter.maxWritableDataBytes
			{
				break
			}

			batchWritableMsgs = append(batchWritableMsgs, msg)
		}

		batchWritableMsgNum := len(batchWritableMsgs)

		if batchWritableMsgNum > 0 {
			indexesBuf, dataBuf, err := newMsgEncoder(msgs).encodeBuf()
			if err != nil {
				return err
			}

			var(
				dataBufLen = len(dataBuf)
				indexesBufLen = len(indexesBuf)
				totalWrittenNum int
			)
			for {
				writtenNum, err := w.indexFileWriter.fp.Write(dataBuf)
				if err != nil {
					return err
				}

				totalWrittenNum += writtenNum
				if totalWrittenNum >= dataBufLen {
					break
				}

				dataBuf = dataBuf[writtenNum:]
			}
			w.dataFileWriter.writtenDataBytes += uint32(dataBufLen)

			totalWrittenNum = 0
			for {
				writtenNum, err := w.indexFileWriter.fp.Write(indexesBuf)
				if err != nil {
					return err
				}

				totalWrittenNum += writtenNum
				if totalWrittenNum >= indexesBufLen {
					break
				}

				indexesBuf = indexesBuf[writtenNum:]
			}
			w.indexFileWriter.writtenIndexNum += uint32(batchWritableMsgNum)

			if len(msgs) <= batchWritableMsgNum {
				msgs = nil
			} else {
				msgs = msgs[batchWritableMsgNum:]
			}
		}

		if !areMsgFilesFull {
			continue
		}

		if err := w.openNewMsgFiles(); err != nil {
			return err
		}
	}

	return nil
}

func (w *topicMsgWriter) runWritingMsgLoop() error {
	for {
		var msgs []*msgstorage.Message
		msg := <- w.msgCh
		msgs = append(msgs, msg)
		for {
			var moreMsg *msgstorage.Message
			select {
				case moreMsg = <- w.msgCh:
					msgs = append(msgs, moreMsg)
				default:
			}
			if moreMsg == nil {
				break
			}
		}

		err := w.writeMsgs(msgs)
		if err != nil {
			return err
		}
	}
}
