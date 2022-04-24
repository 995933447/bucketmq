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

type StorageWriter struct {
	finishInit bool
	DirOfIndexFiles string
	DirOfDataFiles string
	BaseFileName string
	FileSuffixSeq uint32
	*filesWriter
	msgCh chan *msgstorage.Message
	stopWritingMsgLoopCh chan struct{}
	notifyNewFilesOpenedCh chan *NewFilesOpenedSignal
}

func (s *NewFilesOpenedSignal) getSeq() uint32 {
	return s.fileSeq
}

func (w *StorageWriter) getIndexFp() (*os.File, error) {
	fileSeqStr := strconv.FormatUint(uint64(w.FileSuffixSeq), 10)
	indexFileName := strings.TrimRight(w.DirOfDataFiles, "/") + w.BaseFileName + "." + fileSeqStr + ".idx"
	indexFp, err := os.OpenFile(indexFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {

	}
	return indexFp, nil
}

func (w *StorageWriter) getDataFp() (*os.File, error) {
	fileSeqStr := strconv.FormatUint(uint64(w.FileSuffixSeq), 10)
	dataFileName := strings.TrimRight(w.DirOfDataFiles, "/") + w.BaseFileName + "." + fileSeqStr + ".dat"
	dataFp, err := os.OpenFile(dataFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {

	}
	return dataFp, nil
}

func (w *StorageWriter) init(totalWritableMsgNum, totalWritableMsgBytes uint32) error {
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

func (w *StorageWriter) openNewMsgFiles() error {
	w.FileSuffixSeq++
	fileSeqStr := strconv.FormatUint(uint64(w.FileSuffixSeq), 10)

	dataFileName := strings.TrimRight(w.DirOfDataFiles, "/") + w.BaseFileName + "." + fileSeqStr + ".dat"
	dataFp, err := os.OpenFile(dataFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		return err
	}

	indexFileName := strings.TrimRight(w.DirOfDataFiles, "/") + w.BaseFileName + "." + fileSeqStr + ".idx"
	indexFp, err := os.OpenFile(indexFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		return err
	}
	w.dataFileWriter = &dataFileWriter{
		maxWritableDataBytes: w.dataFileWriter.maxWritableDataBytes,
		fp: dataFp,
	}
	w.indexFileWriter = &indexFileWriter{
		maxWritableIndexNum: w.indexFileWriter.maxWritableIndexNum,
		fp: indexFp,
	}
	w.notifyNewFilesOpenedCh <- &NewFilesOpenedSignal{
		fileSeq: w.FileSuffixSeq,
	}
	
	return nil
}

func (w *StorageWriter) msgFilesFull(newMsg *msgstorage.Message) bool {
	msgDataPayload := newMsg.GetDataPayload()
	encodedMsgDataPayloadLen :=  uint32(len(msgDataPayload.GetData()) + len(msgDataPayload.GetMsgId())) + DataPayloadBoundarySize

	return w.indexFileWriter.writtenIndexNum >= w.indexFileWriter.maxWritableIndexNum - 1 ||
		w.dataFileWriter.writtenDataBytes + encodedMsgDataPayloadLen >= w.dataFileWriter.maxWritableDataBytes
}

func (w *StorageWriter) batchWriteMsgs(msgs []*msgstorage.Message) error {
	for len(msgs) > 0 {
		var (
			readyWriteMsgs []*msgstorage.Message
			readyWriteMsgNum uint32
		)

		for _, msg := range msgs {
			if w.msgFilesFull(msg) {
				break
			}

			readyWriteMsgs = append(readyWriteMsgs, msg)
			readyWriteMsgNum++
		}

		// Real write
		// TODO

		if uint32(len(msgs)) <= readyWriteMsgNum {
			msgs = nil
			break
		}

		if readyWriteMsgNum > 0 {
			msgs = msgs[readyWriteMsgNum:]
		}

		if err := w.openNewMsgFiles(); err != nil {
			return err
		}
	}

	return nil
}

func (w *StorageWriter) runWritingMsgLoop() error {
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

		err := w.batchWriteMsgs(msgs)
		if err != nil {
			return err
		}
	}
}
