package file

import (
	"github.com/995933447/bucketmq/core/msgstorage"
	errdef "github.com/995933447/bucketmqerrdef"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultSyncToDiskInterval = time.Second * 5
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

func (fw *filesWriter) syncToDisk() error {
	if err := fw.indexFileWriter.fp.Sync(); err != nil {
		return err
	}
	if err := fw.dataFileWriter.fp.Sync(); err != nil {
		return err
	}
	return nil
}

func (fw *filesWriter) checkFilesCorruption() (bool, error) {
	indexFileInfo, err := fw.indexFileWriter.fp.Stat()
	if err != nil {
		return false, err
	}

	if indexFileInfo.Size() % indexBufSize > 0 || uint32(indexFileInfo.Size() / indexBufSize) != fw.indexFileWriter.writtenIndexNum {
		return true, nil
	}

	dataFileInfo, err := fw.dataFileWriter.fp.Stat()
	if err != nil {
		return false, err
	}

	if uint32(dataFileInfo.Size()) != fw.dataFileWriter.writtenDataBytes {
		return true, nil
	}

	return false, nil
}

type NewFilesOpenedSignal struct {
	fileSeq uint32 `access:"r"`
}

func (s *NewFilesOpenedSignal) getFileSeq() uint32 {
	return s.fileSeq
}

type topicMsgWriter struct {
	IndexDir string
	DataDir string
	TopicName string
	FileSeq uint32
	*filesWriter
	msgCh chan *msgstorage.Message
	stopLoopCh chan struct{}
	notifyNewFilesOpenedCh chan *NewFilesOpenedSignal
	syncToDiskInterval time.Duration
	hasFileCorruption bool
	finishInit bool
}

func newTopicMsgWriter(
	topicName, indexDir, dataDir string,
	beginFileSeq, maxWritableMsgNum, maxWritableMsgBytes uint32,
	syncToDiskInterval time.Duration,
	) (*topicMsgWriter, error) {
	if syncToDiskInterval == 0 {
		syncToDiskInterval = defaultSyncToDiskInterval
	}

	writer := &topicMsgWriter{
		IndexDir: indexDir,
		DataDir: dataDir,
		TopicName: topicName,
		FileSeq: beginFileSeq,
		syncToDiskInterval: syncToDiskInterval,
	}

	if err := writer.init(maxWritableMsgNum, maxWritableMsgBytes); err != nil {
		return nil, err
	}

	return writer, nil
}

func (w *topicMsgWriter) checkFilesCorruption() (bool, error) {
	var err error
	if w.hasFileCorruption, err = w.filesWriter.checkFilesCorruption(); err != nil {
		return false, err
	}
	return w.hasFileCorruption, nil
}

func (w *topicMsgWriter) makeIndexFp() (*os.File, error) {
	_, err := os.Stat(w.IndexDir)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		if err = os.MkdirAll(w.IndexDir, os.FileMode(0755)); err != nil {
			return nil, err
		}
	}

	fileSeqStr := strconv.FormatUint(uint64(w.FileSeq), 10)
	indexFileName := strings.TrimRight(w.IndexDir, "/") + "/" + w.TopicName + "." + fileSeqStr + ".idx"
	indexFp, err := os.OpenFile(indexFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		return nil, err
	}
	return indexFp, nil
}

func (w *topicMsgWriter) makeDataFp() (*os.File, error) {
	_, err := os.Stat(w.DataDir)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		if err = os.MkdirAll(w.DataDir, os.FileMode(0755)); err != nil {
			return nil, err
		}
	}

	fileSeqStr := strconv.FormatUint(uint64(w.FileSeq), 10)
	dataFileName := strings.TrimRight(w.DataDir, "/") + "/" + w.TopicName + "." + fileSeqStr + ".dat"
	dataFp, err := os.OpenFile(dataFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		return nil, err
	}
	return dataFp, nil
}

func (w *topicMsgWriter) init(maxWritableMsgNum, maxWritableMsgBytes uint32) error {
	if w.finishInit {
		return nil
	}
	
	dataFp, err := w.makeDataFp()
	if err != nil {
		return err
	}

	indexFp, err := w.makeIndexFp()
	if err != nil {
		return err
	}

	w.filesWriter = &filesWriter{
		dataFileWriter: &dataFileWriter{
			maxWritableDataBytes: maxWritableMsgBytes,
			fp: dataFp,
		},
		indexFileWriter: &indexFileWriter{
			maxWritableIndexNum: maxWritableMsgNum,
			fp: indexFp,
		},
	}

	hasFileCorruption, err := w.checkFilesCorruption()
	if err != nil {
		return err
	} else if hasFileCorruption {
		return errdef.FileCorruptionErr
	}

	w.msgCh = make(chan *msgstorage.Message, 10000)
	w.stopLoopCh = make(chan struct{})
	w.notifyNewFilesOpenedCh = make(chan *NewFilesOpenedSignal)
	w.finishInit = true

	return nil
}

func (w *topicMsgWriter) openNewMsgFiles() error {
	w.FileSeq++

	dataFp, err := w.makeDataFp()
	if err != nil {
		return err
	}

	indexFp, err := w.makeIndexFp()
	if err != nil {
		return err
	}
	w.filesWriter = &filesWriter{
		dataFileWriter: &dataFileWriter{
			maxWritableDataBytes: w.maxWritableDataBytes,
			fp: dataFp,
		},
		indexFileWriter: &indexFileWriter{
			maxWritableIndexNum: w.maxWritableIndexNum,
			fp: indexFp,
		},
	}
	w.notifyNewFilesOpenedCh <- &NewFilesOpenedSignal{
		fileSeq: w.FileSeq,
	}
	
	return nil
}

func (w *topicMsgWriter) writeMsgs(msgs []*msgstorage.Message) error {
	if w.hasFileCorruption {
		return errdef.FileCorruptionErr
	}

	for len(msgs) > 0 {
		var (
			batchWritableMsgs []*msgstorage.Message
			batchWritableMsgDataBufBytes uint32
			areMsgFilesFull bool
		)

		for _, msg := range msgs {
			batchWritableMsgDataBufBytes += getDefaultMsgEncoder().getMsgsDataBufBytes([]*msgstorage.Message{msg})
			if w.indexFileWriter.writtenIndexNum + uint32(len(batchWritableMsgs)) >= w.indexFileWriter.maxWritableIndexNum ||
				w.dataFileWriter.writtenDataBytes + batchWritableMsgDataBufBytes >= w.dataFileWriter.maxWritableDataBytes {
				areMsgFilesFull = true
				break
			}

			batchWritableMsgs = append(batchWritableMsgs, msg)
		}

		batchWritableMsgNum := len(batchWritableMsgs)

		if batchWritableMsgNum > 0 {
			indexesBuf, dataBuf, err := getDefaultMsgEncoder().encodeBuf(msgs)
			if err != nil {
				return err
			}

			var(
				dataBufLen = len(dataBuf)
				indexesBufLen = len(indexesBuf)
				totalWrittenNum int
			)
			for {
				writtenNum, err := w.dataFileWriter.fp.Write(dataBuf)
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

func (w *topicMsgWriter) loop() error {
	syncToDiskTick := time.NewTicker(w.syncToDiskInterval)
	defer syncToDiskTick.Stop()
	for {
		select {
			case msg := <- w.msgCh:
				var msgs []*msgstorage.Message
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
			case <- syncToDiskTick.C:
				var (
					hasFileCorruption bool
					err error
				)
				if hasFileCorruption, err = w.checkFilesCorruption(); err != nil {
					return err
				} else if hasFileCorruption {
					return errdef.FileCorruptionErr
				} else {
					_ =	w.syncToDisk()
				}
			case <- w.stopLoopCh:
				return nil
		}
	}
}
