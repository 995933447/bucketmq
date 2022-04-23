package file

import (
	"github.com/995933447/bucketmq/core/msgstorage"
	"os"
	"strconv"
	"strings"
)

type indexFileWriter struct {
	fp *os.File
	totalWritableIndexNum uint32
	writtenIndexNum uint32
}

type dataFileWriter struct {
	fp *os.File
	totalWritableDataBytes uint32
	writtenDataBytes uint32
}

type NewFilesOpenedSignal struct {
	fileSeq uint32 `access:"r"`
}

func (s *NewFilesOpenedSignal) getFileSeq() uint32 {
	return s.fileSeq
}

type FileStorageWriter struct {
	finishInit bool
	DirOfIndexFiles string
	DirOfDataFiles string
	BaseFileName string
	FileSuffixSeq uint32
	indexFileWriter *indexFileWriter
	dataFileWriter *dataFileWriter
	msgCh chan *msgstorage.Message
	stopWritingMsgLoopCh chan struct{}
	notifyNewFilesOpenedCh chan *NewFilesOpenedSignal
}

func (s *NewFilesOpenedSignal) getSeq() uint32 {
	return s.fileSeq
}

func (w *FileStorageWriter) getIndexFp() (*os.File, error) {
	fileSeqStr := strconv.FormatUint(uint64(w.FileSuffixSeq), 10)
	indexFileName := strings.TrimRight(w.DirOfDataFiles, "/") + w.BaseFileName + "." + fileSeqStr + ".idx"
	indexFp, err := os.OpenFile(indexFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {

	}
	return indexFp, nil
}

func (w *FileStorageWriter) getDataFp() (*os.File, error) {
	fileSeqStr := strconv.FormatUint(uint64(w.FileSuffixSeq), 10)
	dataFileName := strings.TrimRight(w.DirOfDataFiles, "/") + w.BaseFileName + "." + fileSeqStr + ".dat"
	dataFp, err := os.OpenFile(dataFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {

	}
	return dataFp, nil
}

func (w *FileStorageWriter) init(totalWritableMsgNum, totalWritableMsgBytes uint32) error {
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
		totalWritableDataBytes: totalWritableMsgBytes,
		fp: dataFp,
	}
	w.indexFileWriter = &indexFileWriter{
		totalWritableIndexNum: totalWritableMsgNum,
		fp: indexFp,
	}
	w.msgCh = make(chan *msgstorage.Message, 10000)
	w.stopWritingMsgLoopCh = make(chan struct{})
	w.notifyNewFilesOpenedCh = make(chan *NewFilesOpenedSignal)
	w.finishInit = true

	return nil
}

func (w *FileStorageWriter) openNewMsgFile() error {
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
		totalWritableDataBytes: w.dataFileWriter.totalWritableDataBytes,
		fp: dataFp,
	}
	w.indexFileWriter = &indexFileWriter{
		totalWritableIndexNum: w.indexFileWriter.totalWritableIndexNum,
		fp: indexFp,
	}
	
	w.notifyNewFilesOpenedCh <- &NewFilesOpenedSignal{
		fileSeq: w.FileSuffixSeq,
	}
	
	return nil
}

func (w *FileStorageWriter) runWritingMsgLoop() error {
	for {
		msg := <- w.msgCh

		if w.indexFileWriter.writtenIndexNum >= w.indexFileWriter.totalWritableIndexNum || 
			w.dataFileWriter.writtenDataBytes >= w.dataFileWriter.totalWritableDataBytes {
			w.FileSuffixSeq++
			err := w.openNewMsgFile()
			if err != nil {
				return err
			}
		}
	}
}
