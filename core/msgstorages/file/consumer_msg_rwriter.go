package file

import (
	"context"
	"github.com/995933447/bucketmq/core/log"
	"github.com/995933447/bucketmq/core/utils/fileutil"
	"github.com/995933447/bucketmq/core/utils/structs"
	errdef "github.com/995933447/bucketmqerrdef"
	"io"
	"io/ioutil"
	"os"
	"time"
)

type consumerFileReadWritersWrapper struct {
	fileSeqToIndexFileReaderMap map[uint32]*indexFileReader
	*dataFileReader
	*offsetFileReadWriter
}

type consumerMsgReadWriter struct {
	topicName string
	consumerGroupName string
	indexDir string
	dataDir string
	offsetDir string
	maxFileSeq uint32
	confirmMaxFileSeq bool
	consumingFileSeq uint32
	confirmConsumingFileSeq bool
	fileReadWritersWrapper *consumerFileReadWritersWrapper
	preloadMsgFileNum uint32
	finishedOffsetsOfConsumingFile *structs.Uint32Set
	hasFileCorruption bool
	syncToDiskInterval time.Duration
	*msgEncoder
	logger log.Logger `access:"r"`
	readyStopLoop bool
	stopLoopEventCh chan struct{}
	newFilesOpenEventCh chan *nextSeqFilesOpenEvent
	finishInit bool
}

func (rw *consumerMsgReadWriter) init(ctx context.Context) error {
	if err := rw.calMsgFileSeqInfo(ctx); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	if err := rw.openAndPreloadMsgFiles(ctx); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	return nil
}

func (rw *consumerMsgReadWriter) openAndPreloadMsgFiles(ctx context.Context) error {
	if err := rw.initOffsetFileReadWriter(ctx); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	if err := rw.initIndexFileReaders(ctx); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	if err := rw.initDataFileReader(ctx); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	if err := rw.loadFinishedOffsets(ctx); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	if err := rw.loadIndexes(ctx); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	return nil
}

func (rw *consumerMsgReadWriter) initDataFileReader(ctx context.Context) error {
	dataFp, err := rw.makeDataFp(ctx)
	if err != nil {
		rw.logger.Error(ctx, err)
		return err
	}
	fileInfo, err := dataFp.Stat()
	if err != nil {
		rw.logger.Error(ctx, err)
		return err
	}
	fileSize := fileInfo.Size()
	if fileSize % offsetBufSize > 0 {
		rw.hasFileCorruption = true
		err = errdef.FileCorruptionErr
		rw.logger.Error(ctx, err)
		return err
	}

	if rw.fileReadWritersWrapper == nil {
		rw.fileReadWritersWrapper = &consumerFileReadWritersWrapper{}
	}
	if rw.fileReadWritersWrapper.dataFileReader == nil {
		rw.fileReadWritersWrapper.dataFileReader = &dataFileReader{}
	}
	rw.fileReadWritersWrapper.dataFileReader.fp = dataFp

	return nil
}

func (rw *consumerMsgReadWriter) initIndexFileReaders(ctx context.Context) error {
	rw.assetConfirmedMaxMsgFileSeq()
	rw.assetConfirmedConsumingMsgFileSeq()

	if rw.fileReadWritersWrapper == nil {
		rw.fileReadWritersWrapper = &consumerFileReadWritersWrapper{}
	}
	if rw.fileReadWritersWrapper.fileSeqToIndexFileReaderMap == nil {
		rw.fileReadWritersWrapper.fileSeqToIndexFileReaderMap = make(map[uint32]*indexFileReader)
	}

	var (
		fileSeqToIndexFileReaderMap = rw.fileReadWritersWrapper.fileSeqToIndexFileReaderMap
		currentFileSeq = rw.consumingFileSeq
	)
	for ; currentFileSeq <= rw.maxFileSeq && uint32(len(fileSeqToIndexFileReaderMap)) <= rw.preloadMsgFileNum; currentFileSeq++ {
		if _, ok := fileSeqToIndexFileReaderMap[currentFileSeq]; !ok {
			continue
		}
		fp, err := rw.makeIndexFp(ctx, currentFileSeq)
		if err != nil {
			if os.IsNotExist(err) || err == errdef.FileIsNotRegularFileErr {
				continue
			}
			rw.logger.Error(ctx, err)
			return err
		}
		fileSeqToIndexFileReaderMap[currentFileSeq] = &indexFileReader{fp: fp}
	}

	return nil
}

func (rw *consumerMsgReadWriter) initOffsetFileReadWriter(ctx context.Context) error {
	offsetFp, err := rw.makeOffsetFp(ctx)
	if err != nil {
		rw.logger.Error(ctx, err)
		return err
	}
	fileInfo, err := offsetFp.Stat()
	if err != nil {
		rw.logger.Error(ctx, err)
		return err
	}
	fileSize := fileInfo.Size()
	if fileSize % offsetBufSize > 0 {
		rw.hasFileCorruption = true
		err = errdef.FileCorruptionErr
		rw.logger.Error(ctx, err)
		return err
	}

	if rw.fileReadWritersWrapper == nil {
		rw.fileReadWritersWrapper = &consumerFileReadWritersWrapper{}
	}
	if rw.fileReadWritersWrapper.offsetFileReadWriter == nil {
		rw.fileReadWritersWrapper.offsetFileReadWriter = &offsetFileReadWriter{}
	}
	rw.fileReadWritersWrapper.offsetFileReadWriter.fp = offsetFp

	return nil
}

func (rw *consumerMsgReadWriter) calMsgFileSeqInfo(ctx context.Context) error {
	if err := rw.calMaxMsgFileSeq(ctx); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	if err := rw.sureConsumingMsgFileSeq(ctx); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	return nil
}

func (rw *consumerMsgReadWriter) assetConfirmedMaxMsgFileSeq() {
	if rw.confirmMaxFileSeq {
		return
	}
	panic("must call *consumerMsgReadWriter.calMsgFileSeqInfo(context.Context) to set *consumerMsgReadWriter.maxFileSeq first.")
}

func (rw *consumerMsgReadWriter) assetConfirmedConsumingMsgFileSeq() {
	if rw.confirmConsumingFileSeq {
		return
	}
	panic("must call *consumerMsgReadWriter.sureConsumingMsgFileSeq(context.Context) to set *consumerMsgReadWriter.maxFileSeq first.")
}

func (rw *consumerMsgReadWriter) sureConsumingMsgFileSeq(ctx context.Context) error {
	rw.assetConfirmedMaxMsgFileSeq()

	if err := fileutil.MkdirIfNotExist(rw.offsetDir); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	offsetFiles, err := ioutil.ReadDir(rw.offsetDir)
	if err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	var maxOffsetFileSeq uint32
	for _, file := range offsetFiles {
		if file.IsDir() {
			continue
		}

		seq, err := fileutil.ParseFileSeqBeforeSuffix(file.Name(), offsetFileSuffixName)
		if err != nil {
			rw.logger.Error(ctx, err)
			return err
		}

		if maxOffsetFileSeq == 0 || maxOffsetFileSeq < seq {
			maxOffsetFileSeq = seq
		}
	}

	if maxOffsetFileSeq > 0 {
		if rw.maxFileSeq > maxOffsetFileSeq {
			consumingIndexFileName := fileutil.BuildIndexFileName(rw.topicName, rw.dataDir, indexFileSuffixName, maxOffsetFileSeq)
			consumingIndexFileInfo, err := os.Stat(consumingIndexFileName)
			if err == nil && !consumingIndexFileInfo.IsDir() {
				consumingIndexFileSize := consumingIndexFileInfo.Size()
				if consumingIndexFileSize % indexBufSize > 0 {
					rw.hasFileCorruption = true
					err = errdef.FileCorruptionErr
					rw.logger.Error(ctx, err)
					return err
				}
				indexNumOfConsumingFile := consumingIndexFileSize / indexBufSize

				offsetFileName := fileutil.BuildOffsetFileName(rw.topicName, rw.consumerGroupName, rw.offsetDir, offsetFileSuffixName, maxOffsetFileSeq)
				offsetFileInfo, err := os.Stat(offsetFileName)
				if err == nil {
					offsetFileSize := offsetFileInfo.Size()
					if offsetFileSize % offsetBufSize > 0 {
						rw.hasFileCorruption = true
						err = errdef.FileCorruptionErr
						rw.logger.Error(ctx, err)
						return err
					}
					if offsetFileSize / indexBufSize == indexNumOfConsumingFile {
						maxOffsetFileSeq++
					}
				} else if !os.IsNotExist(err) {
					rw.logger.Error(ctx, err)
					return err
				}
			} else if !os.IsNotExist(err) {
				rw.logger.Error(ctx, err)
				return err
			}
		}
	}


	rw.consumingFileSeq = maxOffsetFileSeq

	return nil
}

func (rw *consumerMsgReadWriter) loadIndexes(ctx context.Context) error {
	return nil
}


func (rw *consumerMsgReadWriter) loadFinishedOffsets(ctx context.Context) error {
	if rw.fileReadWritersWrapper == nil || rw.fileReadWritersWrapper.offsetFileReadWriter == nil {
		if err := rw.initOffsetFileReadWriter(ctx); err != nil {
			rw.logger.Error(ctx, err)
			return err
		}
	}

	var (
		offsetFp =  rw.fileReadWritersWrapper.offsetFileReadWriter.fp
		offsetsBuf []byte
	)
	for {
		buf := make([]byte, 10240 * offsetBufSize)
		_, err := offsetFp.Read(buf)
		if err != nil {
			if err != io.EOF {
				break
			}

			rw.logger.Error(ctx, err)
			return err
		}

		offsetsBuf = append(offsetsBuf, buf...)
	}

	if len(offsetsBuf) % offsetBufSize > 0 {
		rw.hasFileCorruption = true
		err := errdef.FileCorruptionErr
		rw.logger.Error(ctx, err)
		return err
	}

	if rw.msgEncoder == nil {
		rw.msgEncoder = &msgEncoder{}
	}
	offsets, err := rw.msgEncoder.decodeOffsets(offsetsBuf)
	if err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	if rw.finishedOffsetsOfConsumingFile == nil {
		rw.finishedOffsetsOfConsumingFile = &structs.Uint32Set{}
	}
	for _, offset := range offsets {
		rw.finishedOffsetsOfConsumingFile.Put(offset)
	}

	return nil
}

func (rw *consumerMsgReadWriter) makeDataFp(ctx context.Context) (*os.File, error) {
	rw.assetConfirmedConsumingMsgFileSeq()

 	if err := fileutil.MkdirIfNotExist(rw.dataDir); err != nil {
		rw.logger.Error(ctx, err)
		return nil, err
	}

	dataFileName := fileutil.BuildDataFileName(rw.topicName, rw.dataDir, dataFileSuffixName, rw.consumingFileSeq)
	fp, err := os.OpenFile(dataFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		rw.logger.Error(ctx, err)
		return nil, err
	}

	fileInfo, err := fp.Stat()
	if err != nil {
		return nil, err
	}

	if fileInfo.IsDir() {
		err = errdef.FileIsNotRegularFileErr
		rw.logger.Error(ctx, err)
		return nil, err
	}

	return fp, nil
}

func (rw *consumerMsgReadWriter) makeOffsetFp(ctx context.Context) (*os.File, error) {
	rw.assetConfirmedConsumingMsgFileSeq()

	if err := fileutil.MkdirIfNotExist(rw.offsetDir); err != nil {
		rw.logger.Error(ctx, err)
		return nil, err
	}

	offsetFileName := fileutil.BuildOffsetFileName(rw.topicName, rw.dataDir, rw.consumerGroupName, dataFileSuffixName, rw.consumingFileSeq)
	fp, err := os.OpenFile(offsetFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		rw.logger.Error(ctx, err)
		return nil, err
	}

	fileInfo, err := fp.Stat()
	if err != nil {
		return nil, err
	}

	if fileInfo.IsDir() {
		err = errdef.FileIsNotRegularFileErr
		rw.logger.Error(ctx, err)
		return nil, err
	}

	return fp, nil
}

func (rw *consumerMsgReadWriter) makeIndexFp(ctx context.Context, fileSeq uint32) (*os.File, error) {
	indexFileName := fileutil.BuildIndexFileName(rw.topicName, rw.indexDir, indexFileSuffixName, fileSeq)
	fp, err := os.OpenFile(indexFileName, os.O_RDONLY, os.FileMode(0755))
	if err != nil {
		if !os.IsNotExist(err) {
			rw.logger.Error(ctx, err)
		}
		return nil, err
	}

	fileInfo, err := fp.Stat()
	if err != nil {
		return nil, err
	}

	if fileInfo.IsDir() {
		err = errdef.FileIsNotRegularFileErr
		rw.logger.Error(ctx, err)
		return nil, err
	}

	return fp, nil
}

func (rw *consumerMsgReadWriter) calMaxMsgFileSeq(ctx context.Context) error {
	if err := fileutil.MkdirIfNotExist(rw.indexDir); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	files, err := ioutil.ReadDir(rw.indexDir)
	if err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	var maxFileSeq uint32
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		seq, err := fileutil.ParseFileSeqBeforeSuffix(file.Name(), indexFileSuffixName)
		if err != nil {
			rw.logger.Error(ctx, err)
			return err
		}

		if maxFileSeq == 0 || maxFileSeq < seq {
			maxFileSeq = seq
		}
	}
	
	rw.maxFileSeq = maxFileSeq
	rw.confirmMaxFileSeq = true

	return nil
}