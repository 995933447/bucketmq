package file

import (
	"context"
	"github.com/995933447/bucketmq/core/log"
	"github.com/995933447/bucketmq/utils/fileutil"
	errdef "github.com/995933447/bucketmqerrdef"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

type consumerFilesReadWriter struct {
	indexFileReaders []*indexFileReader
	*dataFileReader
	*offsetFileReadWriter
}

type finishedOffsetSet struct {
	offsetMap map[uint32]struct{}
}

func (c *finishedOffsetSet) put(offset uint32) {
	if c.offsetMap == nil {
		c.offsetMap = make(map[uint32]struct{})
	}
	c.offsetMap[offset] = struct{}{}
}

func (c *finishedOffsetSet) exist(offset uint32) bool {
	if c.offsetMap == nil {
		return false
	}
	_, ok := c.offsetMap[offset]
	return ok
}

type consumerMsgReadWriter struct {
	indexDir string
	dataDir string
	offsetDir string
	topicName string
	consumerName string
	minFileSeq uint32
	maxFileSeq uint32
	consumingFileSeq uint32
	finishInitFileSeqInfo bool
	preloadMsgFileNum uint32
	hasFileCorruption bool
	finishedOffsetsOfConsumingFile *finishedOffsetSet
	logger log.Logger `access:"r"`
	readyStopLoop bool
	filesWriter *consumerFilesReadWriter
	stopLoopCh chan struct{}
	notifyNewFilesOpenedCh chan *NewFilesOpenedSignal
	syncToDiskInterval time.Duration
	finishInit bool
}

func (rw *consumerMsgReadWriter) init(ctx context.Context) error {
	if err := rw.initMsgFileSeqInfo(ctx); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}
	if err := rw.initOffsetFileReadWriter(ctx); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}
	if err := rw.loadFinishedOffsets(ctx); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}
	// TODO

	return nil
}

func (rw *consumerMsgReadWriter) initOffsetFileReadWriter(ctx context.Context) error {
	if !rw.finishInitFileSeqInfo {
		if err := rw.initMsgFileSeqInfo(ctx); err != nil {
			rw.logger.Error(ctx, err)
			return err
		}
	}

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

	if rw.filesWriter == nil {
		rw.filesWriter = &consumerFilesReadWriter{}
	}
	if rw.filesWriter.offsetFileReadWriter == nil {
		rw.filesWriter.offsetFileReadWriter = &offsetFileReadWriter{}
	}
	rw.filesWriter.offsetFileReadWriter.fp = offsetFp
	return nil
}

func (rw *consumerMsgReadWriter) initMsgFileSeqInfo(ctx context.Context) error {
	if rw.finishInitFileSeqInfo {
		return nil
	}

	if err := rw.calMinOrMaxMsgFileSeq(ctx); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}
	if err := rw.sureConsumingMsgFileSeq(ctx); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	rw.finishInitFileSeqInfo = true
	return nil
}

func (rw *consumerMsgReadWriter) sureConsumingMsgFileSeq(ctx context.Context) error {
	if err := fileutil.MkdirIfNotExist(ctx, rw.offsetDir); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	files, err := ioutil.ReadDir(rw.offsetDir)
	if err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	var maxFileSeq uint32
	for _, file := range files {
		seq, err := fileutil.ParseFileSeqBeforeSuffix(file.Name(), offsetFileSuffixName)
		if err != nil {
			rw.logger.Error(ctx, err)
			return err
		}
		if maxFileSeq == 0 || maxFileSeq < seq {
			maxFileSeq = seq
		}
	}
	rw.consumingFileSeq = maxFileSeq

	return nil
}

func (rw *consumerMsgReadWriter) loadFinishedOffsets(ctx context.Context) error {
	if rw.filesWriter == nil || rw.filesWriter.offsetFileReadWriter == nil {
		if err := rw.initOffsetFileReadWriter(ctx); err != nil {
			rw.logger.Error(ctx, err)
			return err
		}
	}

	var (
		offsetFp =  rw.filesWriter.offsetFileReadWriter.fp
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

	offsets, err := getDefaultMsgEncoder().decodeOffsets(offsetsBuf)
	if err != nil {
		rw.logger.Error(ctx, err)
		return err
	}
	for _, offset := range offsets {
		rw.finishedOffsetsOfConsumingFile.put(offset)
	}

	return nil
}

func (rw *consumerMsgReadWriter) makeOffsetFp(ctx context.Context) (*os.File, error) {
	fileSeqStr := strconv.FormatUint(uint64(rw.consumingFileSeq), 10)
	finishOffsetFileName := strings.TrimRight(rw.offsetDir, "/") +
		"/" + rw.topicName + "." + rw.consumerName + "." + fileSeqStr + "." + offsetFileSuffixName
	fileFp, err := os.OpenFile(finishOffsetFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
	if err != nil {
		rw.logger.Error(ctx, err)
		return nil, err
	}
	return fileFp, nil
}

func (rw *consumerMsgReadWriter) calMinOrMaxMsgFileSeq(ctx context.Context) error {
	if err := fileutil.MkdirIfNotExist(ctx, rw.dataDir); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	files, err := ioutil.ReadDir(rw.indexDir)
	if err != nil {
		rw.logger.Error(ctx, err)
		return err
	}

	var minFileSeq, maxFileSeq uint32
	for _, file := range files {
		seq, err := fileutil.ParseFileSeqBeforeSuffix(file.Name(), indexFileSuffixName)
		if err != nil {
			rw.logger.Error(ctx, err)
			return err
		}
		if minFileSeq == 0 || minFileSeq > seq {
			minFileSeq = seq
		}
		if maxFileSeq == 0 || maxFileSeq < seq {
			maxFileSeq = seq
		}
	}
	
	rw.maxFileSeq = maxFileSeq
	rw.minFileSeq = minFileSeq

	return nil
}