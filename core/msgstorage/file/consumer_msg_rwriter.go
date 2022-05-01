package file

import (
	"context"
	"github.com/995933447/bucketmq/core/log"
	errdef "github.com/995933447/bucketmqerrdef"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

type consumerFilesReadWriter struct {
	*indexFileReader
	*dataFileReader
	*finishOffsetFileReadWriter
}

type consumerMsgReadWriter struct {
	indexDir string
	dataDir string
	finishOffsetDir string
	topicName string
	consumerName string
	minFileSeq uint32
	maxFileSeq uint32
	consumingFileSeq uint32
	preloadMsgFileNum uint32
	filesWriter *consumerFilesReadWriter
	stopLoopCh chan struct{}
	notifyNewFilesOpenedCh chan *NewFilesOpenedSignal
	syncToDiskInterval time.Duration
	hasFileCorruption bool
	finishInit bool
	logger log.Logger `access:"r"`
	readyStopLoop bool
}

func (rw *consumerMsgReadWriter) init(ctx context.Context) error {
	if err := rw.calMinOrMaxMsgFileSeq(ctx); err != nil {
		rw.logger.Error(ctx, err)
		return err
	}
	return nil
}

func (rw *consumerMsgReadWriter) loadFinishedIndexes() error {
	return nil
}

func (rw *consumerMsgReadWriter) calMinOrMaxMsgFileSeq(ctx context.Context) error {
	dirInfo, err := os.Stat(rw.indexDir)
	if err != nil {
		rw.logger.Error(ctx, err)
		return err
	}
	if os.IsNotExist(err) {
		if err = os.MkdirAll(rw.indexDir, os.FileMode(0755)); err != nil {
			rw.logger.Error(ctx, err)
			return err
		}
	}
	if !dirInfo.IsDir() {
		err = errdef.FileIsNotDirErr
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
		fileName := file.Name()
		suffixPos := strings.LastIndex(fileName, ".")
		if suffixPos == -1 {
			continue
		}
		if suffixName := fileName[suffixPos + 1:]; suffixName != indexFileSuffixName {
			continue
		}
		seqPos := strings.LastIndex(fileName[:suffixPos], ".")
		if suffixPos == -1 {
			continue
		}
		seqStr := fileName[seqPos + 1:suffixPos]
		seqForUnit64, err := strconv.ParseUint(seqStr, 10, 32)
		if err != nil {
			rw.logger.Error(ctx, err)
			return err
		}
		seq := uint32(seqForUnit64)
		
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