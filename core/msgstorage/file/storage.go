package file

import (
	"context"
	"github.com/995933447/bucketmq/core/log"
	"github.com/995933447/bucketmq/core/msgstorage"
)

type FileStorage struct {
	indexDir string	`access:"r"`
	dataDir string `access:"r"`
	minSeqOfIndexFiles uint32 `access:"r"`
	maxSeqOfIndexFiles uint32 `access:"r"`
	topicName string `access:"r"`
	*topicMsgWriter
	finishInit bool
	logger log.Logger `access:"rw"`
}

func (fs *FileStorage) init(ctx context.Context, maxWritableTopicMsgNum, maxWritableTopicMsgBytes uint32) error {
	if fs.finishInit {
		return nil
	}

	var err error
	fs.topicMsgWriter, err = newTopicMsgWriter(
		ctx,
		fs.topicName,
		fs.indexDir,
		fs.dataDir,
		fs.maxSeqOfIndexFiles,
		maxWritableTopicMsgBytes,
		maxWritableTopicMsgNum,
		defaultSyncToDiskInterval,
		fs.logger,
	)
	if err != nil {
		log.DefaultLogger.Error(ctx, err)
		return err
	}

	fs.finishInit = true
	return nil
}

func (fs *FileStorage) GetLogger() log.Logger {
	return fs.logger
}

func (fs *FileStorage) SetLogger(logger log.Logger) {
	if logger == nil {
		return
	}
	fs.logger = logger
}

func (fs *FileStorage) GetTopicName() string {
	return fs.topicName
}

func (fs *FileStorage) GetIndexDir() string {
	return fs.indexDir
}

func (fs *FileStorage) GetDataDir() string {
	return fs.dataDir
}

func (fs *FileStorage) GetMinSeqOfIndexFiles() uint32 {
	return fs.minSeqOfIndexFiles
}

func (fs *FileStorage) GetMaxSeqOfIndexFiles() uint32 {
	return fs.maxSeqOfIndexFiles
}

func (fs *FileStorage) PushMsg(*msgstorage.Message) error {
	return nil
}

func (fs *FileStorage) PopMsg() (*msgstorage.Message, error) {
	return nil, nil
}

func (fs *FileStorage) SetMsgConsumptionCompleted(msgId string) error  {
	return nil
}

func (fs *FileStorage) SetMsgWillBeRetried(*msgstorage.RetryMsgReq) error  {
	return nil
}