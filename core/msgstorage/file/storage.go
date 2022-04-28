package file

import (
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
}

func (fs *FileStorage) init(maxWritableTopicMsgNum, maxWritableTopicMsgBytes uint32) error {
	if fs.finishInit {
		return nil
	}

	var err error
	fs.topicMsgWriter, err = newTopicMsgWriter(
		fs.topicName,
		fs.indexDir,
		fs.dataDir,
		fs.maxSeqOfIndexFiles,
		maxWritableTopicMsgBytes,
		maxWritableTopicMsgNum,
		defaultSyncToDiskInterval,
	)
	if err != nil {
		return err
	}

	fs.finishInit = true
	return nil
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