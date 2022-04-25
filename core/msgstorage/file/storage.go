package file

import (
	"github.com/995933447/bucketmq/core/msgstorage"
)

type FileStorage struct {
	DirOfIndexFiles string
	DirOfDataFiles string
	MinSeqOfIndexFiles uint32
	MaxSeqOfIndexFiles uint32
	*topicMsgWriter
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