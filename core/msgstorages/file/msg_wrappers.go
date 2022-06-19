package file

import (
	"github.com/995933447/bucketmq/core/msgstorages"
)

type fileMsgWrapper struct {
	msg     *msgstorages.Message
	fileSeq string
	dataOffset uint32
	dataLen uint32
}

type doneFileMsgMetadataWrapper struct {
	msgOffset uint64
	doneAt uint32
}

type attemptFileMsgMetadataWrapper struct {
	msgOffset uint64
	attemptCnt uint32
	lastAttemptedAt uint32
	nextAttemptedAt uint32
}