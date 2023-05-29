package engine

import (
	"errors"
)

const topicDirPrefix = "bucketmq_"

type runState int

const (
	runStateNil runState = iota
	runStateRunning
	runStateStopping
	runStateStopped
	runStateExiting
	runStateExited
)

var (
	errSeqNotFound   = errors.New("file sequence not found")
	errFileCorrupted = errors.New("oh wtf?? file occur corruption")
)

const (
	bufBoundariesBytes = 4
	bufBoundaryBytes   = 2
	bufBoundaryBegin   = uint16(0x1234)
	bufBoundaryEnd     = uint16(0x5678)
)

type ExtraOfFileWritten struct {
	Topic            string
	Subscriber       string
	ContentCreatedAt uint32
}

var OnAnyFileWritten = func(fileName string, buf []byte, extra *ExtraOfFileWritten) {}
