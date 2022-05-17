package file

type nextSeqFilesOpenEvent struct {
	fileSeq string `access:"r"`
}

func (e *nextSeqFilesOpenEvent) getFileSeq() string {
	return e.fileSeq
}

type WrittenMsgEvent struct {
	msgOffset uint64 `access:"r"`
	err error `access:"r"`
}

func (e *WrittenMsgEvent) MsgOffset() uint64 {
	return e.msgOffset
}

func (e *WrittenMsgEvent) IsSuccess() bool {
	return e.err == nil
}

func (e *WrittenMsgEvent) GetErr() error {
	return e.err
}