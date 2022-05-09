package file

type nextSeqFilesOpenEvent struct {
	fileSeq string `access:"r"`
}

func (s *nextSeqFilesOpenEvent) getFileSeq() string {
	return s.fileSeq
}

type WrittenMsgEvent struct {
	success bool `access:"r"`
}

func (w *WrittenMsgEvent) IsSuccess() bool {
	return w.success
}