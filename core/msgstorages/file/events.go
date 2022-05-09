package file

type nextSeqFilesOpenEvent struct {
	fileSeq string `access:"r"`
}

func (s *nextSeqFilesOpenEvent) getFileSeq() string {
	return s.fileSeq
}
