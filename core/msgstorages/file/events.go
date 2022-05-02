package file

type nextSeqFilesOpenEvent struct {
	fileSeq uint32 `access:"r"`
}

func (s *nextSeqFilesOpenEvent) getFileSeq() uint32 {
	return s.fileSeq
}
