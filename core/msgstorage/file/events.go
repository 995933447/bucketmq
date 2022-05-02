package file

type newFilesOpenEvent struct {
	fileSeq uint32 `access:"r"`
}

func (s *newFilesOpenEvent) getFileSeq() uint32 {
	return s.fileSeq
}
