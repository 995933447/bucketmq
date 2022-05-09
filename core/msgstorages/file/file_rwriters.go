package file

import (
	"os"
)

const (
	globalFirstMsgOffset = 0
)

const (
	indexFileSuffixName = "idx"
	dataFileSuffixName = "dat"
	doneFileSuffixName = "don"
	attemptCntFileSuffixName = "attempt"
	deadFileSuffixName = "dea"
	indexOfIndexFileSuffixName = "idxidx"
)

const (
	filePrefixNameSep = "."
	filePrefixNameMiddleSep = "."
	fileSeqMiddleSep = "-"
)

type indexFileWriter struct {
	fp *os.File
	maxWritableIndexNum uint32
	writtenIndexNum uint32
}

type indexFileReader struct {
	fp *os.File
}

type dataFileWriter struct {
	fp *os.File
	maxWritableDataBytes uint32
	writtenDataBytes uint32
}

type dataFileReader struct {
	fp *os.File
}
