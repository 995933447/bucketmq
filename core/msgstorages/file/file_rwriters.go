package file

import (
	"os"
)

const (
	indexFileSuffixName            = "index"
	dataFileSuffixName             = "data"
	doneFileSuffixName             = "done"
	attemptFileSuffixName          = "attempt"
	startOffsetCheckFileSuffixName = "start"
)

const (
	filePrefixNameSep       = "."
	filePrefixNameMiddleSep = "."
	fileSeqMiddleSep        = "-"
)

type indexFileWriter struct {
	fp                  *os.File
	maxWritableIndexNum uint32
	writtenIndexNum     uint32
}

type indexFileReader struct {
	fp *os.File
	cursor int64
	indexNum uint32
}

type dataFileWriter struct {
	fp                   *os.File
	maxWritableDataBytes uint32
	writtenDataBytes     uint32
}

type dataFileReader struct {
	fp *os.File
}

type doneFileRWriter struct {
	fp *os.File
}

type attemptFileRWriter struct {
	fp *os.File
}

type startOffsetCheckFileRWriter struct {
	fp *os.File
}
