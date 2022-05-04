package file

import (
	"os"
)

const (
	minFileSeq = 1
)

const (
	indexFileSuffixName = "idx"
	dataFileSuffixName = "dat"
	offsetFileSuffixName = "offset"
	attemptCntFileSuffixName = "attempt"
	failedFileSuffixName = "fail"
)

type indexFileWriter struct {
	dir string
	fp *os.File
	maxWritableIndexNum uint32
	writtenIndexNum uint32
}

type indexFileReader struct {
	fp *os.File
}

type dataFileWriter struct {
	dir string
	fp *os.File
	maxWritableDataBytes uint32
	writtenDataBytes uint32
}

type dataFileReader struct {
	fp *os.File
}

type offsetFileReadWriter struct {
	fp *os.File
}