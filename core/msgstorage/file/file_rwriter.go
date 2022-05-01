package file

import "os"

const (
	indexFileSuffixName = "idx"
	dataFileSuffixName = "dat"
	offsetFileSuffixName = "offset"
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

type offsetFileReadWriter struct {
	fp *os.File
}