package fileutil

import (
	errdef "github.com/995933447/bucketmqerrdef"
	"strconv"
	"strings"
)

func ParseFileSeqBeforeSuffix(fileName, suffixName string) (uint32, error) {
	suffixPos := strings.LastIndex(fileName, ".")
	if suffixPos == -1 {
		return 0, errdef.FileSeqNotFound
	}
	if fileSuffixName := fileName[suffixPos + 1:]; fileSuffixName != suffixName {
		return 0, errdef.FileSeqNotFound
	}
	seqPos := strings.LastIndex(fileName[:suffixPos], ".")
	if suffixPos == -1 {
		return 0, errdef.FileSeqNotFound
	}
	seqStr := fileName[seqPos + 1:suffixPos]
	seqForUnit64, err := strconv.ParseUint(seqStr, 10, 32)
	if err != nil {
		return 0, errdef.FileSeqNotFound
	}
	return uint32(seqForUnit64), nil
}
