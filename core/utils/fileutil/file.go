package fileutil

import (
	errdef "github.com/995933447/bucketmqerrdef"
	"strconv"
	"strings"
)

func TrimPreSuffixToParseFileSeq(fileName, prefixName, suffixName string) (uint32, error) {
	if !strings.HasPrefix(fileName, prefixName + ".") {
		return 0, errdef.FileSeqNotFoundErr
	}
	suffixPos := strings.LastIndex(fileName, ".")
	if suffixPos == -1 {
		return 0, errdef.FileSeqNotFoundErr
	}
	if fileSuffixName := fileName[suffixPos + 1:]; fileSuffixName != suffixName {
		return 0, errdef.FileSeqNotFoundErr
	}
	seqStr := fileName[len(prefixName) + 1:suffixPos]
	seqForUnit64, err := strconv.ParseUint(seqStr, 10, 32)
	if err != nil {
		return 0, errdef.FileSeqNotFoundErr
	}
	return uint32(seqForUnit64), nil
}

func TrimSuffixToParseFileSeq(fileName, suffixName string) (uint32, error) {
	suffixPos := strings.LastIndex(fileName, ".")
	if suffixPos == -1 {
		return 0, errdef.FileSeqNotFoundErr
	}
	if fileSuffixName := fileName[suffixPos + 1:]; fileSuffixName != suffixName {
		return 0, errdef.FileSeqNotFoundErr
	}
	seqPos := strings.LastIndex(fileName[:suffixPos], ".")
	if suffixPos == -1 {
		return 0, errdef.FileSeqNotFoundErr
	}
	seqStr := fileName[seqPos + 1:suffixPos]
	seqForUnit64, err := strconv.ParseUint(seqStr, 10, 32)
	if err != nil {
		return 0, errdef.FileSeqNotFoundErr
	}
	return uint32(seqForUnit64), nil
}

func BuildMsgFileName(dir, prefixName, suffixName string, fileSeq uint32) string {
	fileSeqStr := strconv.FormatUint(uint64(fileSeq), 10)
	fileName := strings.TrimRight(dir, "/") + "/" + prefixName + "." + fileSeqStr + "." + suffixName
	return fileName
}

func BuildIndexFileName(topicName, dir, suffixName string, fileSeq uint32) string {
	return BuildMsgFileName(dir, topicName, suffixName, fileSeq)
}

func BuildDataFileName(topicName, dir, suffixName string, fileSeq uint32) string {
	return BuildMsgFileName(dir, topicName, suffixName, fileSeq)
}

func BuildOffsetFileName(topicName, consumerGroupName, dir, suffixName string, fileSeq uint32) string {
	return BuildMsgFileName(dir, BuildOffsetFilePrefixName(topicName, consumerGroupName), suffixName, fileSeq)
}

func BuildOffsetFilePrefixName(topicName, consumerGroupName string) string {
	return topicName + "." + consumerGroupName
}
