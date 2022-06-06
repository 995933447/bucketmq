package file

import (
	"github.com/995933447/bucketmq/core/utils/timeutil"
	errdef "github.com/995933447/bucketmqerrdef"
	"io/ioutil"
	"strconv"
	"strings"
	"time"
)

func trimPreSuffixToParseFileSeq(fileName, prefixName, suffixName string) (string, error) {
	if !strings.HasPrefix(fileName, prefixName+filePrefixNameSep) {
		return "", errdef.FileSeqNotFoundErr
	}
	suffixPos := strings.LastIndex(fileName, ".")
	if suffixPos == -1 {
		return "", errdef.FileSeqNotFoundErr
	}
	if fileSuffixName := fileName[suffixPos+1:]; fileSuffixName != suffixName {
		return "", errdef.FileSeqNotFoundErr
	}
	seq := fileName[len(prefixName)+1 : suffixPos]
	if !strings.Contains(seq, fileSeqMiddleSep) {
		return "", errdef.FileSeqNotFoundErr
	}
	return seq, nil
}

func buildMsgFileName(dir, prefixName, suffixName string, fileSeq string) string {
	fileName := strings.TrimRight(dir, "/") + "/" + prefixName + filePrefixNameSep + fileSeq + "." + suffixName
	return fileName
}

func buildIndexFileName(topicName, dir, suffixName string, fileSeq string) string {
	return buildMsgFileName(dir, topicName, suffixName, fileSeq)
}

func buildDataFileName(topicName, dir, suffixName string, fileSeq string) string {
	return buildMsgFileName(dir, topicName, suffixName, fileSeq)
}

func calMaxFileSeqFromDir(dir, prefixName, suffixName string) (string, error) {
	indexFiles, err := ioutil.ReadDir(dir)
	if err != nil {
		return "", err
	}

	var maxFileSeq string
	var found bool
	for _, file := range indexFiles {
		if file.IsDir() {
			continue
		}

		seq, err := trimPreSuffixToParseFileSeq(file.Name(), prefixName, suffixName)
		if err != nil {
			if err == errdef.FileSeqNotFoundErr {
				continue
			}
			return "", err
		}

		if maxFileSeq == "" || maxFileSeq < seq {
			maxFileSeq = seq
			found = true
		}
	}

	if found {
		return maxFileSeq, nil
	}

	return "", errdef.FileSeqNotFoundErr
}

func buildFileSeq(time time.Time, firstMsgOffset uint64) string {
	return timeutil.GetTimestampStr(time) + fileSeqMiddleSep + strconv.FormatUint(firstMsgOffset, 10)
}

func parseCreatedAtAndFirstMsgOffsetFromSeq(fileSeq string) (uint32, uint64, error) {
	seqInfo := strings.Split(fileSeq, fileSeqMiddleSep)
	if len(seqInfo) != 2 {
		return 0, 0, nil
	}

	createdAtForUint64, err := strconv.ParseUint(seqInfo[0], 10, 32)
	if err != nil {
		return 0, 0, err
	}

	firstMsgOffset, err := strconv.ParseUint(seqInfo[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}

	return uint32(createdAtForUint64), firstMsgOffset, nil
}
