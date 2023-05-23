package engine

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func genIdxFileName(baseDir, topic string, seq uint64) string {
	return fmt.Sprintf("%s/%s_%d"+idxFileSuffix, getTopicFileDir(baseDir, topic), time.Now().Format("2006010215"), seq)
}

func genDataFileName(baseDir, topic string, seq uint64) string {
	return fmt.Sprintf("%s/%s_%d"+dataFileSuffix, getTopicFileDir(baseDir, topic), time.Now().Format("2006010215"), seq)
}

func genFinishRcFileName(baseDir, topic string) string {
	return fmt.Sprintf("%s/%s"+finishFileSuffix, getTopicFileDir(baseDir, topic), time.Now().Format("2006010215"))
}

func genMsgIdFileName(baseDir, topic string) string {
	return fmt.Sprintf("%s/%s"+msgIdFileSuffix, getTopicFileDir(baseDir, topic), time.Now().Format("2006010215"))
}

func genLoadBootFileName(baseDir, topic string) string {
	return fmt.Sprintf("%s/%s"+loadBootFileSuffix, getTopicFileDir(baseDir, topic), time.Now().Format("2006010215"))
}

func getTopicFileDir(baseDir, topic string) string {
	return fmt.Sprintf("%s/%s%s", baseDir, topicDirPrefix, topic)
}

func scanDirToParseTopics(dir string, checkTopicValid func(topic string) bool) ([]string, error) {
	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var topics []string
	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		fileName := file.Name()
		if !strings.HasPrefix(fileName, topicDirPrefix) {
			continue
		}

		topic := fileName[len(topicDirPrefix):]

		if !checkTopicValid(topic) {
			continue
		}

		topics = append(topics, topic)
	}

	return topics, nil
}

func mkdirIfNotExist(dir string) error {
	if _, err := os.Stat(dir); err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		if err = os.MkdirAll(dir, os.ModePerm); err != nil {
			return err
		}
	}
	return nil
}

func makeSeqIdxFp(baseDir, topic string, seq uint64, flag int) (*os.File, error) {
	dir := getTopicFileDir(baseDir, topic)
	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), idxFileSuffix) {
			continue
		}

		seqStr, ok := parseFileSeqStr(file)
		if !ok {
			continue
		}

		var curSeq uint64
		if seqStr != "" {
			var err error
			curSeq, err = strconv.ParseUint(seqStr, 10, 64)
			if err != nil {
				return nil, err
			}
		}

		if curSeq != seq {
			continue
		}

		return os.OpenFile(dir+"/"+file.Name(), flag, os.ModePerm)
	}

	return os.OpenFile(genIdxFileName(baseDir, topic, seq), flag, os.ModePerm)
}

func makeSeqDataFp(baseDir, topic string, seq uint64, flag int) (*os.File, error) {
	dir := getTopicFileDir(baseDir, topic)
	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), dataFileSuffix) {
			continue
		}

		seqStr, ok := parseFileSeqStr(file)
		if !ok {
			continue
		}

		if seqStr == "" {
			continue
		}

		curSeq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			return nil, err
		}

		if curSeq != seq {
			continue
		}

		return os.OpenFile(dir+"/"+file.Name(), flag, os.ModePerm)
	}

	return os.OpenFile(genDataFileName(baseDir, topic, seq), flag, os.ModePerm)
}

func makeLoadBootFp(baseDir, topic string) (*os.File, error) {
	dir := getTopicFileDir(baseDir, topic)

	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), loadBootFileSuffix) {
			continue
		}

		fp, err := os.OpenFile(dir+"/"+file.Name(), os.O_RDWR, os.ModePerm)
		if err != nil {
			return nil, err
		}

		return fp, nil
	}

	return os.OpenFile(genLoadBootFileName(baseDir, topic), os.O_CREATE|os.O_RDWR, os.ModePerm)
}

func makeMsgIdFp(baseDir, topic string) (*os.File, bool, error) {
	dir := getTopicFileDir(baseDir, topic)

	if err := mkdirIfNotExist(dir); err != nil {
		return nil, false, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, false, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), msgIdFileSuffix) {
			continue
		}

		fp, err := os.OpenFile(dir+"/"+file.Name(), os.O_RDWR, os.ModePerm)
		if err != nil {
			return nil, false, err
		}

		return fp, false, nil
	}

	fp, err := os.OpenFile(genMsgIdFileName(baseDir, topic), os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, false, err
	}

	return fp, true, nil
}

func makeFinishRcFp(baseDir, topic string) (*os.File, error) {
	dir := getTopicFileDir(baseDir, topic)

	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), finishFileSuffix) {
			continue
		}

		fp, err := os.OpenFile(dir+"/"+file.Name(), os.O_RDWR, os.ModePerm)
		if err != nil {
			return nil, err
		}

		return fp, nil
	}

	return os.OpenFile(genFinishRcFileName(baseDir, topic), os.O_CREATE|os.O_RDWR, os.ModePerm)
}

func scanDirToParseOldestSeq(baseDir, topic string) (uint64, error) {
	dir := getTopicFileDir(baseDir, topic)

	if err := mkdirIfNotExist(dir); err != nil {
		return 0, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}

	var oldestSeq uint64 = 1
	for _, file := range files {
		seqStr, ok := parseFileSeqStr(file)
		if !ok {
			continue
		}

		curSeq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			return 0, err
		}

		if curSeq < oldestSeq {
			oldestSeq = curSeq
		}
	}

	return oldestSeq, nil
}

func scanDirToParseNewestSeq(baseDir, topic string) (uint64, error) {
	dir := getTopicFileDir(baseDir, topic)

	if err := mkdirIfNotExist(dir); err != nil {
		return 0, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}

	var newestSeq uint64 = 1
	for _, file := range files {
		seqStr, ok := parseFileSeqStr(file)
		if !ok {
			continue
		}

		var curSeq uint64
		if seqStr != "" {
			curSeq, err = strconv.ParseUint(seqStr, 10, 64)
			if err != nil {
				return 0, err
			}
		}

		if curSeq > newestSeq {
			newestSeq = curSeq
		}
	}

	return newestSeq, nil
}

func scanDirToParseNextSeq(baseDir, topic string, seq uint64) (uint64, error) {
	dir := getTopicFileDir(baseDir, topic)

	if err := mkdirIfNotExist(dir); err != nil {
		return 0, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}

	var nextSeq uint64
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), idxFileSuffix) {
			continue
		}

		seqStr, ok := parseFileSeqStr(file)
		if !ok {
			continue
		}

		if seqStr == "" {
			continue
		}

		curSeq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			return 0, err
		}

		if curSeq <= seq {
			continue
		}

		if nextSeq == 0 {
			nextSeq = curSeq
			continue
		}

		if curSeq < nextSeq {
			nextSeq = curSeq
		}
	}

	if nextSeq == 0 {
		return 0, errSeqNotFound
	}

	return nextSeq, nil
}

func parseFileSeqStr(file os.DirEntry) (string, bool) {
	if file.IsDir() {
		return "", false
	}

	return parseFileSeqStrByFileName(file.Name())
}

func parseFileSeqStrByFileName(fileName string) (string, bool) {
	suffixPos := strings.IndexByte(fileName, '.')
	if suffixPos <= 0 {
		return "", false
	}

	fileNameWithoutSuffix := fileName[:suffixPos]
	fileNameChunk := strings.Split(fileNameWithoutSuffix, "_")
	if len(fileNameChunk) != 2 {
		return "", false
	}

	return fileNameChunk[1], true
}
