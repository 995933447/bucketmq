package engine

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func genIdxUndoFileName(baseDir, topic string) string {
	return fmt.Sprintf("%s/%s"+IdxUndoFileSuffix, GetTopicFileDir(baseDir, topic), time.Now().Format("2006010215"))
}

func genIdxFileName(baseDir, topic string, seq uint64) string {
	return fmt.Sprintf("%s/%s_%d"+IdxFileSuffix, GetTopicFileDir(baseDir, topic), time.Now().Format("2006010215"), seq)
}

func genDataUndoFileName(baseDir, topic string) string {
	return fmt.Sprintf("%s/%s"+DataUndoFileSuffix, GetTopicFileDir(baseDir, topic), time.Now().Format("2006010215"))
}

func genDataFileName(baseDir, topic string, seq uint64) string {
	return fmt.Sprintf("%s/%s_%d"+DataFileSuffix, GetTopicFileDir(baseDir, topic), time.Now().Format("2006010215"), seq)
}

func genFinishRcUndoFileName(baseDir, topic, subscriber string, seq uint64) string {
	return fmt.Sprintf("%s/%s_%d"+FinishUndoFileSuffix, GetSubscriberFileDir(baseDir, topic, subscriber), time.Now().Format("2006010215"), seq)
}

func genFinishRcFileName(baseDir, topic, subscriber string, seq uint64) string {
	return fmt.Sprintf("%s/%s_%d"+FinishFileSuffix, GetSubscriberFileDir(baseDir, topic, subscriber), time.Now().Format("2006010215"), seq)
}

func genMsgIdUndoFileName(baseDir, topic string) string {
	return fmt.Sprintf("%s/%s"+MsgIdUndoFileSuffix, GetTopicFileDir(baseDir, topic), time.Now().Format("2006010215"))
}

func genMsgIdFileName(baseDir, topic string) string {
	return fmt.Sprintf("%s/%s"+MsgIdFileSuffix, GetTopicFileDir(baseDir, topic), time.Now().Format("2006010215"))
}

func genLoadBootUndoFileName(baseDir, topic, subscriber string) string {
	return fmt.Sprintf("%s/%s"+LoadBootUndoFileSuffix, GetSubscriberFileDir(baseDir, topic, subscriber), time.Now().Format("2006010215"))
}

func genLoadBootFileName(baseDir, topic, subscriber string) string {
	return fmt.Sprintf("%s/%s"+LoadBootFileSuffix, GetSubscriberFileDir(baseDir, topic, subscriber), time.Now().Format("2006010215"))
}

func GetTopicFileDir(baseDir, topic string) string {
	return fmt.Sprintf("%s/%s", baseDir, topic)
}

func GetSubscriberFileDir(baseDir, topic, subscriber string) string {
	return fmt.Sprintf("%s/%s/%s", baseDir, topic, subscriber)
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

func makeIdxUndoFp(baseDir, topic string) (*os.File, error) {
	dir := GetTopicFileDir(baseDir, topic)
	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), IdxFileSuffix) {
			continue
		}

		return os.OpenFile(dir+"/"+file.Name(), os.O_RDWR, os.ModePerm)
	}

	return os.OpenFile(genIdxUndoFileName(baseDir, topic), os.O_CREATE|os.O_RDWR, os.ModePerm)
}

func makeSeqIdxFp(baseDir, topic string, seq uint64, flag int) (*os.File, error) {
	dir := GetTopicFileDir(baseDir, topic)
	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), IdxFileSuffix) {
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
	dir := GetTopicFileDir(baseDir, topic)
	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), DataFileSuffix) {
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

func makeDataUndoFp(baseDir, topic string) (*os.File, error) {
	dir := GetTopicFileDir(baseDir, topic)
	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), DataUndoFileSuffix) {
			continue
		}

		return os.OpenFile(dir+"/"+file.Name(), os.O_RDWR, os.ModePerm)
	}

	return os.OpenFile(genDataUndoFileName(baseDir, topic), os.O_CREATE|os.O_RDWR, os.ModePerm)
}

func makeLoadBootFp(baseDir, topic, subscriber string) (*os.File, error) {
	dir := GetSubscriberFileDir(baseDir, topic, subscriber)

	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), LoadBootFileSuffix) {
			continue
		}

		fp, err := os.OpenFile(dir+"/"+file.Name(), os.O_RDWR, os.ModePerm)
		if err != nil {
			return nil, err
		}

		return fp, nil
	}

	return os.OpenFile(genLoadBootFileName(baseDir, topic, subscriber), os.O_CREATE|os.O_RDWR, os.ModePerm)
}

func makeLoadBootUndoFp(baseDir, topic, subscriber string) (*os.File, error) {
	dir := GetSubscriberFileDir(baseDir, topic, subscriber)

	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), LoadBootUndoFileSuffix) {
			continue
		}

		fp, err := os.OpenFile(dir+"/"+file.Name(), os.O_RDWR, os.ModePerm)
		if err != nil {
			return nil, err
		}

		return fp, nil
	}

	return os.OpenFile(genLoadBootUndoFileName(baseDir, topic, subscriber), os.O_CREATE|os.O_RDWR, os.ModePerm)
}

func makeMsgIdFp(baseDir, topic string) (*os.File, bool, error) {
	dir := GetTopicFileDir(baseDir, topic)

	if err := mkdirIfNotExist(dir); err != nil {
		return nil, false, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, false, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), MsgIdFileSuffix) {
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

func makeMsgIdUndoFp(baseDir, topic string) (*os.File, error) {
	dir := GetTopicFileDir(baseDir, topic)

	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), MsgIdUndoFileSuffix) {
			continue
		}

		fp, err := os.OpenFile(dir+"/"+file.Name(), os.O_RDWR, os.ModePerm)
		if err != nil {
			return nil, err
		}

		return fp, nil
	}

	fp, err := os.OpenFile(genMsgIdUndoFileName(baseDir, topic), os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return fp, nil
}

func makeFinishRcFp(baseDir, topic, subscriber string, seq uint64) (*os.File, error) {
	dir := GetSubscriberFileDir(baseDir, topic, subscriber)

	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), FinishFileSuffix) {
			continue
		}

		seqStr, ok := parseFileSeqStr(file)
		if !ok {
			continue
		}

		curSeq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			return nil, err
		}

		if curSeq != seq {
			continue
		}

		fp, err := os.OpenFile(dir+"/"+file.Name(), os.O_RDWR|os.O_APPEND, os.ModePerm)
		if err != nil {
			return nil, err
		}

		return fp, nil
	}

	return os.OpenFile(genFinishRcFileName(baseDir, topic, subscriber, seq), os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
}

func makeFinishRcUndoFp(baseDir, topic, subscriber string, seq uint64) (*os.File, error) {
	dir := GetSubscriberFileDir(baseDir, topic, subscriber)

	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), FinishUndoFileSuffix) {
			continue
		}

		seqStr, ok := parseFileSeqStr(file)
		if !ok {
			continue
		}

		curSeq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			return nil, err
		}

		if curSeq != seq {
			continue
		}

		fp, err := os.OpenFile(dir+"/"+file.Name(), os.O_RDWR|os.O_APPEND, os.ModePerm)
		if err != nil {
			return nil, err
		}

		return fp, nil
	}

	return os.OpenFile(genFinishRcUndoFileName(baseDir, topic, subscriber, seq), os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
}

func scanDirToParseOldestSeq(baseDir, topic string) (uint64, error) {
	dir := GetTopicFileDir(baseDir, topic)

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
	dir := GetTopicFileDir(baseDir, topic)

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
	dir := GetTopicFileDir(baseDir, topic)

	if err := mkdirIfNotExist(dir); err != nil {
		return 0, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}

	var nextSeq uint64
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), IdxFileSuffix) {
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
