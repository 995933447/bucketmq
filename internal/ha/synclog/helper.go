package synclog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/995933447/bucketmq/pkg/rpc/ha"
	"github.com/golang/protobuf/proto"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

var errSeqNotFound = errors.New("sequence file not found")

func genIdxFileName(baseDir, dateTimeSeq string, nOSeq uint64) string {
	return fmt.Sprintf("%s/%s_%d"+IdxFileSuffix, GetHADataDir(baseDir), dateTimeSeq, nOSeq)
}

func genDataFileName(baseDir, dateTimeSeq string, nOSeq uint64) string {
	return fmt.Sprintf("%s/%s_%d"+DataFileSuffix, GetHADataDir(baseDir), dateTimeSeq, nOSeq)
}

func genMsgIdFileName(baseDir string) string {
	return fmt.Sprintf("%s/%s"+msgIdFileSuffix, GetHADataDir(baseDir), time.Now().Format("2006010215"))
}

func genFinishRcFileName(baseDir string) string {
	return fmt.Sprintf("%s/%s"+finishRcSuffix, GetHADataDir(baseDir), time.Now().Format("2006010215"))
}

func genPendingRcFileName(baseDir string) string {
	return fmt.Sprintf("%s/%s"+pendingRcSuffix, GetHADataDir(baseDir), time.Now().Format("2006010215"))
}

func genUnPendRcFileName(baseDir string) string {
	return fmt.Sprintf("%s/%s"+unPendRcSuffix, GetHADataDir(baseDir), time.Now().Format("2006010215"))
}

func GetHADataDir(baseDir string) string {
	return fmt.Sprintf("%s/%s", baseDir, "bucketmq.ha")
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

func ParseFileNOSeqStr(file os.DirEntry) (string, bool) {
	if file.IsDir() {
		return "", false
	}

	fileName := file.Name()

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

func MakeSeqIdxFp(baseDir, dateTimeSeq string, nOSeq uint64, flag int) (*os.File, error) {
	idxFp, err := os.OpenFile(genIdxFileName(baseDir, dateTimeSeq, nOSeq), flag, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return idxFp, nil
}

func MakeSeqDataFp(baseDir, dateTimeSeq string, nOSeq uint64, flag int) (*os.File, error) {
	idxFp, err := os.OpenFile(genDataFileName(baseDir, dateTimeSeq, nOSeq), flag, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return idxFp, nil
}

func makeMsgIdFp(baseDir string) (*os.File, bool, error) {
	dir := GetHADataDir(baseDir)

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

	fp, err := os.OpenFile(genMsgIdFileName(baseDir), os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, false, err
	}

	return fp, true, nil
}

func makeFinishRcFp(baseDir string) (*os.File, error) {
	dir := GetHADataDir(baseDir)

	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), finishRcSuffix) {
			continue
		}

		fp, err := os.OpenFile(dir+"/"+file.Name(), os.O_RDWR, os.ModePerm)
		if err != nil {
			return nil, err
		}

		return fp, nil
	}

	return os.OpenFile(genFinishRcFileName(baseDir), os.O_CREATE|os.O_RDWR, os.ModePerm)
}

func makePendingRcFps(baseDir string) (*os.File, *os.File, error) {
	dir := GetHADataDir(baseDir)

	if err := mkdirIfNotExist(dir); err != nil {
		return nil, nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}

	var pendingFp, unPendFp *os.File
	for _, file := range files {
		if pendingFp == nil && strings.HasSuffix(file.Name(), pendingRcSuffix) {
			pendingFp, err = os.OpenFile(dir+"/"+file.Name(), os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
			if err != nil {
				return nil, nil, err
			}
		}

		if unPendFp == nil && strings.HasSuffix(file.Name(), unPendRcSuffix) {
			unPendFp, err = os.OpenFile(dir+"/"+file.Name(), os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
			if err != nil {
				return nil, nil, err
			}
		}

		if pendingFp != nil && unPendFp != nil {
			return pendingFp, unPendFp, nil
		}
	}

	pendingFp, err = os.OpenFile(genPendingRcFileName(baseDir), os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, nil, err
	}

	unPendFp, err = os.OpenFile(genUnPendRcFileName(baseDir), os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, nil, err
	}

	return pendingFp, unPendFp, nil
}

func ReadLogItem(idxFp, dataFp *os.File, idxOffset uint32) (*ha.SyncMsgFileLogItem, bool, error) {
	idxBuf := make([]byte, IdxBytes)
	seekIdxBufOffset := idxOffset * IdxBytes
	hasMore := true
	_, err := idxFp.ReadAt(idxBuf, int64(seekIdxBufOffset))
	if err != nil {
		if err != io.EOF {
			return nil, false, err
		}
		hasMore = false
	}

	if len(idxBuf) == 0 {
		return nil, hasMore, nil
	}

	boundaryBegin := binary.LittleEndian.Uint16(idxBuf[:bufBoundaryBytes])
	boundaryEnd := binary.LittleEndian.Uint16(idxBuf[IdxBytes-bufBoundaryBytes:])
	if boundaryBegin != bufBoundaryBegin || boundaryEnd != bufBoundaryEnd {
		return nil, false, errFileCorrupted
	}

	offset := binary.LittleEndian.Uint32(idxBuf[bufBoundaryBytes+4 : bufBoundaryBytes+8])
	dataBytes := binary.LittleEndian.Uint32(idxBuf[bufBoundaryBytes+8 : bufBoundaryBytes+12])
	msgId := binary.LittleEndian.Uint64(idxBuf[bufBoundaryBytes+12 : bufBoundaryBytes+20])
	term := binary.LittleEndian.Uint64(idxBuf[bufBoundaryBytes+20 : bufBoundaryBytes+28])

	dataBuf := make([]byte, dataBytes)
	_, err = dataFp.ReadAt(dataBuf, int64(offset))
	if err != nil {
		if err != io.EOF {
			return nil, false, err
		}
		hasMore = false
	}

	if len(dataBuf) == 0 {
		return nil, hasMore, nil
	}

	boundaryBegin = binary.LittleEndian.Uint16(dataBuf[:bufBoundaryBytes])
	boundaryEnd = binary.LittleEndian.Uint16(dataBuf[dataBytes-bufBoundaryBytes:])
	if boundaryBegin != bufBoundaryBegin || boundaryEnd != bufBoundaryEnd {
		return nil, false, errFileCorrupted
	}

	data := dataBuf[bufBoundaryBytes : dataBytes-bufBoundaryBytes]

	var item ha.SyncMsgFileLogItem
	err = proto.Unmarshal(data, &item)
	if err != nil {
		return nil, false, err
	}

	item.LogId = msgId
	item.Term = term

	return &item, hasMore, nil
}

func scanDirToParseOldestSeq(baseDir string) (uint64, string, error) {
	dir := GetHADataDir(baseDir)

	if err := mkdirIfNotExist(dir); err != nil {
		return 0, "", err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, "", err
	}

	var (
		oldestNOSeq       uint64
		oldestDateTimeSeq string
	)
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), IdxFileSuffix) {
			continue
		}

		seqStr, ok := ParseFileNOSeqStr(file)
		if !ok {
			continue
		}

		curNOSeq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			return 0, "", err
		}

		if oldestNOSeq == 0 {
			oldestNOSeq = curNOSeq
			oldestDateTimeSeq = file.Name()[:10]
			continue
		}

		if curNOSeq < oldestNOSeq {
			oldestNOSeq = curNOSeq
			oldestDateTimeSeq = file.Name()[:10]
		}
	}

	if oldestNOSeq == 0 {
		return 0, "", nil
	}

	return oldestNOSeq, oldestDateTimeSeq, nil
}

func scanDirToParseNewestSeq(baseDir string) (uint64, string, error) {
	dir := GetHADataDir(baseDir)

	if err := mkdirIfNotExist(dir); err != nil {
		return 0, "", err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, "", err
	}

	var (
		newestNOSeq       uint64
		newestDateTimeSeq string
	)
	for _, file := range files {
		seqNOStr, ok := ParseFileNOSeqStr(file)
		if !ok {
			continue
		}

		var curNOSeq uint64
		if seqNOStr != "" {
			curNOSeq, err = strconv.ParseUint(seqNOStr, 10, 64)
			if err != nil {
				return 0, "", err
			}
		}

		if curNOSeq > newestNOSeq {
			newestNOSeq = curNOSeq
			newestDateTimeSeq = file.Name()[:10]
		}
	}

	return newestNOSeq, newestDateTimeSeq, nil
}

func scanDirToParseNextSeq(baseDir string, nOSeq uint64) (uint64, string, error) {
	dir := GetHADataDir(baseDir)

	if err := mkdirIfNotExist(dir); err != nil {
		return 0, "", err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, "", err
	}

	var (
		nextNOSeq       uint64
		nextDateTimeSeq string
	)
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), IdxFileSuffix) {
			continue
		}

		nOSeqStr, ok := ParseFileNOSeqStr(file)
		if !ok {
			continue
		}

		if nOSeqStr == "" {
			continue
		}

		curNOSeq, err := strconv.ParseUint(nOSeqStr, 10, 64)
		if err != nil {
			return 0, "", err
		}

		if curNOSeq <= nOSeq {
			continue
		}

		if nextNOSeq == 0 {
			nextNOSeq = curNOSeq
			nextDateTimeSeq = file.Name()[:10]
			continue
		}

		if curNOSeq < nextNOSeq {
			nextNOSeq = nOSeq
			nextDateTimeSeq = file.Name()[:10]
		}
	}

	if nextNOSeq == 0 {
		return 0, "", errSeqNotFound
	}

	return nextNOSeq, nextDateTimeSeq, nil
}

func openOrCreateDateTimeFps(baseDir, dateTimeSeq string, nOSeqIfCreate uint64) (*os.File, *os.File, uint64, error) {
	idxFp, dataFp, nOSeq, err := openDateTimeSeqFps(baseDir, dateTimeSeq)
	if err != nil && err != errSeqNotFound {
		return nil, nil, 0, err
	} else if err == nil {
		return idxFp, dataFp, nOSeq, nil
	}

	dir := GetHADataDir(baseDir)

	idxFp, err = os.OpenFile(dir+"/"+fmt.Sprintf("%s_%d.%s", dateTimeSeq, nOSeqIfCreate, IdxFileSuffix), os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		return nil, nil, 0, err
	}

	dataFp, err = os.OpenFile(dir+"/"+fmt.Sprintf("%s_%d.%s", dateTimeSeq, nOSeqIfCreate, DataFileSuffix), os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		return nil, nil, 0, err
	}

	return idxFp, dataFp, nOSeqIfCreate, nil
}

func openDateTimeSeqFps(baseDir, dateTimeSeq string) (*os.File, *os.File, uint64, error) {
	dir := GetHADataDir(baseDir)
	if err := mkdirIfNotExist(dir); err != nil {
		return nil, nil, 0, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, 0, err
	}

	var (
		idxFp  *os.File
		dataFp *os.File
		nOSeq  uint64
	)
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), IdxFileSuffix) {
			continue
		}

		if !strings.Contains(file.Name(), dateTimeSeq) {
			continue
		}

		nOSeqStr, ok := ParseFileNOSeqStr(file)
		if !ok {
			continue
		}

		if nOSeqStr != "" {
			var err error
			nOSeq, err = strconv.ParseUint(nOSeqStr, 10, 64)
			if err != nil {
				return nil, nil, 0, err
			}
		}

		idxFp, err = os.OpenFile(dir+"/"+file.Name(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
		if err != nil {
			return nil, nil, 0, err
		}

		dataFp, err = os.OpenFile(dir+"/"+strings.ReplaceAll(file.Name(), IdxFileSuffix, DataFileSuffix), os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
		if err != nil {
			return nil, nil, 0, err
		}
	}

	if idxFp == nil || dataFp == nil {
		return nil, nil, 0, errSeqNotFound
	}

	return idxFp, dataFp, nOSeq, nil
}
