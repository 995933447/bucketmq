package synclog

import (
	"encoding/binary"
	"fmt"
	"github.com/995933447/bucketmq/pkg/rpc/ha"
	"github.com/golang/protobuf/proto"
	"io"
	"os"
	"strings"
	"time"
)

func genIdxFileName(baseDir string) string {
	return fmt.Sprintf("%s/%s"+idxFileSuffix, getHADataDir(baseDir), time.Now().Format("2006010215"))
}

func genDataFileName(baseDir string) string {
	return fmt.Sprintf("%s/%s"+dataFileSuffix, getHADataDir(baseDir), time.Now().Format("2006010215"))
}

func genMsgIdFileName(baseDir string) string {
	return fmt.Sprintf("%s/%s"+msgIdFileSuffix, getHADataDir(baseDir), time.Now().Format("2006010215"))
}

func genFinishRcFileName(baseDir string) string {
	return fmt.Sprintf("%s/%s"+finishRcSuffix, getHADataDir(baseDir), time.Now().Format("2006010215"))
}

func genPendingRcFileName(baseDir string) string {
	return fmt.Sprintf("%s/%s"+pendingRcSuffix, getHADataDir(baseDir), time.Now().Format("2006010215"))
}

func genUnPendRcFileName(baseDir string) string {
	return fmt.Sprintf("%s/%s"+unPendRcSuffix, getHADataDir(baseDir), time.Now().Format("2006010215"))
}

func getHADataDir(baseDir string) string {
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

func makeIdxFp(baseDir string, flag int) (*os.File, error) {
	dir := getHADataDir(baseDir)
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

		if !strings.Contains(file.Name(), time.Now().Format("2006010215")) {
			continue
		}

		return os.OpenFile(dir+"/"+file.Name(), flag, os.ModePerm)
	}

	return os.OpenFile(genIdxFileName(baseDir), flag, os.ModePerm)
}

func makeDataFp(baseDir string, flag int) (*os.File, error) {
	dir := getHADataDir(baseDir)
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

		if !strings.Contains(file.Name(), time.Now().Format("2006010215")) {
			continue
		}

		return os.OpenFile(dir+"/"+file.Name(), flag, os.ModePerm)
	}

	return os.OpenFile(genDataFileName(baseDir), flag, os.ModePerm)
}

func makeMsgIdFp(baseDir string) (*os.File, bool, error) {
	dir := getHADataDir(baseDir)

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
	dir := getHADataDir(baseDir)

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
	dir := getHADataDir(baseDir)

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

func ReadLogItem(idxFp, dataFp *os.File, idxOffset uint64) (*ha.SyncMsgFileLogItem, bool, error) {
	idxBuf := make([]byte, idxBytes)
	seekIdxBufOffset := idxOffset * idxBytes
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
	boundaryEnd := binary.LittleEndian.Uint16(idxBuf[idxBytes-bufBoundaryBytes:])
	if boundaryBegin != bufBoundaryBegin || boundaryEnd != bufBoundaryEnd {
		return nil, false, errFileCorrupted
	}

	offset := binary.LittleEndian.Uint32(idxBuf[bufBoundaryBytes+4 : bufBoundaryBytes+8])
	dataBytes := binary.LittleEndian.Uint32(idxBuf[bufBoundaryBytes+8 : bufBoundaryBytes+12])

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

	return &item, hasMore, nil
}
