package synclog

import (
	"fmt"
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
