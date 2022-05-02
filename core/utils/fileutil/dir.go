package fileutil

import (
	errdef "github.com/995933447/bucketmqerrdef"
	"os"
)

func MkdirIfNotExist(dirName string) error {
	dirInfo, err := os.Stat(dirName)
	if err != nil {
		return err
	}
	if os.IsNotExist(err) {
		if err = os.MkdirAll(dirName, os.FileMode(0755)); err != nil {
			return err
		}
	}
	if !dirInfo.IsDir() {
		return errdef.FileIsNotDirErr
	}
	return nil
}
