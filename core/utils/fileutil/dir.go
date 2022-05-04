package fileutil

import (
	errdef "github.com/995933447/bucketmqerrdef"
	"os"
)

func MkdirIfNotExist(dirName string) error {
	dirInfo, err := os.Stat(dirName)
	if err != nil  {
		if !os.IsNotExist(err) {
			return err
		}
		if err = os.MkdirAll(dirName, os.FileMode(0755)); err != nil {
			return err
		}
	} else if !dirInfo.IsDir() {
		return errdef.FileIsNotDirErr
	}
	return nil
}
