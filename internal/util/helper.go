package util

import (
	"strconv"
	"strings"
)

func ParseMemSizeStrToBytes(size string) (uint32, error) {
	size = strings.ToUpper(size)
	switch true {
	case strings.HasSuffix(size, "KB"), strings.HasSuffix(size, "K"):
		sizeVal, err := strconv.ParseUint(strings.TrimRight(strings.TrimRight(size, "K"), "KB"), 10, 32)
		if err != nil {
			return 0, err
		}
		return uint32(sizeVal) * 1024, nil
	case strings.HasSuffix(size, "M"), strings.HasSuffix(size, "MB"):
		sizeVal, err := strconv.ParseUint(strings.TrimRight(strings.TrimRight(size, "M"), "MB"), 10, 32)
		if err != nil {
			return 0, err
		}
		return uint32(sizeVal) * 1024 * 1024, nil
	case strings.HasSuffix(size, "G"), strings.HasSuffix(size, "GB"):
		sizeVal, err := strconv.ParseUint(strings.TrimRight(strings.TrimRight(size, "G"), "GB"), 10, 32)
		if err != nil {
			return 0, err
		}
		return uint32(sizeVal) * 1024 * 1024 * 1024, nil
	case strings.HasSuffix(size, "B"):
		sizeVal, err := strconv.ParseUint(strings.TrimRight(size, "B"), 10, 32)
		if err != nil {
			return 0, err
		}
		return uint32(sizeVal), nil
	}
	sizeVal, err := strconv.ParseUint(size, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(sizeVal), nil
}
