package timeutil

import (
	"strconv"
	"time"
)

func GetTimestampStr(time time.Time) string {
	return strconv.FormatInt(time.Unix(), 10)
}
