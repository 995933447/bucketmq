package core

import (
	"fmt"
	"github.com/995933447/bucketmq/core/msgstorage"
	"testing"
)

func TestPrintMsg (t *testing.T) {
	fmt.Printf("%+v", &msgstorage.Message{})
}
