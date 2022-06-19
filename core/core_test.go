package core

import (
	"encoding/binary"
	"fmt"
	"github.com/go-basic/uuid"
	"testing"
)

func TestSetBuf(t *testing.T) {
	b := make([]byte, 1000)
	b[0] = byte(123)
	t.Logf("%v", b)
	l := binary.LittleEndian
	l.PutUint32(b[1:5], uint32(400))
	t.Logf("%v", b)
	fmt.Println(len(uuid.New()))
	copy(b[6:], "1")
}
