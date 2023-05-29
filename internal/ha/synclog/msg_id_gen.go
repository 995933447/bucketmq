package synclog

import (
	"encoding/binary"
	"io"
	"os"
)

const (
	msgIdFileSuffix = ".mid"
	msgIdGenBytes   = 12
)

type msgIdGen struct {
	baseDir     string
	fp          *os.File
	curMaxMsgId uint64
}

func newMsgIdGen(baseDir string) (*msgIdGen, error) {
	gen := &msgIdGen{
		baseDir: baseDir,
	}
	var (
		err         error
		isNewCreate bool
	)
	gen.fp, isNewCreate, err = makeMsgIdFp(baseDir)
	if err != nil {
		return nil, err
	}
	if !isNewCreate {
		if err = gen.load(); err != nil {
			return nil, err
		}
	}
	return gen, nil
}

func (r *msgIdGen) Incr(incr uint64) error {
	maxId := r.curMaxMsgId + incr
	var buf []byte
	binary.LittleEndian.PutUint16(buf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint64(buf[bufBoundaryBytes:bufBoundaryBytes+8], maxId)
	binary.LittleEndian.PutUint16(buf[bufBoundaryEnd:], bufBoundaryEnd)
	_, err := r.fp.Write(buf)
	if err != nil {
		return err
	}
	r.curMaxMsgId = maxId
	return nil
}

func (r *msgIdGen) load() error {
	buf, err := io.ReadAll(r.fp)
	if err != nil && err != io.EOF {
		return err
	}

	if len(buf) == 0 {

	}

	if len(buf) != msgIdGenBytes {
		return errFileCorrupted
	}

	binaryBegin := binary.LittleEndian.Uint16(buf[:bufBoundaryBytes])
	binaryEnd := binary.LittleEndian.Uint16(buf[bufBoundaryBytes+8:])

	if binaryBegin != bufBoundaryBegin || binaryEnd != bufBoundaryEnd {
		return errFileCorrupted
	}

	r.curMaxMsgId = binary.LittleEndian.Uint64(buf[bufBoundaryBytes:])

	return nil
}
