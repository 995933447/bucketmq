package engine

import (
	"encoding/binary"
	"io"
	"os"
	"time"
)

const (
	MsgIdFileSuffix = ".mid"
	msgIdGenBytes   = 12
)

type msgIdGen struct {
	baseDir, topic string
	fp             *os.File
	curMaxMsgId    uint64
	buf            [msgIdGenBytes]byte
}

func newMsgIdGen(baseDir, topic string) (*msgIdGen, error) {
	gen := &msgIdGen{
		baseDir: baseDir,
		topic:   topic,
	}
	var (
		err         error
		isNewCreate bool
	)
	gen.fp, isNewCreate, err = makeMsgIdFp(baseDir, topic)
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
	now := time.Now().Unix()

	maxId := r.curMaxMsgId + incr
	binary.LittleEndian.PutUint16(r.buf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint64(r.buf[bufBoundaryBytes:bufBoundaryBytes+8], maxId)
	binary.LittleEndian.PutUint16(r.buf[bufBoundaryEnd:], bufBoundaryEnd)
	_, err := r.fp.Write(r.buf[:])
	if err != nil {
		return err
	}

	OnAnyFileWritten(r.fp.Name(), r.buf[:], &ExtraOfFileWritten{
		Topic:            r.topic,
		ContentCreatedAt: uint32(now),
	})

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
