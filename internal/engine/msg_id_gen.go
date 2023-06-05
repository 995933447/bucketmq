package engine

import (
	"encoding/binary"
	"io"
	"os"
	"time"
)

const (
	MsgIdFileSuffix     = ".mid"
	MsgIdUndoFileSuffix = ".mid_wal"
)

const msgIdGenBytes = 12

type msgIdGen struct {
	baseDir, topic string
	fp             *os.File
	undoFp         *os.File
	curMaxMsgId    uint64
	buf            [msgIdGenBytes]byte
	undoBuf        [msgIdGenBytes]byte
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

	gen.undoFp, err = makeMsgIdUndoFp(baseDir, topic)
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

func (g *msgIdGen) Incr(incr uint64) error {
	now := time.Now().Unix()

	origMaxMsgId := g.curMaxMsgId

	binary.LittleEndian.PutUint16(g.undoBuf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint64(g.undoBuf[bufBoundaryBytes:bufBoundaryBytes+8], origMaxMsgId)
	binary.LittleEndian.PutUint16(g.undoBuf[bufBoundaryEnd:], bufBoundaryEnd)
	var total int
	for {
		n, err := g.undoFp.WriteAt(g.undoBuf[total:], int64(total))
		if err != nil {
			if err := g.clearUndo(); err != nil {
				panic(err)
			}
			return err
		}

		total += n

		if total >= msgIdGenBytes {
			break
		}
	}

	g.curMaxMsgId = g.curMaxMsgId + incr

	undo := func() error {
		var total int
		for {
			n, err := g.fp.WriteAt(g.undoBuf[total:], int64(total))
			if err != nil {
				return err
			}

			total += n

			if total >= msgIdGenBytes {
				break
			}
		}
		if err := g.clearUndo(); err != nil {
			return err
		}
		g.curMaxMsgId = origMaxMsgId
		return nil
	}

	binary.LittleEndian.PutUint16(g.buf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint64(g.buf[bufBoundaryBytes:bufBoundaryBytes+8], g.curMaxMsgId)
	binary.LittleEndian.PutUint16(g.buf[bufBoundaryEnd:], bufBoundaryEnd)
	total = 0
	for {
		n, err := g.fp.WriteAt(g.buf[total:], int64(total))
		if err != nil {
			if err := undo(); err != nil {
				return err
			}
			return err
		}

		total += n

		if total >= msgIdGenBytes {
			break
		}
	}

	err := OnOutputFile(g.fp.Name(), g.buf[:], 0, &OutputExtra{
		Topic:            g.topic,
		ContentCreatedAt: uint32(now),
	})
	if err != nil {
		if err := undo(); err != nil {
			return err
		}
		return err
	}

	if err = g.clearUndo(); err != nil {
		return err
	}

	return nil
}

func (g *msgIdGen) clearUndo() error {
	if err := g.undoFp.Truncate(0); err != nil {
		if err = os.Truncate(g.undoFp.Name(), 0); err != nil {
			return err
		}
	}
	return nil
}

func (g *msgIdGen) load() error {
	buf, err := io.ReadAll(g.fp)
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

	g.curMaxMsgId = binary.LittleEndian.Uint64(buf[bufBoundaryBytes:])

	return nil
}
