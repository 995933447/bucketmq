package engine

import (
	"encoding/binary"
	"github.com/995933447/bucketmq/internal/util"
	"io"
	"os"
	"time"
)

const (
	MsgIdFileSuffix     = ".mid"
	MsgIdUndoFileSuffix = ".mid_undo."
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
		util.Logger.Error(nil, err)
		return nil, err
	}

	gen.undoFp, err = makeMsgIdUndoFp(baseDir, topic)
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	undoFileInfo, err := gen.undoFp.Stat()
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	if undoFileInfo.Size() > 0 {
		n, err := gen.undoFp.ReadAt(gen.undoBuf[:], 0)
		if err != nil && err != io.EOF {
			util.Logger.Error(nil, err)
			return nil, err
		}

		if n != msgIdGenBytes {
			util.Logger.Error(nil, errFileCorrupted)
			return nil, errFileCorrupted
		}

		if err = gen.undo(); err != nil {
			util.Logger.Error(nil, err)
			return nil, err
		}
	}

	if !isNewCreate {
		if err = gen.load(); err != nil {
			util.Logger.Error(nil, err)
			return nil, err
		}
	}

	return gen, nil
}

func (g *msgIdGen) Incr(incr uint64) error {
	now := time.Now().Unix()

	if err := g.logUndo(); err != nil {
		util.Logger.Error(nil, err)
		return err
	}

	g.curMaxMsgId = g.curMaxMsgId + incr

	binary.LittleEndian.PutUint16(g.buf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint64(g.buf[bufBoundaryBytes:bufBoundaryBytes+8], g.curMaxMsgId)
	binary.LittleEndian.PutUint16(g.buf[bufBoundaryBytes:], bufBoundaryEnd)
	var total int
	for {
		n, err := g.fp.WriteAt(g.buf[total:], int64(total))
		if err != nil {
			if err := g.undo(); err != nil {
				util.Logger.Error(nil, err)
				panic(err)
			}
			return err
		}

		total += n

		if total >= msgIdGenBytes {
			break
		}
	}

	err := LogMsgFileOp(g.fp.Name(), g.buf[:], 0, &OutputExtra{
		Topic:            g.topic,
		ContentCreatedAt: uint32(now),
	})
	if err != nil {
		if err := g.undo(); err != nil {
			util.Logger.Error(nil, err)
			panic(err)
		}

		return err
	}

	if err = g.clearUndo(); err != nil {
		util.Logger.Error(nil, err)
		panic(err)
	}

	return nil
}

func (g *msgIdGen) logUndo() error {
	binary.LittleEndian.PutUint16(g.undoBuf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint64(g.undoBuf[bufBoundaryBytes:bufBoundaryBytes+8], g.curMaxMsgId)
	binary.LittleEndian.PutUint16(g.undoBuf[bufBoundaryBytes:], bufBoundaryEnd)
	var total int
	for {
		n, err := g.undoFp.WriteAt(g.undoBuf[total:], int64(total))
		if err != nil {
			if err := g.clearUndo(); err != nil {
				util.Logger.Error(nil, err)
				panic(err)
			}

			return err
		}

		total += n

		if total >= msgIdGenBytes {
			break
		}
	}

	return nil
}

func (g *msgIdGen) undo() error {
	var total int
	for {
		n, err := g.fp.WriteAt(g.undoBuf[total:], int64(total))
		if err != nil {
			util.Logger.Error(nil, err)
			return err
		}

		total += n

		if total >= msgIdGenBytes {
			break
		}
	}

	if err := g.clearUndo(); err != nil {
		util.Logger.Error(nil, err)
		return err
	}

	g.curMaxMsgId = binary.LittleEndian.Uint64(g.undoBuf[bufBoundaryBytes : bufBoundaryBytes+8])

	return nil
}

func (g *msgIdGen) clearUndo() error {
	if err := g.undoFp.Truncate(0); err != nil {
		if err = os.Truncate(g.undoFp.Name(), 0); err != nil {
			util.Logger.Error(nil, err)
			return err
		}
	}
	return nil
}

func (g *msgIdGen) load() error {
	buf, err := io.ReadAll(g.fp)
	if err != nil && err != io.EOF {
		util.Logger.Error(nil, err)
		return err
	}

	if len(buf) == 0 {

	}

	if len(buf) != msgIdGenBytes {
		util.Logger.Error(nil, errFileCorrupted)
		return errFileCorrupted
	}

	binaryBegin := binary.LittleEndian.Uint16(buf[:bufBoundaryBytes])
	binaryEnd := binary.LittleEndian.Uint16(buf[bufBoundaryBytes+8:])

	if binaryBegin != bufBoundaryBegin || binaryEnd != bufBoundaryEnd {
		util.Logger.Error(nil, errFileCorrupted)
		return errFileCorrupted
	}

	g.curMaxMsgId = binary.LittleEndian.Uint64(buf[bufBoundaryBytes:])

	return nil
}
