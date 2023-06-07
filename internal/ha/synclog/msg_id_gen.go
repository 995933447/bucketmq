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
	undoFp      *os.File
	curMaxMsgId uint64
	buf         [msgIdGenBytes]byte
	undoBuf     [msgIdGenBytes]byte
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

func (g *msgIdGen) reset(maxMsgId uint64) error {
	if err := g.logUndo(); err != nil {
		return err
	}

	g.curMaxMsgId = maxMsgId
	binary.LittleEndian.PutUint16(g.buf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint64(g.buf[bufBoundaryBytes:bufBoundaryBytes+8], g.curMaxMsgId)
	binary.LittleEndian.PutUint16(g.buf[bufBoundaryEnd:], bufBoundaryEnd)
	var total int
	for {
		n, err := g.fp.WriteAt(g.buf[total:], int64(total))
		if err != nil {
			if err := g.undo(); err != nil {
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

func (g *msgIdGen) resetWithoutUndo(maxMsgId uint64) error {
	g.curMaxMsgId = maxMsgId
	binary.LittleEndian.PutUint16(g.buf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint64(g.buf[bufBoundaryBytes:bufBoundaryBytes+8], g.curMaxMsgId)
	binary.LittleEndian.PutUint16(g.buf[bufBoundaryEnd:], bufBoundaryEnd)
	var total int
	for {
		n, err := g.fp.WriteAt(g.buf[total:], int64(total))
		if err != nil {
			return err
		}

		total += n

		if total >= msgIdGenBytes {
			break
		}
	}
	return nil
}

func (g *msgIdGen) incr(incr uint64) error {
	if err := g.logUndo(); err != nil {
		return err
	}

	g.curMaxMsgId = g.curMaxMsgId + incr
	binary.LittleEndian.PutUint16(g.buf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint64(g.buf[bufBoundaryBytes:bufBoundaryBytes+8], g.curMaxMsgId)
	binary.LittleEndian.PutUint16(g.buf[bufBoundaryEnd:], bufBoundaryEnd)
	var total int
	for {
		n, err := g.fp.WriteAt(g.buf[total:], int64(total))
		if err != nil {
			if err := g.undo(); err != nil {
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

func (g *msgIdGen) logUndo() error {
	binary.LittleEndian.PutUint16(g.undoBuf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint64(g.undoBuf[bufBoundaryBytes:bufBoundaryBytes+8], g.curMaxMsgId)
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

	return nil
}

func (g *msgIdGen) undo() error {
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

	g.curMaxMsgId = binary.LittleEndian.Uint64(g.undoBuf[bufBoundaryBytes : bufBoundaryBytes+8])

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
