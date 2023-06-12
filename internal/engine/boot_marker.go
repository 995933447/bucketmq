package engine

import (
	"encoding/binary"
	"github.com/995933447/bucketmq/internal/util"
	"io"
	"os"
	"time"
)

// file format is:
// item begin marker | boot id | boot seq | boot idx offset | item end marker
//
//	2                |   4     |  8        |        4       |   2
const loadBootBytes = 20

const (
	LoadBootFileSuffix     = ".boot"
	LoadBootUndoFileSuffix = ".boot"
)

func newBootMarker(readerGrp *readerGrp) (*bootMarker, error) {
	boot := &bootMarker{
		readerGrp: readerGrp,
	}

	var err error
	boot.fp, err = makeLoadBootFp(boot.readerGrp.Subscriber.baseDir, boot.readerGrp.Subscriber.topic, boot.readerGrp.Subscriber.name)
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	boot.undoFp, err = makeLoadBootUndoFp(boot.readerGrp.Subscriber.baseDir, boot.readerGrp.Subscriber.topic, boot.readerGrp.Subscriber.name)
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	if err = boot.load(); err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	return boot, nil
}

type bootMarker struct {
	*readerGrp
	fp            *os.File
	undoFp        *os.File
	bootId        uint32
	bootSeq       uint64
	bootIdxOffset uint32
	undoBuf       [loadBootBytes]byte
}

func (b *bootMarker) load() error {
	buf, err := io.ReadAll(b.fp)
	if err != nil && err != io.EOF {
		util.Logger.Error(nil, err)
		return err
	}

	if len(buf) == 0 {
		return nil
	}

	binaryBegin := binary.LittleEndian.Uint16(buf[:bufBoundaryBytes])
	binaryEnd := binary.LittleEndian.Uint16(buf[bufBoundaryBytes+16:])

	if binaryBegin != bufBoundaryBegin || binaryEnd != bufBoundaryEnd {
		return errFileCorrupted
	}

	b.bootId = binary.LittleEndian.Uint32(buf[bufBoundaryBytes : bufBoundaryBytes+4])
	b.bootSeq = binary.LittleEndian.Uint64(buf[bufBoundaryBytes+4 : bufBoundaryBytes+12])
	b.bootIdxOffset = binary.LittleEndian.Uint32(buf[bufBoundaryBytes+12 : bufBoundaryBytes+16])

	return nil
}

func (b *bootMarker) mark(bootId uint32, seq uint64, idxOffset uint32) error {
	now := time.Now().Unix()

	if err := b.logUndo(); err != nil {
		util.Logger.Error(nil, err)
		return err
	}

	buf := make([]byte, loadBootBytes)
	binary.LittleEndian.PutUint16(buf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint32(buf[bufBoundaryBytes:bufBoundaryBytes+4], bootId)
	binary.LittleEndian.PutUint64(buf[bufBoundaryBytes+4:bufBoundaryBytes+12], seq)
	binary.LittleEndian.PutUint32(buf[bufBoundaryBytes+12:bufBoundaryBytes+16], idxOffset)
	binary.LittleEndian.PutUint16(buf[bufBoundaryEnd+16:], bufBoundaryEnd)
	var total int
	for {
		n, err := b.fp.WriteAt(buf[total:], int64(total))
		if err != nil {
			if err := b.undo(); err != nil {
				util.Logger.Error(nil, err)
				panic(err)
			}
			return err
		}

		total += n

		if total >= loadBootBytes {
			break
		}
	}

	err := LogMsgFileOp(b.fp.Name(), buf, 0, &OutputExtra{
		Topic:            b.Subscriber.topic,
		Subscriber:       b.Subscriber.name,
		ContentCreatedAt: uint32(now),
	})
	if err != nil {
		if err := b.undo(); err != nil {
			util.Logger.Error(nil, err)
			panic(err)
		}
		return err
	}

	if err := b.clearUndo(); err != nil {
		util.Logger.Error(nil, err)
		panic(err)
	}

	b.bootId = bootId
	b.bootSeq = seq
	b.bootIdxOffset = idxOffset

	return nil
}

func (b *bootMarker) undo() error {
	b.bootId = binary.LittleEndian.Uint32(b.undoBuf[bufBoundaryBytes : bufBoundaryBytes+4])
	b.curSeq = binary.LittleEndian.Uint64(b.undoBuf[bufBoundaryBytes+4 : bufBoundaryBytes+12])
	b.bootIdxOffset = binary.LittleEndian.Uint32(b.undoBuf[bufBoundaryBytes+12 : bufBoundaryBytes+16])

	var total int
	for {
		n, err := b.fp.WriteAt(b.undoBuf[total:], int64(total))
		if err != nil {
			util.Logger.Error(nil, err)
			return err
		}

		total += n

		if total >= loadBootBytes {
			break
		}
	}

	if err := b.clearUndo(); err != nil {
		util.Logger.Error(nil, err)
		return err
	}

	return nil
}

func (b *bootMarker) logUndo() error {
	binary.LittleEndian.PutUint16(b.undoBuf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint32(b.undoBuf[bufBoundaryBytes:bufBoundaryBytes+4], b.bootId)
	binary.LittleEndian.PutUint64(b.undoBuf[bufBoundaryBytes+4:bufBoundaryBytes+12], b.curSeq)
	binary.LittleEndian.PutUint32(b.undoBuf[bufBoundaryBytes+12:bufBoundaryBytes+16], b.bootIdxOffset)
	binary.LittleEndian.PutUint16(b.undoBuf[bufBoundaryEnd+16:], bufBoundaryEnd)
	var total int
	for {
		n, err := b.undoFp.WriteAt(b.undoBuf[total:], int64(total))
		if err != nil {
			util.Logger.Error(nil, err)
			return err
		}

		total += n

		if total >= loadBootBytes {
			break
		}
	}
	return nil
}

func (b *bootMarker) clearUndo() error {
	if err := b.undoFp.Truncate(0); err != nil {
		util.Logger.Warn(nil, err)
		if err = os.Truncate(b.undoFp.Name(), 0); err != nil {
			util.Logger.Error(nil, err)
			return err
		}
	}
	return nil
}
