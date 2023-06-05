package engine

import (
	"encoding/binary"
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
		return nil, err
	}
	if err = boot.load(); err != nil {
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
}

func (b *bootMarker) load() error {
	buf, err := io.ReadAll(b.fp)
	if err != nil && err != io.EOF {
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

	undoBuf := make([]byte, loadBootBytes)
	n, err := b.fp.Read(undoBuf)
	if err != nil {
		if err != io.EOF {
			return err
		}
	}

	if n > 0 {
		var total int
		for {
			n, err = b.undoFp.WriteAt(undoBuf[total:], int64(total))
			if err != nil {
				return err
			}

			total += n

			if total >= loadBootBytes {
				break
			}
		}
	}

	undo := func() error {
		if n == 0 {
			if err := b.fp.Truncate(0); err != nil {
				if err = os.Truncate(b.fp.Name(), 0); err != nil {
					return err
				}
			}
			return nil
		}

		var total int
		for {
			n, err = b.fp.WriteAt(undoBuf[total:], int64(total))
			if err != nil {
				return err
			}

			total += n

			if total >= loadBootBytes {
				break
			}
		}

		if err := b.clearUndo(); err != nil {
			return err
		}

		return nil
	}

	buf := make([]byte, loadBootBytes)
	binary.LittleEndian.PutUint16(buf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint32(buf[bufBoundaryBytes:bufBoundaryBytes+4], bootId)
	binary.LittleEndian.PutUint64(buf[bufBoundaryBytes+4:bufBoundaryBytes+12], seq)
	binary.LittleEndian.PutUint32(buf[bufBoundaryBytes+12:bufBoundaryBytes+16], idxOffset)
	binary.LittleEndian.PutUint16(buf[bufBoundaryEnd+16:], bufBoundaryEnd)
	var total int
	for {
		n, err = b.fp.WriteAt(buf[total:], int64(total))
		if err != nil {
			if err := undo(); err != nil {
				return err
			}
			return err
		}

		total += n

		if total >= loadBootBytes {
			break
		}
	}

	err = OnOutputFile(b.fp.Name(), buf, 0, &OutputExtra{
		Topic:            b.Subscriber.topic,
		Subscriber:       b.Subscriber.name,
		ContentCreatedAt: uint32(now),
	})
	if err != nil {
		if err := undo(); err != nil {
			return err
		}
		return err
	}

	if err := b.clearUndo(); err != nil {
		return err
	}

	b.bootId = bootId
	b.bootSeq = seq
	b.bootIdxOffset = idxOffset

	return nil
}

func (b *bootMarker) clearUndo() error {
	if err := b.undoFp.Truncate(0); err != nil {
		if err = os.Truncate(b.undoFp.Name(), 0); err != nil {
			return err
		}
	}
	return nil
}
