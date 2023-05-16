package engine

import (
	"encoding/binary"
	"io"
	"os"
)

// file format is:
// item begin marker | boot id | boot seq | boot idx offset | item end marker
//
//	2                |   4     |  8        |        4       |   2
//

const (
	loadBootFileSuffix = ".boot"
)

func newBootMarker(readerGrp *readerGrp) (*bootMarker, error) {
	boot := &bootMarker{
		readerGrp: readerGrp,
	}
	var err error
	boot.fp, err = makeLoadBootFp(boot.readerGrp.Subscriber.baseDir, boot.readerGrp.Subscriber.topic)
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
	var buf []byte
	binary.LittleEndian.PutUint16(buf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint32(buf[bufBoundaryBytes:bufBoundaryBytes+4], bootId)
	binary.LittleEndian.PutUint64(buf[bufBoundaryBytes+4:bufBoundaryBytes+12], seq)
	binary.LittleEndian.PutUint32(buf[bufBoundaryBytes+12:bufBoundaryBytes+16], idxOffset)
	binary.LittleEndian.PutUint16(buf[bufBoundaryEnd+16:], bufBoundaryEnd)
	_, err := b.fp.Write(buf)
	if err != nil {
		return err
	}

	b.bootId = bootId
	b.bootSeq = seq
	b.bootIdxOffset = idxOffset

	return nil
}
