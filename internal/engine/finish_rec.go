package engine

import (
	"encoding/binary"
	"io"
	"os"
)

const finishFileSuffix = ".fin"

// item begin marker  | idx offset | item end marker
//
//	2                 |   4        | 	   2
const (
	idxOffsetInFinishIdxBufBytes = 4
	finishIdxBufBytes            = 4 + bufBoundariesBytes
)

type confirmedMsgIdx struct {
	idxOffset uint32
}

func newConfirmedMsgIdx(idxOffset uint32) *confirmedMsgIdx {
	return &confirmedMsgIdx{
		idxOffset: idxOffset,
	}
}

func newFinishRec(r *reader) (*finishRec, error) {
	rec := &finishRec{
		reader:   r,
		msgIdxes: map[uint32]struct{}{},
	}

	var err error
	rec.idxFp, err = makeSeqIdxFp(r.reader.readerGrp.Subscriber.baseDir, r.reader.readerGrp.Subscriber.topic, r.seq, os.O_CREATE|os.O_RDONLY)
	if err != nil {
		return nil, err
	}

	rec.fp, err = makeFinishRcFp(r.reader.readerGrp.Subscriber.baseDir, r.reader.readerGrp.Subscriber.topic, r.reader.readerGrp.Subscriber.name)
	if err != nil {
		return nil, err
	}

	return rec, nil
}

type finishRec struct {
	*reader
	fp       *os.File
	idxFp    *os.File
	buf      []byte
	msgIdxes map[uint32]struct{}
}

func (r *finishRec) close() {
	_ = r.fp.Close()
	_ = r.idxFp.Close()
}

func (r *finishRec) load() error {
	var cursor int64
	bin := binary.LittleEndian
	var batchBufSize = finishIdxBufBytes * 170
	for {
		buf := make([]byte, batchBufSize)
		var isEOF bool
		n, err := r.fp.ReadAt(buf, cursor)
		if err != nil {
			if err != io.EOF {
				return err
			}
			isEOF = true
		}

		if n%finishIdxBufBytes > 0 {
			return errFileCorrupted
		}

		buf = buf[:n]
		if len(buf) == 0 {
			break
		}

		for {
			boundaryBegin := bin.Uint16(buf[:bufBoundaryBytes])
			boundaryEnd := bin.Uint16(buf[finishIdxBufBytes-bufBoundaryBytes:])
			if boundaryBegin != bufBoundaryBegin || boundaryEnd != bufBoundaryEnd {
				return errFileCorrupted
			}

			idxOffset := bin.Uint32(buf[bufBoundaryBytes : bufBoundaryBytes+idxOffsetInFinishIdxBufBytes])

			r.msgIdxes[idxOffset] = struct{}{}

			buf = buf[finishIdxBufBytes:]
			if len(buf) < finishIdxBufBytes {
				break
			}
		}

		if n < batchBufSize {
			break
		}

		if isEOF {
			break
		}

		cursor += int64(n)
	}
	return nil
}

func (r *finishRec) confirm(confirmedList []*confirmedMsgIdx) error {
	needBufLen := len(confirmedList) * finishIdxBufBytes
	var bufLen int

	var totalBuf []byte
	needExpandBuf := bufLen < needBufLen
	if needExpandBuf {
		r.buf = make([]byte, needBufLen)
	}

	buf := totalBuf

	bin := binary.LittleEndian
	for _, confirmed := range confirmedList {
		bin.PutUint16(buf[:bufBoundaryBytes], bufBoundaryBegin)
		bin.PutUint32(buf[bufBoundaryBytes:bufBoundaryBytes+idxOffsetInFinishIdxBufBytes], confirmed.idxOffset)
		bin.PutUint16(buf[finishIdxBufBytes-bufBoundaryBytes:], bufBoundaryEnd)
		buf = buf[finishIdxBufBytes:]
	}

	var total int
	for {
		n, err := r.fp.Write(totalBuf[total:needBufLen])
		if err != nil {
			return err
		}

		total += n

		if total >= needBufLen {
			break
		}
	}

	for _, confirmed := range confirmedList {
		r.msgIdxes[confirmed.idxOffset] = struct{}{}
	}

	return nil
}

func (r *finishRec) isFinished() (bool, error) {
	idxFpState, err := r.idxFp.Stat()
	if err != nil {
		return false, err
	}

	if idxFpState.Size()/int64(idxBytes) <= int64(len(r.msgIdxes)) {
		return true, nil
	}

	return false, nil
}

func (r *finishRec) isConfirmed(confirmed *confirmedMsgIdx) bool {
	_, ok := r.msgIdxes[confirmed.idxOffset]
	return ok
}
