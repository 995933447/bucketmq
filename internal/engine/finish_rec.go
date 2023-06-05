package engine

import (
	"encoding/binary"
	"io"
	"os"
	"time"
)

const (
	FinishFileSuffix     = ".fin"
	FinishUndoFileSuffix = ".fin_wal"
)

// finish record format is:
// item begin marker  | idx offset | item end marker
//
//	2                 |   4        | 	   2
// undo finish record format is:
// item begin marker  | confirmed num | item end marker
//
//	2                 |   4           | 	   2

const (
	idxOffsetInFinishIdxBufBytes = 4
	finishIdxBufBytes            = 4 + bufBoundariesBytes
	finishIdxUndoBufBytes        = 4 + bufBoundariesBytes
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

	rec.fp, err = makeFinishRcFp(r.reader.readerGrp.Subscriber.baseDir, r.reader.readerGrp.Subscriber.topic, r.reader.readerGrp.Subscriber.name, r.seq)
	if err != nil {
		return nil, err
	}

	rec.undoFp, err = makeFinishRcUndoFp(r.reader.readerGrp.Subscriber.baseDir, r.reader.readerGrp.Subscriber.topic, r.reader.readerGrp.Subscriber.name, r.seq)
	if err != nil {
		return nil, err
	}

	undoFileInfo, err := rec.undoFp.Stat()
	if err != nil {
		return nil, err
	}

	if undoFileInfo.Size() > 0 {
		n, err := rec.undoFp.ReadAt(rec.undoBuf[:], 0)
		if err != nil && err != io.EOF {
			return nil, err
		}

		if n != idxUndoBytes {
			return nil, errFileCorrupted
		}

		if err = rec.undo(); err != nil {
			return nil, err
		}
	}

	return rec, nil
}

type finishRec struct {
	*reader
	fp       *os.File
	idxFp    *os.File
	undoFp   *os.File
	buf      []byte
	undoBuf  [finishIdxUndoBufBytes]byte
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
	now := time.Now().Unix()

	if err := r.logUndo(); err != nil {
		return err
	}

	needBufLen := len(confirmedList) * finishIdxBufBytes
	needExpandBuf := len(r.buf) < needBufLen
	if needExpandBuf {
		r.buf = make([]byte, needBufLen)
	}

	buf := r.buf
	for _, confirmed := range confirmedList {
		binary.LittleEndian.PutUint16(buf[:bufBoundaryBytes], bufBoundaryBegin)
		binary.LittleEndian.PutUint32(buf[bufBoundaryBytes:bufBoundaryBytes+idxOffsetInFinishIdxBufBytes], confirmed.idxOffset)
		binary.LittleEndian.PutUint16(buf[finishIdxBufBytes-bufBoundaryBytes:], bufBoundaryEnd)
		buf = buf[finishIdxBufBytes:]
	}
	_, err := r.fp.Write(r.buf[:])
	if err != nil {
		if err := r.undo(); err != nil {
			panic(err)
		}
		return err
	}

	err = OnOutputFile(r.fp.Name(), buf, uint32(len(r.msgIdxes)*finishIdxBufBytes), &OutputExtra{
		Topic:            r.Subscriber.topic,
		Subscriber:       r.Subscriber.name,
		ContentCreatedAt: uint32(now),
	})
	if err != nil {
		if err := r.undo(); err != nil {
			panic(err)
		}
		return err
	}

	if err = r.clearUndo(); err != nil {
		panic(err)
	}

	for _, confirmed := range confirmedList {
		r.msgIdxes[confirmed.idxOffset] = struct{}{}
	}

	return nil
}

func (r *finishRec) undo() error {
	origFileBytes := int64(binary.LittleEndian.Uint32(r.undoBuf[bufBoundaryBytes:bufBoundaryBytes+idxOffsetInFinishIdxBufBytes]) * finishIdxBufBytes)
	if err := r.fp.Truncate(origFileBytes); err != nil {
		if err = os.Truncate(r.fp.Name(), origFileBytes); err != nil {
			return err
		}
		return err
	}

	if err := r.clearUndo(); err != nil {
		return err
	}

	return nil
}

func (r *finishRec) logUndo() error {
	binary.LittleEndian.PutUint16(r.undoBuf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint32(r.undoBuf[bufBoundaryBytes:bufBoundaryBytes+idxOffsetInFinishIdxBufBytes], uint32(len(r.msgIdxes)))
	binary.LittleEndian.PutUint16(r.undoBuf[finishIdxBufBytes-bufBoundaryBytes:], bufBoundaryEnd)
	var total int
	for {
		n, err := r.undoFp.WriteAt(r.undoBuf[:total], int64(total))
		if err != nil {
			return err
		}

		total += n

		if total >= idxUndoBytes {
			break
		}
	}

	return nil
}

func (r *finishRec) clearUndo() error {
	if err := r.undoFp.Truncate(0); err != nil {
		if err = os.Truncate(r.undoFp.Name(), 0); err != nil {
			return err
		}
		return err
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
