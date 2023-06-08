package synclog

import (
	"encoding/binary"
	"fmt"
	"github.com/995933447/bucketmq/internal/util"
	"os"
)

//  no seq  | data time seq |     index num
//
//	  8     |   10          |      4

const (
	finishRcSuffix                    = ".fin"
	finishWaterMarkBytes              = 22
	nOSeqInFinishWaterMarkBytes       = 8
	dateTimeSeqInFinishWaterMarkBytes = 10
	idxNumInFinishWaterMarkBytes      = 8
)

func newConsumeWaterMarkRec(baseDir string) (*ConsumeWaterMarkRec, error) {
	rec := &ConsumeWaterMarkRec{
		baseDir: baseDir,
	}

	dir := fmt.Sprintf(GetHADataDir(baseDir))
	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	var (
		err error
	)
	rec.fp, err = makeFinishRcFp(dir)
	if err != nil {
		return nil, err
	}

	fileState, err := rec.fp.Stat()
	if err != nil {
		return nil, err
	}

	if fileState.Size() <= 0 {
		nOSeq, dateTimeSeq, err := scanDirToParseOldestSeq(baseDir)
		if err != nil {
			return nil, err
		}

		rec.nOSeq = nOSeq
		rec.dateTimeSeq = dateTimeSeq
		if rec.nOSeq != 0 {
			err = rec.updateWaterMark(rec.nOSeq, dateTimeSeq, 0)
		}
	} else if _, _, _, err = rec.refreshAndGetOffset(); err != nil {
		return nil, err
	}

	if rec.nOSeq != 0 && rec.dateTimeSeq != "" {
		rec.idxFp, err = MakeSeqIdxFp(baseDir, rec.dateTimeSeq, rec.nOSeq, os.O_CREATE|os.O_RDONLY)
		if err != nil {
			return nil, err
		}
	}

	return rec, nil
}

type FinishWaterMark struct {
	nOSeq       uint64
	dateTimeSeq string
	idxNum      uint32
}

type ConsumeWaterMarkRec struct {
	baseDir string
	fp      *os.File
	undoFp  *os.File
	idxFp   *os.File
	buf     [finishWaterMarkBytes]byte
	undoBuf [finishWaterMarkBytes]byte
	FinishWaterMark
}

func (r *ConsumeWaterMarkRec) isOffsetsFinishedInSeq() (bool, error) {
	newestNOSeq, _, err := scanDirToParseNewestSeq(r.baseDir)
	if err != nil {
		return false, err
	}

	if newestNOSeq == r.nOSeq {
		return false, nil
	}

	idxFileState, err := r.idxFp.Stat()
	if err != nil {
		return false, err
	}

	return idxFileState.Size()/IdxBytes == int64(r.idxNum), nil
}

func (r *ConsumeWaterMarkRec) refreshAndGetOffset() (uint64, string, uint32, error) {
	n, err := r.fp.Read(r.buf[:])
	if err != nil {
		return 0, "", 0, err
	}

	for {
		if n >= finishWaterMarkBytes {
			break
		}

		more, err := r.fp.Read(r.buf[n:])
		if err != nil {
			return 0, "", 0, err
		}

		n += more
	}

	bin := binary.LittleEndian
	r.nOSeq = bin.Uint64(r.buf[:nOSeqInFinishWaterMarkBytes])
	r.dateTimeSeq = string(r.buf[nOSeqInFinishWaterMarkBytes : nOSeqInFinishWaterMarkBytes+dateTimeSeqInFinishWaterMarkBytes])
	r.idxNum = bin.Uint32(r.buf[nOSeqInFinishWaterMarkBytes+dateTimeSeqInFinishWaterMarkBytes:])

	return r.nOSeq, r.dateTimeSeq, r.idxNum, nil
}

func (r *ConsumeWaterMarkRec) getWaterMark() (uint64, string, uint32) {
	return r.nOSeq, r.dateTimeSeq, r.idxNum
}

func (r *ConsumeWaterMarkRec) syncDisk() {
	if err := r.fp.Sync(); err != nil {
		util.Logger.Warn(nil, err)
	}
}

func (r *ConsumeWaterMarkRec) updateWaterMark(nOSeq uint64, dateTimeSeq string, idxNum uint32) error {
	var err error
	if nOSeq != r.nOSeq {
		r.idxFp, err = MakeSeqIdxFp(r.baseDir, dateTimeSeq, nOSeq, os.O_RDONLY)
		if err != nil {
			return err
		}

		idxFileState, err := r.idxFp.Stat()
		if err != nil {
			return err
		}

		maxIdxNum := uint32(idxFileState.Size() / IdxBytes)
		if maxIdxNum < idxNum {
			idxNum = maxIdxNum
		}
	}

	if err := r.logUndo(); err != nil {
		return err
	}

	bin := binary.LittleEndian
	bin.PutUint64(r.buf[:nOSeqInFinishWaterMarkBytes], nOSeq)
	copy(r.buf[nOSeqInFinishWaterMarkBytes:nOSeqInFinishWaterMarkBytes+dateTimeSeqInFinishWaterMarkBytes], dateTimeSeq)
	bin.PutUint32(r.buf[nOSeqInFinishWaterMarkBytes+dateTimeSeqInFinishWaterMarkBytes:], idxNum)
	var total int
	for {
		n, err := r.fp.WriteAt(r.buf[:total], int64(total))
		if err != nil {
			if err := r.undo(); err != nil {
				panic(err)
			}
			return err
		}

		total += n

		if total >= finishWaterMarkBytes {
			break
		}
	}

	r.nOSeq, r.dateTimeSeq, r.idxNum = nOSeq, dateTimeSeq, idxNum

	return nil
}

func (r *ConsumeWaterMarkRec) undo() error {
	var total int
	for {
		n, err := r.fp.WriteAt(r.undoBuf[:total], int64(total))
		if err != nil {
			return err
		}

		total += n

		if total >= idxUndoBytes {
			break
		}
	}

	if err := r.clearUndo(); err != nil {
		return err
	}

	return nil
}

func (r *ConsumeWaterMarkRec) logUndo() error {
	binary.LittleEndian.PutUint16(r.undoBuf[:bufBoundaryBytes], bufBoundaryBegin)
	copy(r.undoBuf[bufBoundaryBytes:bufBoundaryBytes+dateTimeSeqInFinishWaterMarkBytes], r.dateTimeSeq)
	binary.LittleEndian.PutUint64(r.undoBuf[bufBoundaryBytes+dateTimeSeqInFinishWaterMarkBytes:], r.nOSeq)
	binary.LittleEndian.PutUint16(r.undoBuf[finishWaterMarkBytes-bufBoundaryBytes:], bufBoundaryEnd)
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

func (r *ConsumeWaterMarkRec) clearUndo() error {
	if err := r.undoFp.Truncate(0); err != nil {
		if err = os.Truncate(r.undoFp.Name(), 0); err != nil {
			return err
		}
		return err
	}
	return nil
}
