package synclog

import (
	"encoding/binary"
	"fmt"
	"github.com/995933447/bucketmq/internal/util"
	"os"
)

//  no seq  | data time seq |     index num
//
//	  8     |   10          |      8

const (
	finishRcSuffix                    = ".fin"
	finishWaterMarkBytes              = 26
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
	idxNum      uint64
}

type ConsumeWaterMarkRec struct {
	baseDir string
	fp      *os.File
	idxFp   *os.File
	buf     [finishWaterMarkBytes]byte
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

	return idxFileState.Size()/idxBytes == int64(r.idxNum), nil
}

func (r *ConsumeWaterMarkRec) refreshAndGetOffset() (uint64, string, uint64, error) {
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
	r.idxNum = bin.Uint64(r.buf[nOSeqInFinishWaterMarkBytes+dateTimeSeqInFinishWaterMarkBytes:])

	return r.nOSeq, r.dateTimeSeq, r.idxNum, nil
}

func (r *ConsumeWaterMarkRec) getWaterMark() (uint64, string, uint64) {
	return r.nOSeq, r.dateTimeSeq, r.idxNum
}

func (r *ConsumeWaterMarkRec) syncDisk() {
	if err := r.fp.Sync(); err != nil {
		util.Logger.Warn(nil, err)
	}
}

func (r *ConsumeWaterMarkRec) updateWaterMark(nOSeq uint64, dateTimeSeq string, idxNum uint64) error {
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

		maxIdxNum := uint64(idxFileState.Size() / idxBytes)
		if maxIdxNum < idxNum {
			idxNum = maxIdxNum
		}
	}

	bin := binary.LittleEndian
	bin.PutUint64(r.buf[:nOSeqInFinishWaterMarkBytes], nOSeq)
	copy(r.buf[nOSeqInFinishWaterMarkBytes:nOSeqInFinishWaterMarkBytes+dateTimeSeqInFinishWaterMarkBytes], dateTimeSeq)
	bin.PutUint64(r.buf[nOSeqInFinishWaterMarkBytes+dateTimeSeqInFinishWaterMarkBytes:], idxNum)
	n, err := r.fp.WriteAt(r.buf[:], 0)
	if err != nil {
		return err
	}

	for {
		if n >= finishWaterMarkBytes {
			break
		}
		more, err := r.fp.Write(r.buf[:])
		if err != nil {
			return err
		}
		n += more
	}

	r.nOSeq, r.dateTimeSeq, r.idxNum = nOSeq, dateTimeSeq, idxNum

	return nil
}
