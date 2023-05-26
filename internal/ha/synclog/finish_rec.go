package synclog

import (
	"encoding/binary"
	"github.com/995933447/bucketmq/internal/util"
	"os"
)

const finishRcSuffix = ".fin"

const (
	finishWaterMarkBytes = 8
)

func newConsumeWaterMarkRec(baseDir string) (*ConsumeWaterMarkRec, error) {
	rec := &ConsumeWaterMarkRec{
		baseDir: baseDir,
	}

	var (
		err error
	)
	rec.fp, err = makeFinishRcFp(baseDir)
	if err != nil {
		return nil, err
	}

	fileState, err := rec.fp.Stat()
	if err != nil {
		return nil, err
	}

	if fileState.Size() <= 0 {
		err = rec.updateWaterMark(0)
		if err != nil {
			return nil, err
		}
	} else if _, err = rec.refreshAndGetOffset(); err != nil {
		return nil, err
	}

	rec.idxFp, err = makeIdxFp(baseDir, os.O_CREATE|os.O_RDONLY)
	if err != nil {
		return nil, err
	}

	return rec, nil
}

type FinishWaterMark struct {
	idxNum uint64
}

type ConsumeWaterMarkRec struct {
	baseDir string
	fp      *os.File
	idxFp   *os.File
	buf     [finishWaterMarkBytes]byte
	FinishWaterMark
}

func (r *ConsumeWaterMarkRec) isFinished() (bool, error) {
	idxFileState, err := r.idxFp.Stat()
	if err != nil {
		return false, err
	}

	return idxFileState.Size()/idxBytes == int64(r.idxNum), nil
}

func (r *ConsumeWaterMarkRec) refreshAndGetOffset() (uint64, error) {
	n, err := r.fp.Read(r.buf[:])
	if err != nil {
		return 0, err
	}

	for {
		if n >= finishWaterMarkBytes {
			break
		}

		more, err := r.fp.Read(r.buf[n:])
		if err != nil {
			return 0, err
		}

		n += more
	}

	bin := binary.LittleEndian
	r.idxNum = bin.Uint64(r.buf[:])

	return r.idxNum, nil
}

func (r *ConsumeWaterMarkRec) getWaterMark() uint64 {
	return r.idxNum
}

func (r *ConsumeWaterMarkRec) syncDisk() {
	if err := r.fp.Sync(); err != nil {
		util.Logger.Warn(nil, err)
	}
}

func (r *ConsumeWaterMarkRec) updateWaterMark(idxNum uint64) error {
	var err error

	bin := binary.LittleEndian
	bin.PutUint64(r.buf[:], idxNum)
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

	r.idxNum = idxNum

	return nil
}
