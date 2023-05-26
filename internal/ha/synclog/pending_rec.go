package synclog

import (
	"encoding/binary"
	"github.com/995933447/bucketmq/internal/util"
	"io"
	"os"
	"sync"
)

// item begin marker | seq | item end marker
//
//	2                |  8  |	   2
const (
	pendingIdxBufBytes = 8 + bufBoundariesBytes
)

const (
	pendingRcSuffix = ".pend"
	unPendRcSuffix  = ".unp"
)

type pendingMsgIdx struct {
	idxOffset uint64
}

type ConsumePendingRec struct {
	baseDir         string
	pendingMu       sync.RWMutex
	unPendMu        sync.RWMutex
	pendingMsgIdxes map[uint64]struct{}
	unPendMsgIdxes  map[uint64]struct{}
	pendingFp       *os.File
	unPendFp        *os.File
	pendingBuf      []byte
	unPendBuf       []byte
}

func newConsumePendingRec(baseDir string) (*ConsumePendingRec, error) {
	rec := &ConsumePendingRec{
		baseDir:         baseDir,
		pendingMsgIdxes: map[uint64]struct{}{},
		unPendMsgIdxes:  map[uint64]struct{}{},
	}

	var err error
	rec.pendingFp, rec.unPendFp, err = makePendingRcFps(baseDir)
	if err != nil {
		return nil, err

	}

	if err = rec.load(); err != nil {
		return nil, err
	}

	return rec, nil
}

func (r *ConsumePendingRec) syncDisk() {
	if err := r.pendingFp.Sync(); err != nil {
		util.Logger.Warn(nil, err)
	}

	if err := r.unPendFp.Sync(); err != nil {
		util.Logger.Warn(nil, err)
	}
}

func (r *ConsumePendingRec) isConfirmed(idxOffset uint64) bool {
	r.unPendMu.RLock()
	defer r.unPendMu.RUnlock()

	_, ok := r.unPendMsgIdxes[idxOffset]
	return ok
}

func (r *ConsumePendingRec) isPending(idxOffset uint64) bool {
	r.pendingMu.RLock()
	defer r.pendingMu.RUnlock()

	_, ok := r.pendingMsgIdxes[idxOffset]
	return ok
}

func (r *ConsumePendingRec) isEmpty() bool {
	r.pendingMu.RLock()
	defer r.pendingMu.RUnlock()

	return len(r.pendingMsgIdxes) == 0
}

func (r *ConsumePendingRec) load() error {
	pendings, err := r.loadPendings(false)
	if err != nil {
		return err
	}

	if err = r.pending(pendings, true); err != nil {
		return err
	}

	unPends, err := r.loadPendings(true)
	if err != nil {
		return err
	}

	if err = r.unPend(unPends, true); err != nil {
		return err
	}

	return nil
}

func (r *ConsumePendingRec) loadPendings(isLoadUnPend bool) ([]*pendingMsgIdx, error) {
	var cursor int64
	bin := binary.LittleEndian
	var (
		batchBufSize = pendingIdxBufBytes * 170
		pendings     []*pendingMsgIdx
		fp           *os.File
	)
	if isLoadUnPend {
		fp = r.unPendFp
	} else {
		fp = r.pendingFp
	}
	for {
		pendingBuf := make([]byte, batchBufSize)
		var isEOF bool
		n, err := fp.ReadAt(pendingBuf, cursor)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			isEOF = true
		}

		if n%pendingIdxBufBytes > 0 {
			return nil, errFileCorrupted
		}

		pendingBuf = pendingBuf[:n]
		if len(pendingBuf) == 0 {
			break
		}

		for {
			boundaryBegin := bin.Uint16(pendingBuf[:bufBoundaryBytes])
			boundaryEnd := bin.Uint16(pendingBuf[pendingIdxBufBytes-bufBoundaryBytes:])
			if boundaryBegin != bufBoundaryBegin || boundaryEnd != bufBoundaryEnd {
				return nil, errFileCorrupted
			}

			idxOffset := bin.Uint64(pendingBuf[bufBoundaryBytes : bufBoundaryBytes+8])
			pendings = append(pendings, &pendingMsgIdx{
				idxOffset: idxOffset,
			})
			pendingBuf = pendingBuf[pendingIdxBufBytes:]
			if len(pendingBuf) < pendingIdxBufBytes {
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

	return pendings, nil
}

func (r *ConsumePendingRec) pending(pendings []*pendingMsgIdx, onlyPendOnMem bool) error {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	r.unPendMu.RLock()
	var enqueued []*pendingMsgIdx
	for _, pending := range pendings {
		if _, ok := r.unPendMsgIdxes[pending.idxOffset]; ok {
			continue
		}

		_, ok := r.pendingMsgIdxes[pending.idxOffset]
		if ok {
			continue
		}
		r.pendingMsgIdxes[pending.idxOffset] = struct{}{}

		enqueued = append(enqueued, pending)
	}
	r.unPendMu.RUnlock()

	if !onlyPendOnMem {
		if err := r.write(false, enqueued); err != nil {
			return err
		}
	}

	for _, pending := range enqueued {
		r.pendingMsgIdxes[pending.idxOffset] = struct{}{}
	}

	return nil
}

func (r *ConsumePendingRec) unPend(pendings []*pendingMsgIdx, onlyUnPendMem bool) error {
	r.unPendMu.Lock()
	defer r.unPendMu.Unlock()

	var enqueued []*pendingMsgIdx
	for _, pending := range pendings {
		_, ok := r.unPendMsgIdxes[pending.idxOffset]
		if ok {
			continue
		}
		enqueued = append(enqueued, pending)
	}

	if !onlyUnPendMem {
		if err := r.write(true, enqueued); err != nil {
			return err
		}
	}

	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	for _, pending := range enqueued {
		r.unPendMsgIdxes[pending.idxOffset] = struct{}{}
		_, ok := r.pendingMsgIdxes[pending.idxOffset]
		if !ok {
			return nil
		}
		delete(r.pendingMsgIdxes, pending.idxOffset)
	}

	if len(r.pendingMsgIdxes) == 0 {
		var isTruncateFailed bool
		if err := r.pendingFp.Truncate(0); err == nil {
			if _, err = r.pendingFp.Seek(0, 0); err != nil {
				return err
			}
		} else {
			isTruncateFailed = true
		}

		if err := r.unPendFp.Truncate(0); err == nil {
			if _, err = r.unPendFp.Seek(0, 0); err != nil {
				return err
			}
		} else {
			isTruncateFailed = true
		}

		// compatible windows os
		if isTruncateFailed {
			err := r.pendingFp.Close()
			if err != nil {
				return err
			}

			err = r.unPendFp.Close()
			if err != nil {
				return err
			}

			_ = os.Truncate(r.pendingFp.Name(), 0)
			_ = os.Truncate(r.unPendFp.Name(), 0)

			r.pendingFp, r.unPendFp, err = makePendingRcFps(r.baseDir)
		}
	}

	return nil
}

func (r *ConsumePendingRec) write(isUnPend bool, pendings []*pendingMsgIdx) error {
	needBufLen := len(pendings) * pendingIdxBufBytes
	var bufLen int
	if !isUnPend {
		bufLen = len(r.pendingBuf)
	} else {
		bufLen = len(r.unPendBuf)
	}

	var totalBuf []byte
	needExpandBuf := bufLen < needBufLen
	if isUnPend {
		if needExpandBuf {
			r.unPendBuf = make([]byte, needBufLen)
		}
		totalBuf = r.unPendBuf
	} else {
		if needExpandBuf {
			r.pendingBuf = make([]byte, needBufLen)
		}
		totalBuf = r.pendingBuf
	}

	buf := totalBuf

	bin := binary.LittleEndian
	for _, pending := range pendings {
		bin.PutUint16(buf[:bufBoundaryBytes], bufBoundaryBegin)
		bin.PutUint64(buf[bufBoundaryBytes:], pending.idxOffset)
		bin.PutUint16(buf[pendingIdxBufBytes-bufBoundaryBytes:], bufBoundaryEnd)
		buf = buf[pendingIdxBufBytes:]
	}

	var (
		total int
		fp    *os.File
	)
	if isUnPend {
		fp = r.unPendFp
	} else {
		fp = r.pendingFp
	}
	for {
		n, err := fp.Write(totalBuf[total:needBufLen])
		if err != nil {
			return err
		}

		total += n

		if total >= needBufLen {
			break
		}
	}

	return nil
}
