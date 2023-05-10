package engine

import (
	"encoding/binary"
	"github.com/995933447/bucketmq/util"
	"io"
	"os"
)

type FileMsg struct {
	dataOffset        uint32
	dataBytes         uint32
	enabledCompressed bool
	createdAt         uint32
	priority          uint8
	retryAt           uint32
	maxRetryCnt       uint32
	delaySec          uint32
	bucketId          uint32
	seq               uint64
	offset            uint32
	data              []byte
}

func newReader(rg *readerGrp, seq uint64) (*reader, error) {
	r := &reader{
		readerGrp:    rg,
		seq:          seq,
		confirmMsgCh: make(chan *confirmedMsgIdx),
	}

	var err error
	r.finishRec, err = newFinishRec(r)
	if err != nil {
		return nil, err
	}

	r.idxFp, err = makeSeqIdxFp(r.baseDir, r.topic, seq, os.O_CREATE|os.O_RDONLY)
	if err != nil {
		return nil, err
	}

	r.idxFp, err = makeSeqDataFp(r.baseDir, r.topic, seq, os.O_CREATE|os.O_RDONLY)
	if err != nil {
		return nil, err
	}

	go r.loop()

	return r, nil
}

type reader struct {
	*readerGrp
	*finishRec
	seq           uint64
	idxFp         *os.File
	dataFp        *os.File
	nextIdxCursor uint32
	curSeqMsgNum  uint32
	confirmMsgCh  chan *confirmedMsgIdx
	exitCh        chan struct{}
}

func (r *reader) loop() {
	for {
		select {
		case <-r.exitCh:
			goto out
		case confirmed := <-r.confirmMsgCh:
			var confirmedList []*confirmedMsgIdx
			confirmedList = append(confirmedList, confirmed)
			for {
				var more *confirmedMsgIdx
				select {
				case more = <-r.confirmMsgCh:
					confirmedList = append(confirmedList, more)
				default:
				}

				if more == nil {
					break
				}
			}
			if err := r.finishRec.confirm(confirmedList); err != nil {
				util.Logger.Error(nil, err)
			}
		}
	}
out:
	return
}

func (r *reader) refreshMsgNum() error {
	idxFileState, err := r.idxFp.Stat()
	if err != nil {
		return err
	}
	r.curSeqMsgNum = uint32(idxFileState.Size()) / idxBytes
	return nil
}

func (r *reader) loadMsgIdxes() error {
	if r.curSeqMsgNum <= r.nextIdxCursor {
		return nil
	}

	bin := binary.LittleEndian
	for {
		if r.curSeqMsgNum <= r.nextIdxCursor {
			break
		}

		idxBuf := make([]byte, idxBytes)
		seekIdxBufOffset := r.nextIdxCursor * idxBytes
		var isEOF bool
		n, err := r.idxFp.ReadAt(idxBuf, int64(seekIdxBufOffset))
		if err != nil {
			if err != io.EOF {
				return err
			}
			isEOF = true
		}

		if n == 0 {
			break
		}

		if n%idxBytes > 0 {
			return errFileCorrupted
		}

		idxBuf = idxBuf[:n]

		boundaryBegin := bin.Uint16(idxBuf[:bufBoundaryBytes])
		boundaryEnd := bin.Uint16(idxBuf[idxBytes-bufBoundaryBytes:])
		if boundaryBegin != bufBoundaryBegin || boundaryEnd != bufBoundaryEnd {
			return errFileCorrupted
		}

		createdAt := bin.Uint32(idxBuf[bufBoundaryBytes : bufBoundaryBytes+4])
		dataOffset := bin.Uint32(idxBuf[bufBoundaryBytes+4 : bufBoundaryBytes+8])
		dataBytes := bin.Uint32(idxBuf[bufBoundaryBytes+8 : bufBoundaryBytes+12])
		isCompressedFlag := idxBuf[bufBoundaryBytes+12]
		priority := idxBuf[bufBoundaryBytes+13]
		bucketId := bin.Uint32(idxBuf[bufBoundaryBytes+14 : bufBoundaryBytes+18])
		delaySec := bin.Uint32(idxBuf[bufBoundaryBytes+18 : bufBoundaryBytes+22])
		retryCnt := bin.Uint32(idxBuf[bufBoundaryBytes+22 : bufBoundaryBytes+26])

		item := &FileMsg{
			dataOffset:  dataOffset,
			dataBytes:   dataBytes,
			createdAt:   createdAt,
			seq:         r.seq,
			offset:      r.nextIdxCursor,
			priority:    priority,
			bucketId:    bucketId,
			delaySec:    delaySec,
			maxRetryCnt: retryCnt,
		}
		if isCompressedFlag == 1 {
			item.enabledCompressed = true
		}

		if !r.isConfirmed(newConfirmedMsgIdx(item.offset)) {
			r.queue.push(item, true)
		}

		r.nextIdxCursor++

		if isEOF {
			break
		}
	}

	return nil
}

func (r *reader) confirmMsg(idxOffset uint32) {
	r.confirmMsgCh <- newConfirmedMsgIdx(idxOffset)
}

func (r *reader) close() {
	_ = r.idxFp.Close()
	_ = r.dataFp.Close()
	r.finishRec.close()
	r.exitCh <- struct{}{}
}
