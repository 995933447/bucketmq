package engine

import (
	"encoding/binary"
	"github.com/995933447/bucketmq/internal/util"
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
	retriedCnt        uint32
	maxRetryCnt       uint32
	delayMs           uint32
	bucketId          uint32
	seq               uint64
	offset            uint32
	msgId             uint64
	data              []byte
}

func (f *FileMsg) GetData() []byte {
	return f.data
}

func (f *FileMsg) GetRetriedCnt() uint32 {
	return f.retriedCnt
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
		util.Logger.Error(nil, err)
		return nil, err
	}

	r.idxFp, err = makeSeqIdxFp(r.readerGrp.Subscriber.baseDir, r.readerGrp.Subscriber.topic, seq, os.O_CREATE|os.O_RDONLY)
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	r.idxFp, err = makeSeqDataFp(r.readerGrp.Subscriber.baseDir, r.readerGrp.Subscriber.topic, seq, os.O_CREATE|os.O_RDONLY)
	if err != nil {
		util.Logger.Error(nil, err)
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
		util.Logger.Error(nil, err)
		return err
	}
	r.curSeqMsgNum = uint32(idxFileState.Size()) / idxBytes
	return nil
}

func (r *reader) loadMsgData(bufOffset, bufBytes uint32) ([]byte, error) {
	buf := make([]byte, bufBytes)

	n, err := r.dataFp.ReadAt(buf, int64(bufOffset))
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	if uint32(n) < bufBytes {
		util.Logger.Error(nil, errFileCorrupted)
		return nil, errFileCorrupted
	}

	boundaryBegin := binary.LittleEndian.Uint16(buf[:bufBoundaryBytes])
	boundaryEnd := binary.LittleEndian.Uint16(buf[bufBytes-bufBoundaryBytes:])

	if boundaryBegin != bufBoundaryBegin || boundaryEnd != bufBoundaryEnd {
		util.Logger.Error(nil, errFileCorrupted)
		return nil, errFileCorrupted
	}

	return buf[bufBoundaryBytes : bufBytes-bufBoundaryBytes], nil
}

func (r *reader) loadMsgIdxes() error {
	if r.curSeqMsgNum <= r.nextIdxCursor {
		return nil
	}

	var startMsgId uint64
	if r.loadMode != loadModeNewest {
		if r.loadMode == loadModeSeekMsgId {
			startMsgId = r.startMsgId
		}
	} else if !r.readerGrp.isFirstBoot {
		_, err := os.Stat(genIdxFileName(r.Subscriber.baseDir, r.Subscriber.topic, r.bootMarker.bootSeq))
		if err != nil {
			if !os.IsNotExist(err) {
				util.Logger.Error(nil, err)
				return err
			}

			startMsgId, err = scanDirToParseNewestSeq(r.Subscriber.baseDir, r.Subscriber.topic)
			if err != nil {
				util.Logger.Error(nil, err)
				return err
			}
		}

		if startMsgId == 0 {
			firstConsumeFp, err := makeSeqIdxFp(r.Subscriber.baseDir, r.Subscriber.topic, r.bootMarker.bootSeq, os.O_RDONLY)
			if err != nil {
				util.Logger.Error(nil, err)
				return err
			}

			idxBuf := make([]byte, idxBytes)
			n, err := firstConsumeFp.ReadAt(idxBuf, int64(r.bootMarker.bootIdxOffset*idxBytes))
			if err != nil {
				util.Logger.Error(nil, err)
				return err
			}

			if n < idxBytes {
				util.Logger.Error(nil, errFileCorrupted)
				return errFileCorrupted
			}

			startMsgId = binary.LittleEndian.Uint64(idxBuf[bufBoundaryBytes+26 : bufBoundaryBytes+34])
		}
	} else {
		fileState, err := r.idxFp.Stat()
		if err != nil {
			util.Logger.Error(nil, err)
			return err
		}

		fileSize := fileState.Size()

		if fileSize == 0 {
			return nil
		}

		if fileSize%idxBytes > 0 {
			util.Logger.Error(nil, errFileCorrupted)
			return errFileCorrupted
		}

		idxNum := fileSize / idxBytes
		r.nextIdxCursor = uint32(idxNum)
		startMsgId = r.msgIdGen.curMaxMsgId
	}

	bin := binary.LittleEndian
	for {
		if r.curSeqMsgNum <= r.nextIdxCursor {
			break
		}

		idxBuf := make([]byte, idxBytes*25)
		seekIdxBufOffset := r.nextIdxCursor * idxBytes
		var isEOF bool
		n, err := r.idxFp.ReadAt(idxBuf, int64(seekIdxBufOffset))
		if err != nil {
			if err != io.EOF {
				util.Logger.Error(nil, err)
				return err
			}
			isEOF = true
		}

		if n == 0 {
			break
		}

		if n%idxBytes > 0 {
			util.Logger.Error(nil, errFileCorrupted)
			return errFileCorrupted
		}

		readIdxNum := n / idxBytes

		idxBuf = idxBuf[:n]

		for i := 0; i < readIdxNum; i++ {
			idxBuf = idxBuf[i*idxBytes:]

			boundaryBegin := bin.Uint16(idxBuf[:bufBoundaryBytes])
			boundaryEnd := bin.Uint16(idxBuf[idxBytes-bufBoundaryBytes:])
			if boundaryBegin != bufBoundaryBegin || boundaryEnd != bufBoundaryEnd {
				util.Logger.Error(nil, errFileCorrupted)
				return errFileCorrupted
			}

			msgId := bin.Uint64(idxBuf[bufBoundaryBytes+26 : bufBoundaryBytes+34])
			if msgId >= startMsgId {
				createdAt := bin.Uint32(idxBuf[bufBoundaryBytes : bufBoundaryBytes+4])
				dataOffset := bin.Uint32(idxBuf[bufBoundaryBytes+4 : bufBoundaryBytes+8])
				dataBytes := bin.Uint32(idxBuf[bufBoundaryBytes+8 : bufBoundaryBytes+12])
				isCompressedFlag := idxBuf[bufBoundaryBytes+12]
				priority := idxBuf[bufBoundaryBytes+13]
				bucketId := bin.Uint32(idxBuf[bufBoundaryBytes+14 : bufBoundaryBytes+18])
				delayMs := bin.Uint32(idxBuf[bufBoundaryBytes+18 : bufBoundaryBytes+22])
				retryCnt := bin.Uint32(idxBuf[bufBoundaryBytes+22 : bufBoundaryBytes+26])

				item := &FileMsg{
					dataOffset:  dataOffset,
					dataBytes:   dataBytes,
					createdAt:   createdAt,
					seq:         r.seq,
					offset:      r.nextIdxCursor,
					priority:    priority,
					bucketId:    bucketId,
					delayMs:     delayMs,
					maxRetryCnt: retryCnt,
					msgId:       msgId,
				}
				if isCompressedFlag == 1 {
					item.enabledCompressed = true
				}

				if !r.isConfirmed(newConfirmedMsgIdx(item.offset)) {
					r.queue.push(item, true)
				}

				if r.readerGrp.isFirstBoot {
					if err = r.readerGrp.bootMarker.mark(r.readerGrp.bootId, item.seq, item.offset); err != nil {
						return err
					}
					r.readerGrp.isFirstBoot = false
				}
			}

			r.nextIdxCursor++
		}

		if isEOF {
			break
		}
	}

	return nil
}

func (r *reader) close() {
	_ = r.idxFp.Close()
	_ = r.dataFp.Close()
	r.finishRec.close()
	r.exitCh <- struct{}{}
}
