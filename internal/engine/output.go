package engine

import (
	"encoding/binary"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/golang/snappy"
	"io"
	"os"
	"time"
)

// data file format is:
// item begin marker | data | item end marker
//
//	2                |   V  |       2
//
// index file format is:
// item begin marker | created at | data offset | data len |  enabled compress |   priority  | bucketId | delay sec | retryCnt | msgId | item end marker
//
//	2                |      4     |   4         |      4   |        1          |       1	 |      4   |	   4     |  4      |   8   |   2
//
// undo index file format is:
// item begin marker | seq | idx num  | item end marker
//
//	2                |  8  |   4      |   2
//
// undo data file format is:
// item begin marker | seq | size  | item end marker
//
//	2                |  8  |   4   |   2
const (
	idxBytes      = 38
	idxUndoBytes  = 16
	dataUndoBytes = 16
)

const (
	IdxFileSuffix      = ".idx"
	DataFileSuffix     = ".dat"
	IdxUndoFileSuffix  = ".idx_undo"
	DataUndoFileSuffix = ".dat_undo"
)

func newOutput(writer *Writer, seq uint64) (*output, error) {
	out := &output{
		Writer: writer,
		seq:    seq,
	}

	var err error

	out.msgIdGen, err = newMsgIdGen(out.Writer.baseDir, out.Writer.topic)
	if err != nil {
		return nil, err
	}

	out.idxUndoFp, err = makeIdxUndoFp(out.Writer.baseDir, out.Writer.topic)
	if err != nil {
		return nil, err
	}

	idxUndoFileInfo, err := out.idxUndoFp.Stat()
	if err != nil {
		return nil, err
	}

	if idxUndoFileInfo.Size() > 0 {
		n, err := out.idxUndoFp.ReadAt(out.idxUndoBuf[:], 0)
		if err != nil && err != io.EOF {
			return nil, err
		}

		if n != idxUndoBytes {
			return nil, errFileCorrupted
		}

		if err = out.undoIdx(); err != nil {
			return nil, err
		}
	}

	out.dataUndoFp, err = makeDataUndoFp(out.Writer.baseDir, out.Writer.topic)
	if err != nil {
		return nil, err
	}

	dataUndoFileInfo, err := out.dataUndoFp.Stat()
	if err != nil {
		return nil, err
	}

	if dataUndoFileInfo.Size() > 0 {
		n, err := out.dataUndoFp.ReadAt(out.dataUndoBuf[:], 0)
		if err != nil && err != io.EOF {
			return nil, err
		}

		if n != dataUndoBytes {
			return nil, errFileCorrupted
		}

		if err = out.undoData(); err != nil {
			return nil, err
		}
	}

	return out, nil
}

type output struct {
	*Writer
	*msgIdGen
	seq              uint64
	idxFp            *os.File
	dataFp           *os.File
	idxUndoFp        *os.File
	dataUndoFp       *os.File
	writtenIdxNum    uint32
	writtenDataBytes uint32
	openFileTime     time.Time
	idxBuf           []byte
	dataBuf          []byte
	idxUndoBuf       [idxUndoBytes]byte
	dataUndoBuf      [dataUndoBytes]byte
}

func (o *output) isCorrupted() (bool, error) {
	if o.idxFp != nil {
		fileState, err := o.idxFp.Stat()
		if err != nil {
			return false, err
		}
		curBytes := uint32(fileState.Size())
		if curBytes%idxBytes != 0 {
			return true, nil
		}
		expectBytes := o.writtenIdxNum * idxBytes
		if expectBytes != curBytes {
			if curBytes < expectBytes {
				return true, nil
			} else {
				// reset index
				o.writtenIdxNum = curBytes / idxBytes
			}
		}
	}
	if o.dataFp != nil {
		fileState, err := o.dataFp.Stat()
		if err != nil {
			return false, err
		}
		curBytes := uint32(fileState.Size())
		if curBytes != o.writtenDataBytes {
			if curBytes < o.writtenDataBytes {
				return true, nil
			}
			// reset write bytes
			o.writtenDataBytes = curBytes
		}
	}
	return false, nil
}

func (o *output) syncDisk() {
	if o.idxFp != nil {
		if err := o.idxFp.Sync(); err != nil {
			util.Logger.Warn(nil, err)
		}
	}
	if o.dataFp != nil {
		if err := o.dataFp.Sync(); err != nil {
			util.Logger.Warn(nil, err)
		}
	}
}

func (o *output) write(msgList []*Msg) error {
	if len(msgList) == 0 {
		return nil
	}

	now := time.Now()

	if o.idxFp == nil || o.dataFp == nil || o.openFileTime.Format("2006010215") != now.Format("2006010215") {
		if err := o.openNewFile(); err != nil {
			return err
		}
	}

	var (
		compressedFlagInIdx byte
		enabledCompress     bool
	)
	if enabledCompress = o.enabledCompress.Load(); enabledCompress {
		compressedFlagInIdx = 1
	}
	for {
		accumIdxNum := o.writtenIdxNum
		accumDataBytes := o.writtenDataBytes
		var (
			msg               *Msg
			needSwitchNewFile bool
		)
		for _, msg = range msgList {
			var incrDataBytes uint32
			if enabledCompress {
				msg.compressed = snappy.Encode(nil, msg.Buf)
				incrDataBytes = uint32(len(msg.compressed))
			} else {
				incrDataBytes = uint32(len(msg.Buf))
			}

			incrDataBytes += bufBoundariesBytes
			accumDataBytes += incrDataBytes
			accumIdxNum++

			if (o.idxFileMaxItemNum == 0 || accumIdxNum <= o.idxFileMaxItemNum) && (o.dataFileMaxBytes == 0 || accumDataBytes <= o.dataFileMaxBytes) {
				continue
			}

			needSwitchNewFile = true
			accumIdxNum--
			accumDataBytes -= incrDataBytes
			break
		}

		batchWriteIdxNum := accumIdxNum - o.writtenIdxNum

		// file had reached max size
		if batchWriteIdxNum <= 0 {
			if err := o.openNewFile(); err != nil {
				return err
			}
			continue
		}

		origMaxMsgId := o.msgIdGen.curMaxMsgId
		if err := o.msgIdGen.Incr(uint64(batchWriteIdxNum)); err != nil {
			return err
		}

		idxBufBytes := batchWriteIdxNum * idxBytes
		if uint32(len(o.idxBuf)) < idxBufBytes {
			o.idxBuf = make([]byte, idxBufBytes)
		}

		dataBufBytes := accumDataBytes - o.writtenDataBytes
		if uint32(len(o.dataBuf)) < dataBufBytes {
			o.dataBuf = make([]byte, dataBufBytes)
		}

		dataBuf := o.dataBuf
		idxBuf := o.idxBuf
		dataBufOffset := o.writtenDataBytes
		var waitingResMsgList []*Msg
		for i := uint32(0); i < batchWriteIdxNum; i++ {
			msg := msgList[i]
			if msg.WriteMsgResWait != nil {
				waitingResMsgList = append(waitingResMsgList, msg)
			}

			var buf []byte
			if !enabledCompress {
				buf = msg.Buf
			} else {
				buf = msg.compressed
			}

			bufBytes := len(buf)

			binary.LittleEndian.PutUint16(dataBuf[:bufBoundaryBytes], bufBoundaryBegin)
			copy(dataBuf[bufBoundaryBytes:], buf)
			binary.LittleEndian.PutUint16(dataBuf[bufBoundaryBytes+bufBytes:], bufBoundaryEnd)
			dataItemBufBytes := bufBytes + bufBoundariesBytes
			dataBuf = dataBuf[dataItemBufBytes:]

			binary.LittleEndian.PutUint16(idxBuf[:bufBoundaryBytes], bufBoundaryBegin)
			idxBuf = idxBuf[bufBoundaryBytes:]
			binary.LittleEndian.PutUint32(idxBuf[0:4], uint32(now.Unix()))        // created at
			binary.LittleEndian.PutUint32(idxBuf[4:8], dataBufOffset)             // offset
			binary.LittleEndian.PutUint32(idxBuf[8:12], uint32(dataItemBufBytes)) // size
			idxBuf[12] = compressedFlagInIdx
			idxBuf[13] = msg.Priority
			binary.LittleEndian.PutUint32(idxBuf[14:18], msg.BucketId)
			binary.LittleEndian.PutUint32(idxBuf[18:22], msg.DelayMs)
			binary.LittleEndian.PutUint32(idxBuf[22:26], msg.RetryCnt)
			binary.LittleEndian.PutUint64(idxBuf[26:34], origMaxMsgId+uint64(i+1))
			binary.LittleEndian.PutUint16(idxBuf[34:], bufBoundaryEnd)
			idxBuf = idxBuf[34+bufBoundaryBytes:]

			dataBufOffset += uint32(dataItemBufBytes)
		}

		if err := o.logDataUndo(); err != nil {
			return err
		}

		_, err := o.dataFp.Write(o.dataBuf[:dataBufBytes])
		if err != nil {
			if err := o.clearDataUndo(); err != nil {
				panic(err)
			}
			return err
		}

		err = OnOutputFile(o.dataFp.Name(), o.dataBuf[:dataBufBytes], o.writtenDataBytes, &OutputExtra{
			Topic:            o.Writer.topic,
			ContentCreatedAt: uint32(now.Unix()),
		})
		if err != nil {
			// rollback
			if err := o.undoData(); err != nil {
				panic(err)
			}
			return err
		}

		if err = o.clearDataUndo(); err != nil {
			panic(err)
		}

		o.writtenDataBytes = dataBufBytes

		if err = o.logIdxUndo(); err != nil {
			return err
		}

		_, err = o.idxFp.Write(o.idxBuf[:idxBufBytes])
		if err != nil {
			if err := o.clearIdxUndo(); err != nil {
				panic(err)
			}
			return err
		}

		err = OnOutputFile(o.idxFp.Name(), o.idxBuf[:idxBufBytes], o.writtenIdxNum*idxBytes, &OutputExtra{
			Topic:            o.Writer.topic,
			ContentCreatedAt: uint32(now.Unix()),
		})
		if err != nil {
			// data file has callback success, only rollback index file
			if err := o.undoIdx(); err != nil {
				panic(err)
			}

			return err
		}

		if err = o.clearIdxUndo(); err != nil {
			panic(err)
		}

		o.writtenIdxNum += batchWriteIdxNum

		for _, afterWriteCh := range o.afterWriteChs {
			afterWriteCh <- o.seq
		}

		for _, msg := range waitingResMsgList {
			if msg.WriteMsgResWait != nil {
				msg.WriteMsgResWait.IsFinished = true
				msg.WriteMsgResWait.Wg.Done()
			}
		}

		msgList = msgList[batchWriteIdxNum:]

		// all msg been written
		if len(msgList) == 0 {
			break
		}

		if needSwitchNewFile {
			if err = o.openNewFile(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (o *output) close() {
	if o.idxFp != nil {
		_ = o.idxFp.Close()
	}
	if o.dataFp != nil {
		_ = o.dataFp.Close()
	}
	o.writtenDataBytes = 0
	o.writtenIdxNum = 0
	o.openFileTime = time.Time{}
}

func (o *output) logIdxUndo() error {
	binary.LittleEndian.PutUint16(o.idxUndoBuf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint64(o.idxUndoBuf[bufBoundaryBytes:], o.seq)
	binary.LittleEndian.PutUint32(o.idxUndoBuf[bufBoundaryBytes+8:], o.writtenIdxNum)
	binary.LittleEndian.PutUint16(o.idxUndoBuf[idxUndoBytes-bufBoundaryBytes:], bufBoundaryEnd)
	total := 0
	for {
		n, err := o.idxUndoFp.WriteAt(o.idxUndoBuf[total:], int64(total))
		if err != nil {
			if err := o.clearIdxUndo(); err != nil {
				panic(err)
			}
			return err
		}

		total += n

		if total >= idxUndoBytes {
			break
		}
	}
	return nil
}

func (o *output) undoIdx() error {
	origIdxFileBytes := binary.LittleEndian.Uint32(o.idxUndoBuf[bufBoundaryBytes+8:]) * idxBytes

	if err := o.idxFp.Truncate(int64(origIdxFileBytes)); err != nil {
		if err = os.Truncate(o.idxFp.Name(), int64(origIdxFileBytes)); err != nil {
			return err
		}
	}

	if err := o.clearIdxUndo(); err != nil {
		return err
	}

	return nil
}

func (o *output) clearIdxUndo() error {
	if err := o.idxUndoFp.Truncate(0); err != nil {
		if err = os.Truncate(o.idxUndoFp.Name(), 0); err != nil {
			return err
		}
	}
	return nil
}

func (o *output) undoData() error {
	origWrittenDataBytes := binary.LittleEndian.Uint32(o.dataUndoBuf[bufBoundaryBytes+8:])
	if err := o.dataFp.Truncate(int64(origWrittenDataBytes)); err != nil {
		if err = os.Truncate(o.dataFp.Name(), int64(origWrittenDataBytes)); err != nil {
			return err
		}
	}

	o.writtenDataBytes = origWrittenDataBytes

	if err := o.clearDataUndo(); err != nil {
		return err
	}

	return nil
}

func (o *output) logDataUndo() error {
	binary.LittleEndian.PutUint16(o.dataUndoBuf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint64(o.dataUndoBuf[bufBoundaryBytes:], o.seq)
	binary.LittleEndian.PutUint32(o.dataUndoBuf[bufBoundaryBytes+8:], o.writtenDataBytes)
	binary.LittleEndian.PutUint16(o.dataUndoBuf[idxUndoBytes-bufBoundaryBytes:], bufBoundaryEnd)
	var total int
	for {
		n, err := o.dataUndoFp.WriteAt(o.dataUndoBuf[total:], int64(total))
		if err != nil {
			if err := o.clearDataUndo(); err != nil {
				panic(err)
			}
			return err
		}

		total += n

		if total >= idxUndoBytes {
			break
		}
	}
	return nil
}

func (o *output) clearDataUndo() error {
	if err := o.dataUndoFp.Truncate(0); err != nil {
		if err = os.Truncate(o.dataUndoFp.Name(), 0); err != nil {
			return err
		}
	}
	return nil
}

func (o *output) openNewFile() error {
	curSeq := o.seq
	if o.idxFp != nil && o.dataFp != nil {
		curSeq = o.curMaxMsgId + 1
	}

	idxFp, err := makeSeqIdxFp(o.Writer.baseDir, o.Writer.topic, curSeq, os.O_CREATE|os.O_APPEND)
	if err != nil {
		return err
	}

	dataFp, err := makeSeqDataFp(o.Writer.baseDir, o.Writer.topic, curSeq, os.O_CREATE|os.O_APPEND)
	if err != nil {
		return err
	}

	o.close()

	o.seq = curSeq
	o.idxFp = idxFp
	o.dataFp = dataFp
	o.openFileTime = time.Now()

	idxFileState, err := o.idxFp.Stat()
	if err != nil {
		return err
	}

	dataFileState, err := o.dataFp.Stat()
	if err != nil {
		return err
	}

	if idxFileState.Size()%idxBytes > 0 {
		return errFileCorrupted
	}

	o.writtenIdxNum = uint32(idxFileState.Size() / int64(idxBytes))
	o.writtenDataBytes = uint32(dataFileState.Size())

	return nil
}
