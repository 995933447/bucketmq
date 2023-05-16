package engine

import (
	"encoding/binary"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/golang/snappy"
	"os"
	"time"
)

// data file format is:
// item begin marker | data | item end marker
//
//	2                |   V  |       2
//
// index file format is
// item begin marker | created at | data offset | data len |  enabled compress |   priority  | bucketId | delay sec | retryCnt | msgId | item end marker
//
//	2                |      4     |   4         |      4   |        1          |       1	 |      4   |	   4     |  4      |   8   |   2

const (
	idxBytes = 38
)

const (
	idxFileSuffix  = ".idx"
	dataFileSuffix = ".dat"
)

func newOutput(writer *Writer, topic string, seq uint64) (*output, error) {
	out := &output{
		Writer: writer,
		seq:    seq,
	}
	var err error
	out.msgIdGen, err = newMsgIdGen(out.Writer.baseDir, out.Writer.topic)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type output struct {
	*Writer
	seq    uint64
	idxFp  *os.File
	dataFp *os.File
	*msgIdGen
	writtenIdxNum    uint32
	writtenDataBytes uint32
	openFileTime     time.Time
	idxBuf           []byte
	dataBuf          []byte
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

	if o.idxFp == nil || o.dataFp == nil {
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
	bin := binary.LittleEndian
	for {
		accumIdxNum := o.writtenIdxNum
		oldAccumIdxNum := o.writtenIdxNum
		accumDataBytes := o.writtenDataBytes
		var (
			msg               *Msg
			needSwitchNewFile bool
		)
		for _, msg = range msgList {
			var incrementBytes uint32
			if enabledCompress {
				msg.compressed = snappy.Encode(nil, msg.buf)
				incrementBytes = uint32(len(msg.compressed))
			} else {
				incrementBytes = uint32(len(msg.buf))
			}

			incrementBytes += bufBoundariesBytes

			accumIdxNum++
			accumDataBytes += incrementBytes
			if (o.idxFileMaxItemNum == 0 || accumIdxNum <= o.idxFileMaxItemNum) && (o.dataFileMaxBytes == 0 || accumDataBytes <= o.dataFileMaxBytes) {
				continue
			}

			needSwitchNewFile = true
			accumIdxNum--
			accumDataBytes -= incrementBytes
			break
		}

		batchWriteIdxNum := accumIdxNum - oldAccumIdxNum

		// file had reached max size
		if batchWriteIdxNum <= 0 {
			if err := o.openNewFile(); err != nil {
				return err
			}
			continue
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
				buf = msg.buf
			} else {
				buf = msg.compressed
			}

			bufBytes := len(buf)

			bin.PutUint16(dataBuf[:bufBoundaryBytes], bufBoundaryBegin)
			copy(dataBuf[bufBoundaryBytes:], buf)
			bin.PutUint16(dataBuf[bufBoundaryBytes+bufBytes:], bufBoundaryEnd)
			dataItemBufBytes := bufBytes + bufBoundariesBytes
			dataBuf = dataBuf[dataItemBufBytes:]

			bin.PutUint16(idxBuf[:bufBoundaryBytes], bufBoundaryBegin)
			idxBuf = idxBuf[bufBoundaryBytes:]
			bin.PutUint32(idxBuf[0:4], uint32(time.Now().Unix())) // created at
			bin.PutUint32(idxBuf[4:8], dataBufOffset)             // offset
			bin.PutUint32(idxBuf[8:12], uint32(dataItemBufBytes)) // size
			idxBuf[12] = compressedFlagInIdx
			idxBuf[13] = msg.priority
			bin.PutUint32(idxBuf[14:18], msg.bucketId)
			bin.PutUint32(idxBuf[18:22], msg.delaySec)
			bin.PutUint32(idxBuf[22:26], msg.retryCnt)
			bin.PutUint64(idxBuf[26:34], o.curMaxMsgId+uint64(i+1))
			bin.PutUint16(idxBuf[34:], bufBoundaryEnd)
			idxBuf = idxBuf[34+bufBoundaryBytes:]

			dataBufOffset += uint32(dataItemBufBytes)
		}

		n, err := o.dataFp.Write(o.dataBuf[:dataBufBytes])
		if err != nil {
			return err
		}
		for uint32(n) < dataBufBytes {
			more, err := o.dataFp.Write(o.dataBuf[n:dataBufBytes])
			if err != nil {
				return err
			}
			n += more
		}

		n, err = o.idxFp.Write(o.idxBuf[:idxBufBytes])
		if err != nil {
			return err
		}
		for uint32(n) < idxBufBytes {
			more, err := o.idxFp.Write(o.idxBuf[n:idxBufBytes])
			if err != nil {
				return err
			}
			n += more
		}

		if err := o.msgIdGen.Incr(uint64(batchWriteIdxNum)); err != nil {
			return err
		}

		for _, msg := range waitingResMsgList {
			if msg.WriteMsgResWait != nil {
				msg.WriteMsgResWait.IsFinished = true
				msg.WriteMsgResWait.Wg.Done()
			}
		}

		msgList = msgList[batchWriteIdxNum:]

		// all msg been written
		if !needSwitchNewFile || len(msgList) == 0 {
			o.writtenIdxNum = accumIdxNum
			o.writtenDataBytes = accumDataBytes
			break
		}

		if err = o.openNewFile(); err != nil {
			return err
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
