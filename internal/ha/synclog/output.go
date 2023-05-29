package synclog

import (
	"encoding/binary"
	"fmt"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/rpc/ha"
	"os"
	"strconv"
	"strings"
	"time"
)

// data file format is:
// item begin marker | data | item end marker
//
//	2                |   V  |       2
//
// index file format is
// item begin marker | created at | data offset | data len  | msgId | item end marker
//
//	2                |      4     |   4         |      4   |    8   |   2

const idxBytes = 14

const (
	IdxFileSuffix  = ".idx"
	DataFileSuffix = ".dat"
)

func newOutput(writer *Writer) (*output, error) {
	out := &output{
		Writer: writer,
	}
	var err error
	out.msgIdGen, err = newMsgIdGen(out.Writer.baseDir)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type output struct {
	*Writer
	nOSeq       uint64
	dateTimeSeq string
	idxFp       *os.File
	dataFp      *os.File
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

func (o *output) write(msgList []*ha.SyncMsgFileLogItem) error {
	if len(msgList) == 0 {
		return nil
	}

	bin := binary.LittleEndian
	for {
		accumIdxNum := o.writtenIdxNum
		oldAccumIdxNum := o.writtenIdxNum
		accumDataBytes := o.writtenDataBytes
		firstMsg := msgList[0]
		firstMsgHour := time.Unix(int64(firstMsg.CreatedAt), 0).Hour()
		firstMsgNOSeq := o.curMaxMsgId + 1
		for _, msg := range msgList {
			if time.Unix(int64(msgList[0].CreatedAt), 0).Hour() != firstMsgHour {
				break
			}
			incrementBytes := uint32(len(msg.FileBuf))
			incrementBytes += bufBoundariesBytes
			accumIdxNum++
			accumDataBytes += incrementBytes
		}

		batchWriteIdxNum := accumIdxNum - oldAccumIdxNum

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
		for i := uint32(0); i < batchWriteIdxNum; i++ {
			msg := msgList[i]
			bufBytes := len(msg.FileBuf)

			bin.PutUint16(dataBuf[:bufBoundaryBytes], bufBoundaryBegin)
			copy(dataBuf[bufBoundaryBytes:], msg.FileBuf)
			bin.PutUint16(dataBuf[bufBoundaryBytes+bufBytes:], bufBoundaryEnd)
			dataItemBufBytes := bufBytes + bufBoundariesBytes
			dataBuf = dataBuf[dataItemBufBytes:]

			bin.PutUint16(idxBuf[:bufBoundaryBytes], bufBoundaryBegin)
			idxBuf = idxBuf[bufBoundaryBytes:]
			bin.PutUint32(idxBuf[0:4], msg.CreatedAt)             // created at
			bin.PutUint32(idxBuf[4:8], dataBufOffset)             // offset
			bin.PutUint32(idxBuf[8:12], uint32(dataItemBufBytes)) // size
			bin.PutUint64(idxBuf[12:20], o.curMaxMsgId+uint64(i+1))
			bin.PutUint16(idxBuf[20:], bufBoundaryEnd)
			idxBuf = idxBuf[20+bufBoundaryBytes:]

			dataBufOffset += uint32(dataItemBufBytes)
		}

		dateTimeSeq := time.Unix(int64(firstMsg.CreatedAt), 0).Format("2006010215")
		if o.dateTimeSeq != dateTimeSeq {
			o.idxFp = nil
			o.dataFp = nil
			dir := GetHADataDir(o.Writer.baseDir)
			if err := mkdirIfNotExist(dir); err != nil {
				return err
			}

			files, err := os.ReadDir(dir)
			if err != nil {
				return err
			}

			for _, file := range files {
				if !strings.HasSuffix(file.Name(), IdxFileSuffix) {
					continue
				}

				if !strings.Contains(file.Name(), dateTimeSeq) {
					continue
				}

				nOSeqStr, ok := ParseFileNOSeqStr(file)
				if !ok {
					continue
				}

				var nOSeq uint64
				if nOSeqStr != "" {
					var err error
					nOSeq, err = strconv.ParseUint(nOSeqStr, 10, 64)
					if err != nil {
						return err
					}
				}

				o.dateTimeSeq = dateTimeSeq
				o.nOSeq = nOSeq
				o.idxFp, err = os.OpenFile(dir+"/"+file.Name(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
				if err != nil {
					return err
				}
				o.dataFp, err = os.OpenFile(dir+"/"+strings.ReplaceAll(file.Name(), IdxFileSuffix, DataFileSuffix), os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
				if err != nil {
					return err
				}
			}

			if o.idxFp == nil || o.dataFp == nil {
				o.dateTimeSeq = dateTimeSeq
				o.nOSeq = firstMsgNOSeq
				o.idxFp, err = os.OpenFile(dir+"/"+fmt.Sprintf("%s_%d.%s", o.dateTimeSeq, o.nOSeq, IdxFileSuffix), os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
				if err != nil {
					return err
				}
				o.idxFp, err = os.OpenFile(dir+"/"+fmt.Sprintf("%s_%d.%s", o.dateTimeSeq, o.nOSeq, DataFileSuffix), os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
				if err != nil {
					return err
				}
			}
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

		msgList = msgList[batchWriteIdxNum:]
		if len(msgList) == 0 {
			break
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
