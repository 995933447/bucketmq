package synclog

import (
	"encoding/binary"
	nodegrpha "github.com/995933447/bucketmq/internal/ha"
	"github.com/995933447/bucketmq/internal/util"
	"os"
	"time"
)

// data file format is:
// item begin marker | data | item end marker
//
//	2                |   V  |       2
//
// index file format is
// item begin marker | created at | data offset | data len  | msgId | elect term |item end marker
//
//	2                |      4     |   4         |      4   |    8   |      8     | 2
//
// undo index file format is:
// item begin marker | idx num  | item end marker
//
//	2                |   4      |   2
//
// undo data file format is:
// item begin marker | size  | item end marker
//
//	2                |   4   |   2
const (
	IdxBytes      = 32
	idxUndoBytes  = 8
	dataUndoBytes = 8
)

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

	out.finishRc, err = newConsumeWaterMarkRec(out.Writer.baseDir)
	if err != nil {
		return nil, err
	}

	return out, nil
}

type output struct {
	*Writer
	*msgIdGen
	finishRc         *ConsumeWaterMarkRec
	nOSeq            uint64
	dateTimeSeq      string
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
		if curBytes%IdxBytes != 0 {
			return true, nil
		}
		expectBytes := o.writtenIdxNum * IdxBytes
		if expectBytes != curBytes {
			if curBytes < expectBytes {
				return true, nil
			} else {
				// reset index
				o.writtenIdxNum = curBytes / IdxBytes
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

	for {
		accumIdxNum := o.writtenIdxNum
		accumDataBytes := o.writtenDataBytes
		firstMsg := msgList[0]
		firstMsgHour := time.Unix(int64(firstMsg.logItem.CreatedAt), 0).Hour()

		var firstMsgNOSeq uint64
		if firstMsg.logItem.IsSyncFromMaster {
			firstMsgNOSeq = firstMsg.logItem.LogId
		} else {
			firstMsgNOSeq = o.curMaxMsgId + 1
		}

		for _, msg := range msgList {
			if time.Unix(int64(firstMsg.logItem.CreatedAt), 0).Hour() != firstMsgHour {
				break
			}
			incrDataBytes := uint32(len(msg.logItem.FileBuf))
			incrDataBytes += bufBoundariesBytes
			accumIdxNum++
			accumDataBytes += incrDataBytes
		}

		batchWriteIdxNum := accumIdxNum - o.writtenIdxNum

		idxBufBytes := batchWriteIdxNum * IdxBytes
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
			bufBytes := len(msg.logItem.FileBuf)

			binary.LittleEndian.PutUint16(dataBuf[:bufBoundaryBytes], bufBoundaryBegin)
			copy(dataBuf[bufBoundaryBytes:], msg.logItem.FileBuf)
			binary.LittleEndian.PutUint16(dataBuf[bufBoundaryBytes+bufBytes:], bufBoundaryEnd)
			dataItemBufBytes := bufBytes + bufBoundariesBytes
			dataBuf = dataBuf[dataItemBufBytes:]

			binary.LittleEndian.PutUint16(idxBuf[:bufBoundaryBytes], bufBoundaryBegin)
			idxBuf = idxBuf[bufBoundaryBytes:]
			binary.LittleEndian.PutUint32(idxBuf[0:4], msg.logItem.CreatedAt)     // created at
			binary.LittleEndian.PutUint32(idxBuf[4:8], dataBufOffset)             // offset
			binary.LittleEndian.PutUint32(idxBuf[8:12], uint32(dataItemBufBytes)) // size
			if !firstMsg.logItem.IsSyncFromMaster {
				binary.LittleEndian.PutUint64(idxBuf[12:20], o.msgIdGen.curMaxMsgId+uint64(i+1))
			} else {
				binary.LittleEndian.PutUint64(idxBuf[12:20], msg.logItem.LogId)
			}
			binary.LittleEndian.PutUint64(idxBuf[20:28], nodegrpha.GetCurElectTerm())
			binary.LittleEndian.PutUint16(idxBuf[28:], bufBoundaryEnd)
			idxBuf = idxBuf[28+bufBoundaryBytes:]

			dataBufOffset += uint32(dataItemBufBytes)
		}

		dateTimeSeq := time.Unix(int64(firstMsg.logItem.CreatedAt), 0).Format("2006010215")
		if o.dateTimeSeq != dateTimeSeq {
			var err error
			o.idxFp, o.dataFp, o.nOSeq, err = openOrCreateDateTimeFps(o.Writer.baseDir, dateTimeSeq, firstMsgNOSeq)
			if err != nil {
				return err
			}
			o.dateTimeSeq = dateTimeSeq
		}

		if err := o.logDataUndo(); err != nil {
			return err
		}

		if _, err := o.dataFp.Write(o.dataBuf[:dataBufBytes]); err != nil {
			return err
		}

		if err := o.logIdxUndo(); err != nil {
			return err
		}

		if _, err := o.idxFp.Write(o.idxBuf[:idxBufBytes]); err != nil {
			if err := o.undoData(); err != nil {
				panic(err)
			}

			return err
		}

		if !firstMsg.logItem.IsSyncFromMaster {
			if err := o.msgIdGen.logUndo(); err != nil {
				if err := o.undoData(); err != nil {
					panic(err)
				}

				if err := o.undoIdx(); err != nil {
					panic(err)
				}

				return err
			}

			if err := o.msgIdGen.incrWithoutUndoProtect(uint64(batchWriteIdxNum)); err != nil {
				if err := o.undoData(); err != nil {
					panic(err)
				}

				if err := o.undoIdx(); err != nil {
					panic(err)
				}

				if err := o.msgIdGen.undo(); err != nil {
					panic(err)
				}

				return err
			}

			if o.Writer.isRealTimeBackupMasterLogMeta {
				err := o.Writer.BackupMasterLogMeta()
				if err != nil {
					if err := o.undoData(); err != nil {
						panic(err)
					}

					if err := o.undoIdx(); err != nil {
						panic(err)
					}

					if err := o.msgIdGen.undo(); err != nil {
						panic(err)
					}

					return err
				}
			}

			if err := o.clearDataUndo(); err != nil {
				panic(err)
			}

			if err := o.clearIdxUndo(); err != nil {
				panic(err)
			}

			if err := o.msgIdGen.clearUndo(); err != nil {
				panic(err)
			}
		} else {
			if err := o.msgIdGen.logUndo(); err != nil {
				if err := o.undoData(); err != nil {
					panic(err)
				}

				if err := o.undoIdx(); err != nil {
					panic(err)
				}

				return err
			}

			if err := o.msgIdGen.resetWithoutUndoProtect(msgList[batchWriteIdxNum-1].logItem.LogId); err != nil {
				if err := o.undoData(); err != nil {
					panic(err)
				}

				if err := o.undoIdx(); err != nil {
					panic(err)
				}

				if err := o.msgIdGen.undo(); err != nil {
					panic(err)
				}

				return err
			}

			if err := o.finishRc.updateWaterMark(o.nOSeq, o.dateTimeSeq, accumIdxNum); err != nil {
				if err := o.undoData(); err != nil {
					panic(err)
				}

				if err := o.undoIdx(); err != nil {
					panic(err)
				}

				if err := o.msgIdGen.undo(); err != nil {
					panic(err)
				}

				return err
			}

			if err := o.clearDataUndo(); err != nil {
				panic(err)
			}

			if err := o.clearIdxUndo(); err != nil {
				panic(err)
			}

			if err := o.msgIdGen.clearUndo(); err != nil {
				panic(err)
			}
		}

		for _, msg := range msgList {
			msg.isFinished = true
			msg.resWait.Done()
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

func (o *output) logIdxUndo() error {
	binary.LittleEndian.PutUint16(o.idxUndoBuf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint32(o.idxUndoBuf[bufBoundaryBytes:], o.writtenIdxNum)
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
	origIdxFileBytes := binary.LittleEndian.Uint32(o.idxUndoBuf[bufBoundaryBytes:]) * IdxBytes

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

func (o *output) logDataUndo() error {
	binary.LittleEndian.PutUint16(o.dataUndoBuf[:bufBoundaryBytes], bufBoundaryBegin)
	binary.LittleEndian.PutUint32(o.dataUndoBuf[bufBoundaryBytes:], o.writtenDataBytes)
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

func (o *output) undoData() error {
	origWrittenDataBytes := binary.LittleEndian.Uint32(o.dataUndoBuf[bufBoundaryBytes:])
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

func (o *output) clearDataUndo() error {
	if err := o.dataUndoFp.Truncate(0); err != nil {
		if err = os.Truncate(o.dataUndoFp.Name(), 0); err != nil {
			return err
		}
	}
	return nil
}
