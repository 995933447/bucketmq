package engine

import (
	"github.com/995933447/bucketmq/internal/util"
	"github.com/golang/snappy"
	"os"
	"sort"
	"strconv"
	"strings"
)

type LoadMsgMode int

const (
	loadModeNil LoadMsgMode = iota
	loadModeAll
	loadModeSeekMsgId
	loadModeNewest
)

func newReaderGrp(subscriber *Subscriber, loadMode LoadMsgMode, startMsgId uint64, maxOpenReaderNum uint32, bootId uint32) (*readerGrp, error) {
	grp := &readerGrp{
		Subscriber:       subscriber,
		loadMode:         loadMode,
		startMsgId:       startMsgId,
		maxOpenReaderNum: maxOpenReaderNum,
	}
	var err error
	grp.msgIdGen, err = newMsgIdGen(subscriber.baseDir, subscriber.topic)
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	grp.bootMarker, err = newBootMarker(grp)
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	if bootId != grp.bootMarker.bootId {
		grp.isFirstBoot = true
		grp.bootMarker.bootId = bootId
	}

	if err = grp.load(); err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	return grp, nil
}

type readerGrp struct {
	*Subscriber
	*msgIdGen
	*bootMarker
	loadMode         LoadMsgMode
	startMsgId       uint64
	bootId           uint32
	isFirstBoot      bool
	curSeq           uint64
	maxSeq           uint64
	maxOpenReaderNum uint32
	seqToReaderMap   map[uint64]*reader
}

func (rg *readerGrp) close() {
	for _, reader := range rg.seqToReaderMap {
		reader.close()
	}
}

func (rg *readerGrp) load() error {
	dir := GetTopicFileDir(rg.Subscriber.baseDir, rg.Subscriber.topic)

	if err := mkdirIfNotExist(dir); err != nil {
		util.Logger.Error(nil, err)
		return err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		util.Logger.Error(nil, err)
		return err
	}

	var (
		validSeqs []uint64
		startSeq  uint64
	)
	for _, file := range files {
		if !strings.Contains(file.Name(), IdxFileSuffix) {
			continue
		}

		seqStr, ok := parseFileSeqStr(file)
		if !ok {
			continue
		}

		if seqStr == "" {
			continue
		}

		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			util.Logger.Error(nil, err)
			return err
		}

		switch rg.loadMode {
		case loadModeAll:
			validSeqs = append(validSeqs, seq)
		case loadModeSeekMsgId:
			if seq > rg.startMsgId {
				validSeqs = append(validSeqs, seq)
				break
			}
			if startSeq == rg.startMsgId {
				break
			}
			if seq == startSeq {
				startSeq = seq
				break
			}
			if startSeq < seq {
				startSeq = seq
			}
		case loadModeNewest:
			if !rg.isFirstBoot {
				if seq >= rg.bootMarker.bootSeq {
					validSeqs = append(validSeqs, seq)
				}
				break
			}

			if startSeq > seq {
				break
			}
			startSeq = seq
			break
		}
	}

	if startSeq != 0 {
		validSeqs = append(validSeqs, startSeq)
	}

	if len(validSeqs) == 0 {
		return nil
	}

	sort.Slice(validSeqs, func(i, j int) bool {
		return validSeqs[i] < validSeqs[j]
	})

	rg.curSeq = validSeqs[0]
	rg.maxSeq = validSeqs[len(validSeqs)-1]

	for _, seq := range validSeqs {
		reader, err := rg.loadSpec(seq)
		if err != nil {
			return err
		}

		if seq < rg.maxSeq {
			ok, err := reader.isFinished()
			if err != nil {
				util.Logger.Error(nil, err)
				return err
			}

			if ok {
				reader.close()
				continue
			}
		}

		rg.seqToReaderMap[seq] = reader
		if uint32(len(rg.seqToReaderMap)) >= rg.maxOpenReaderNum {
			break
		}
	}

	return nil
}

func (rg *readerGrp) loadMsgData(msg *FileMsg) error {
	reader, err := rg.loadSpec(msg.seq)
	if err != nil {
		util.Logger.Error(nil, err)
		return err
	}

	msg.data, err = reader.loadMsgData(msg.dataOffset, msg.dataBytes)
	if err != nil {
		util.Logger.Error(nil, err)
		return err
	}

	if msg.enabledCompressed {
		msg.data, err = snappy.Decode(nil, msg.data)
		if err != nil {
			util.Logger.Error(nil, err)
			return err
		}
	}

	return nil
}

func (rg *readerGrp) loadSpec(seq uint64) (*reader, error) {
	var (
		read *reader
		ok   bool
		err  error
	)
	if read, ok = rg.seqToReaderMap[seq]; !ok {
		read, err = newReader(rg, seq)
		if err != nil {
			util.Logger.Error(nil, err)
			return nil, err
		}
	}

	err = read.refreshMsgNum()
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	err = read.finishRec.load()
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	err = read.loadMsgIdxes()
	if err != nil {
		util.Logger.Error(nil, err)
		return nil, err
	}

	return read, nil
}
