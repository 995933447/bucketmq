package engine

import (
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
		return nil, err
	}

	grp.bootMarker, err = newBootMarker(grp)
	if err != nil {
		return nil, err
	}

	if bootId != grp.bootMarker.bootId {
		grp.isFirstBoot = true
		grp.bootMarker.bootId = bootId
	}

	if err = grp.load(); err != nil {
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
	dir := getTopicFileDir(rg.Subscriber.baseDir, rg.Subscriber.topic)

	if err := mkdirIfNotExist(dir); err != nil {
		return err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	var (
		validSeqs []uint64
		startSeq  uint64
	)
	for _, file := range files {
		if !strings.Contains(file.Name(), idxFileSuffix) {
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

func (rg *readerGrp) loadSpec(seq uint64) (*reader, error) {
	var (
		read *reader
		ok   bool
		err  error
	)
	if read, ok = rg.seqToReaderMap[seq]; !ok {
		read, err = newReader(rg, seq)
		if err != nil {
			return nil, err
		}
	}

	err = read.refreshMsgNum()
	if err != nil {
		return nil, err
	}

	err = read.finishRec.load()
	if err != nil {
		return nil, err
	}

	err = read.loadMsgIdxes()
	if err != nil {
		return nil, err
	}

	return read, nil
}