package engine

import (
	"os"
	"sort"
	"strconv"
	"strings"
)

func newReaderGroup(subscriber *Subscriber, maxOpenFileNum uint32) (*readerGrp, error) {
	rg := &readerGrp{
		Subscriber:     subscriber,
		maxOpenFileNum: maxOpenFileNum,
	}

	if err := rg.load(); err != nil {
		return nil, err
	}

	return rg, nil
}

type readerGrp struct {
	*Subscriber
	maxOpenFileNum uint32
	curSeq         uint64
	maxSeq         uint64
	seqToReaderMap map[uint64]*reader
}

func (rg *readerGrp) close() {
	for _, reader := range rg.seqToReaderMap {
		reader.close()
	}
}

func (rg *readerGrp) load() error {
	dir := getTopicFileDir(rg.baseDir, rg.topic)

	if err := mkdirIfNotExist(dir); err != nil {
		return err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	var orderedSeqs []uint64
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

		orderedSeqs = append(orderedSeqs, seq)
	}

	sort.Slice(orderedSeqs, func(i, j int) bool {
		return orderedSeqs[i] < orderedSeqs[j]
	})

	rg.curSeq = orderedSeqs[0]
	rg.maxSeq = orderedSeqs[len(orderedSeqs)-1]

	lastSeqIdx := len(orderedSeqs)
	for i, seq := range orderedSeqs {
		reader, err := rg.loadReader(seq)
		if err != nil {
			return err
		}

		if i < lastSeqIdx {
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
		if uint32(len(rg.seqToReaderMap)) > rg.maxOpenFileNum {
			break
		}
	}

	return nil
}

func (rg *readerGrp) loadReader(seq uint64) (*reader, error) {
	var (
		read *reader
		ok   bool
		err  error
	)
	if read, ok = rg.seqToReaderMap[seq]; !ok {
		read, err = newReader(rg, seq)
		if err != nil {
			return read, err
		}
	}

	err = read.refreshMsgNum()
	if err != nil {
		return read, err
	}

	err = read.loadMsgIdxes()
	if err != nil {
		return read, err
	}

	return read, nil
}
