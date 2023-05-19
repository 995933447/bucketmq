package snrpc

import (
	"encoding/binary"
	"errors"
	"github.com/995933447/bucketmq/pkg/api/errs"
	"github.com/golang/protobuf/proto"
)

// HeaderLen begin boundary(2) + data len(4) + proto id (4) + serial no(16) + is error(1) + end boundary(2)
const HeaderLen = 29
const HeaderBegin = 0x12
const HeaderEnd = 0x34

var ErrBufInvalid = errors.New("buf invalid")

type Msg struct {
	ProtoId uint32
	SN      string
	Data    []byte
	Err     *errs.RPCError
}

func Unpack(buf []byte) (completeMsgList []*Msg, incompleteBuf []byte, err error) {
	for {
		if len(buf) < HeaderLen {
			break
		}

		headerBegin := binary.LittleEndian.Uint16(buf[:2])
		headerEnd := binary.LittleEndian.Uint16(buf[HeaderEnd-2 : HeaderEnd])
		if headerBegin != HeaderBegin || headerEnd != HeaderEnd {
			err = ErrBufInvalid
			return
		}

		dataLen := binary.LittleEndian.Uint32(buf[2:6])
		if len(buf) < int(HeaderLen+dataLen) {
			break
		}

		var msg Msg
		msg.ProtoId = binary.LittleEndian.Uint32(buf[6:10])
		msg.SN = string(buf[10:26])
		msg.Data = buf[HeaderLen:dataLen]
		if buf[26] == 1 {
			var rpcErr errs.RPCError
			err = proto.Unmarshal(msg.Data, &rpcErr)
			if err != nil {
				return nil, nil, err
			}
			msg.Err = &rpcErr
		}
		completeMsgList = append(completeMsgList, &msg)

		buf = buf[HeaderLen+dataLen:]
	}

	incompleteBuf = buf

	return
}

func Pack(protoId uint32, SN string, data proto.Message) ([]byte, error) {
	buf, err := proto.Marshal(data)
	if err != nil {
		return nil, err
	}

	dataLen := HeaderLen + len(buf)
	buf = make([]byte, dataLen)
	header := make([]byte, HeaderLen)
	buf = append(header, buf...)
	binary.LittleEndian.PutUint16(buf[:2], HeaderBegin)
	binary.LittleEndian.PutUint32(buf[2:6], uint32(dataLen))
	binary.LittleEndian.PutUint32(buf[6:10], protoId)
	for i, b := range []byte(SN) {
		buf[10+i] = b
		if i >= 15 {
			break
		}
	}
	binary.LittleEndian.PutUint16(buf[26:28], HeaderEnd)

	return buf, nil
}
