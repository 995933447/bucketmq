package snrpc

import (
	"encoding/binary"
	"errors"
	"github.com/995933447/bucketmq/pkg/rpc/errs"
	"github.com/golang/protobuf/proto"
)

// HeaderLen begin boundary(2) + data len(4) + proto id (4) + req serial no(36) + error flag(1) + request flag(1) + end boundary(2)
const HeaderLen = 50
const HeaderBegin = 0x12
const HeaderEnd = 0x34

var ErrBufInvalid = errors.New("buf invalid")

type Msg struct {
	ProtoId uint32
	SN      string
	Data    []byte
	Err     *errs.RPCError
	IsReq   bool
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
		msg.SN = string(buf[10:46])
		msg.Data = buf[HeaderLen:dataLen]
		if buf[46] == 1 {
			var rpcErr errs.RPCError
			err = proto.Unmarshal(msg.Data, &rpcErr)
			if err != nil {
				return nil, nil, err
			}
			msg.Err = &rpcErr
		}
		if buf[47] == 1 {
			msg.IsReq = true
		}
		completeMsgList = append(completeMsgList, &msg)

		buf = buf[HeaderLen+dataLen:]
	}

	incompleteBuf = buf

	return
}

func Pack(protoId uint32, SN string, isReq bool, data proto.Message) ([]byte, error) {
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
		if i >= 35 {
			break
		}
	}
	var isErrMsgFlag uint8
	if _, ok := data.(*errs.RPCError); ok {
		isErrMsgFlag = 1
	}
	buf[36] = isErrMsgFlag
	var isReqMsgFlag uint8
	if isReq {
		isReqMsgFlag = 1
	}
	buf[37] = isReqMsgFlag
	binary.LittleEndian.PutUint16(buf[38:50], HeaderEnd)

	return buf, nil
}
