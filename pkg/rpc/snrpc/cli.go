package snrpc

import (
	"fmt"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/rpc/errs"
	"github.com/golang/protobuf/proto"
	"io"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

func NewMsgHandler(protoId uint32, msg interface{}, handleFunc HandleMsgFunc) *MsgHandler {
	handler := &MsgHandler{
		handleFunc: handleFunc,
		protoId:    protoId,
	}
	handler.msgType = reflect.TypeOf(msg)
	return handler
}

type MsgHandler struct {
	protoId    uint32
	msgType    reflect.Type
	handleFunc HandleMsgFunc
}

type HandleMsgFunc func(req proto.Message) (proto.Message, error)

type callSrvResp struct {
	err  *errs.RPCError
	data []byte
}

type callSrvCtx struct {
	respCh chan *callSrvResp
}

type OnCliErr func(error)

type Cli struct {
	conn                net.Conn
	sNToCallSrvCtxMap   sync.Map
	protoIdToHandlerMap map[uint32]*MsgHandler
	closed              atomic.Bool
	cachedBuf           []byte
	onErr               OnCliErr
}

func NewCli(host string, port int, onCliErr OnCliErr) (*Cli, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}

	cli := &Cli{
		conn:                conn,
		protoIdToHandlerMap: map[uint32]*MsgHandler{},
		onErr:               onCliErr,
	}

	go cli.readAndProc()

	return cli, nil
}

func (c *Cli) Close() {
	c.closed.Store(true)
}

func (c *Cli) RegProto(protoId uint32, msg interface{}, msgFunc HandleMsgFunc) {
	c.protoIdToHandlerMap[protoId] = &MsgHandler{
		protoId:    protoId,
		msgType:    reflect.TypeOf(msg),
		handleFunc: msgFunc,
	}
}

func (c *Cli) readAndProc() {
	defer c.conn.Close()
	for {
		if c.closed.Load() {
			_ = c.conn.Close()
			break
		}

		msgList, err := c.read(c.conn)
		if err != nil {
			util.Logger.Error(nil, err)
			c.onErr(err)
			break
		}

		for _, msg := range msgList {
			go c.handleMsg(msg)
		}
	}
}

func (c *Cli) handleMsg(msg *Msg) {
	if !msg.IsReq {
		callSrvCtxt, ok := c.sNToCallSrvCtxMap.Load(msg.SN)
		if !ok {
			return
		}
		var callSrvRsp callSrvResp
		callSrvRsp.data = msg.Data
		if msg.Err != nil {
			callSrvRsp.err = msg.Err
		}
		callSrvCtxt.(*callSrvCtx).respCh <- &callSrvRsp
		return
	}

	handler, ok := c.protoIdToHandlerMap[msg.ProtoId]
	if !ok {
		util.Logger.Warnf(nil, "proto id %d not register handler", msg.ProtoId)
		return
	}

	req := reflect.New(handler.msgType).Interface().(proto.Message)
	err := proto.Unmarshal(msg.Data, req)
	if err != nil {
		util.Logger.Error(nil, err)
		return
	}

	resp, err := handler.handleFunc(req)
	if err != nil {
		if _, ok := err.(*errs.RPCError); !ok {
			err = errs.RPCErr(errs.ErrCode_ErrCodeInternal, err.Error())
		}

		resp = err.(*errs.RPCError)
	}

	buf, err := Pack(msg.ProtoId, msg.SN, false, resp)
	if err != nil {
		util.Logger.Error(nil, err)
		return
	}

	if err = c.write(buf); err != nil {
		return
	}
}

func (c *Cli) read(conn net.Conn) ([]*Msg, error) {
	buf, err := io.ReadAll(conn)
	if err != nil {
		return nil, err
	}

	if c.cachedBuf != nil {
		buf = append(c.cachedBuf, buf...)
	}

	msgList, buf, err := Unpack(buf)
	if err != nil {
		return nil, err
	}

	if len(buf) > 0 {
		c.cachedBuf = buf
	} else {
		c.cachedBuf = nil
	}

	return msgList, nil
}

func (c *Cli) write(buf []byte) error {
	bufLen := len(buf)
	var written int
	for {
		n, err := c.conn.Write(buf[written:])
		if err != nil {
			return err
		}
		written += n
		if written >= bufLen {
			break
		}
	}
	return nil
}

func (c *Cli) Call(protoId uint32, timeout time.Duration, req proto.Message, resp proto.Message) error {
	sN := GenSN()
	buf, err := Pack(protoId, sN, true, req)
	if err != nil {
		return err
	}

	respCh := make(chan *callSrvResp)
	c.sNToCallSrvCtxMap.Store(sN, &callSrvCtx{
		respCh: respCh,
	})

	if err = c.write(buf); err != nil {
		c.sNToCallSrvCtxMap.Delete(sN)
		return err
	}

	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()
	select {
	default:
		select {
		case <-timeoutTimer.C:
			return ErrCallbackReqTimeout
		case respWrap := <-respCh:
			if respWrap.err != nil {
				return err
			}
			if err = proto.Unmarshal(respWrap.data, resp); err != nil {
				return err
			}
		}
	}

	return nil
}
