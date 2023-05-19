package snrpc

import (
	"fmt"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/api/errs"
	"github.com/995933447/bucketmq/pkg/api/snrpc"
	"github.com/995933447/gonetutil"
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

type HandleMsgFunc func(conn net.Conn, sn string, msg proto.Message) (proto.Message, error)

type Server struct {
	host                string
	port                int
	sNCtx               sync.Map
	maxConnNum          uint32
	listener            net.Listener
	exitCh              chan struct{}
	exited              atomic.Bool
	conns               []net.Conn
	connCachedBuf       map[net.Conn][]byte
	protoIdToHandlerMap map[uint32]*MsgHandler
}

func (s *Server) RegHandler(protoId uint32, msg, msgFunc HandleMsgFunc) {
	s.protoIdToHandlerMap[protoId] = &MsgHandler{
		protoId:    protoId,
		msgType:    reflect.TypeOf(msg),
		handleFunc: msgFunc,
	}
}

func (s *Server) Serve() error {
	host, err := gonetutil.EvalVarToParseIp(s.host)
	if err != nil {
		return err
	}

	s.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", host, s.port))
	if err != nil {
		return err
	}

	var tempDelay time.Duration // how long to sleep on accept failure
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if ne, ok := err.(interface {
				Temporary() bool
			}); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				util.Logger.Errorf(nil, "Accept error: %v; retrying in %v", err, tempDelay)
				timer := time.NewTimer(tempDelay)
				select {
				case <-timer.C:
				case <-s.exitCh:
					timer.Stop()
					s.exited.Store(true)
					return nil
				}
				continue
			}

			util.Logger.Error(nil, err)
			continue
		}

		if s.maxConnNum < uint32(len(s.conns)) {
			continue
		}

		s.conns = append(s.conns, conn)

		go s.readAndHandle(conn)

		tempDelay = 0
	}
}

func (s *Server) readAndHandle(conn net.Conn) {
	for {
		msgList, err := s.read(conn)
		if err != nil {
			util.Logger.Error(nil, err)
			continue
		}

		for _, msg := range msgList {
			go s.handleMsg(conn, msg)
		}
	}
}

func (s *Server) handleMsg(conn net.Conn, msg *snrpc.Msg) {
	handler, ok := s.protoIdToHandlerMap[msg.ProtoId]
	if !ok {
		util.Logger.Warnf(nil, "proto id %d not register handler", msg.ProtoId)
		return
	}

	protoMsg := reflect.New(handler.msgType).Interface().(proto.Message)
	err := proto.Unmarshal(msg.Data, protoMsg)
	if err != nil {
		util.Logger.Error(nil, err)
		return
	}

	resp, err := handler.handleFunc(conn, msg.SN, protoMsg)
	if err != nil {
		if _, ok := err.(*errs.RPCError); !ok {
			err = errs.RPCErr(errs.ErrCode_ErrCodeInternal, err.Error())
		}

		resp = err.(*errs.RPCError)
	}

	buf, err := snrpc.Pack(msg.ProtoId, msg.SN, resp)
	if err != nil {
		util.Logger.Error(nil, err)
		return
	}

	if err = s.write(conn, buf); err != nil {
		return
	}
}

func (s *Server) read(conn net.Conn) ([]*snrpc.Msg, error) {
	buf, err := io.ReadAll(conn)
	if err != nil {
		return nil, err
	}

	if cachedBuf, ok := s.connCachedBuf[conn]; ok {
		buf = append(cachedBuf, buf...)
	}

	msgList, buf, err := snrpc.Unpack(buf)
	if err != nil {
		return nil, err
	}

	s.connCachedBuf[conn] = buf

	return msgList, nil
}

func (s *Server) write(conn net.Conn, buf []byte) error {
	bufLen := len(buf)
	var written int
	for {
		n, err := conn.Write(buf[written:])
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
