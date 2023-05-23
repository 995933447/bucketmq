package snrpc

import (
	"fmt"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/rpc"
	"github.com/995933447/bucketmq/pkg/rpc/errs"
	"github.com/995933447/bucketmq/pkg/rpc/snrpc"
	"github.com/995933447/gonetutil"
	"github.com/golang/protobuf/proto"
	"io"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type MsgHandler struct {
	protoId    uint32
	msgType    reflect.Type
	handleFunc HandleMsgFunc
}

type HandleMsgFunc func(conn net.Conn, req proto.Message) (proto.Message, error)

type callbackResp struct {
	err  *errs.RPCError
	data []byte
}

type callbackCtx struct {
	respCh chan *callbackResp
}

func NewServer(host string, port int, maxConnNum uint32) (*Server, error) {
	return &Server{
		host:                host,
		port:                port,
		maxConnNum:          maxConnNum,
		exitCh:              make(chan struct{}),
		conns:               map[net.Conn]struct{}{},
		protoIdToHandlerMap: map[uint32]*MsgHandler{},
	}, nil
}

type Server struct {
	host                string
	port                int
	sNToCallbackCtxMap  sync.Map
	maxConnNum          uint32
	listener            net.Listener
	exitCh              chan struct{}
	exited              atomic.Bool
	conns               map[net.Conn]struct{}
	connCachedBuf       sync.Map
	opConnsMu           sync.RWMutex
	protoIdToHandlerMap map[uint32]*MsgHandler
}

func (s *Server) GetConns() []net.Conn {
	s.opConnsMu.RLock()
	defer s.opConnsMu.RUnlock()
	var conns []net.Conn
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	return conns
}

func (s *Server) CloseConn(conn net.Conn) {
	s.opConnsMu.Lock()
	defer s.opConnsMu.Unlock()

	delete(s.conns, conn)
	s.connCachedBuf.Delete(conn)
	_ = conn.Close()
}

func (s *Server) Callback(conn net.Conn, timeout time.Duration, protoId uint32, req proto.Message, resp proto.Message) error {
	sN := snrpc.GenSN()
	buf, err := snrpc.Pack(protoId, sN, true, req)
	if err != nil {
		return err
	}

	respCh := make(chan *callbackResp)
	s.sNToCallbackCtxMap.Store(sN, &callbackCtx{
		respCh: respCh,
	})

	if err = s.write(conn, buf); err != nil {
		s.sNToCallbackCtxMap.Delete(sN)
		return err
	}

	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()
	select {
	case <-s.exitCh:
	default:
		if s.exited.Load() {
			return snrpc.ErrServerExited
		}
		select {
		case <-s.exitCh:
			return snrpc.ErrServerExited
		case <-timeoutTimer.C:
			return snrpc.ErrCallbackReqTimeout
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

func (s *Server) RegProto(protoId uint32, msg interface{}, msgFunc HandleMsgFunc) {
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
		if s.exited.Load() {
			goto out
		}

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
					goto out
				}
				continue
			}

			util.Logger.Error(nil, err)
			continue
		}

		s.opConnsMu.RLock()
		if s.maxConnNum < uint32(len(s.conns)) {
			s.opConnsMu.RUnlock()
			continue
		}
		s.opConnsMu.RUnlock()

		s.opConnsMu.Lock()
		s.conns[conn] = struct{}{}
		s.opConnsMu.Unlock()

		go s.readAndProc(conn)

		tempDelay = 0
	}

out:
	for conn := range s.conns {
		s.CloseConn(conn)
	}
	return nil
}

func (s *Server) Exit() error {
	if err := s.listener.Close(); err != nil {
		return err
	}
	s.exited.Store(true)
	for {
		select {
		case s.exitCh <- struct{}{}:
		default:
			goto out
		}
	}
out:
	return nil
}

func (s *Server) readAndProc(conn net.Conn) {
	defer s.CloseConn(conn)
	for {
		if s.exited.Load() {
			break
		}

		msgList, err := s.read(conn)
		if err != nil {
			util.Logger.Error(nil, err)
			break
		}

		for _, msg := range msgList {
			go s.handleMsg(conn, msg)
		}
	}
}

func (s *Server) handleMsg(conn net.Conn, msg *snrpc.Msg) {
	if !msg.IsReq {
		callbackCtxt, ok := s.sNToCallbackCtxMap.Load(msg.SN)
		if !ok {
			return
		}
		var callbackRsp callbackResp
		callbackRsp.data = msg.Data
		if msg.Err != nil {
			callbackRsp.err = msg.Err
		}
		callbackCtxt.(*callbackCtx).respCh <- &callbackRsp
		return
	}

	handler, ok := s.protoIdToHandlerMap[msg.ProtoId]
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

	var resp proto.Message
	if validator, ok := req.(rpc.Validator); ok {
		if err := validator.Validate(); err != nil {
			resp = err
		}
	} else {
		resp, err = handler.handleFunc(conn, req)
		if err != nil {
			if _, ok := err.(*errs.RPCError); !ok {
				err = errs.RPCErr(errs.ErrCode_ErrCodeInternal, err.Error())
			}

			resp = err.(*errs.RPCError)
		}
	}

	buf, err := snrpc.Pack(msg.ProtoId, msg.SN, false, resp)
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

	if cachedBuf, ok := s.connCachedBuf.Load(conn); ok {
		buf = append(cachedBuf.([]byte), buf...)
	}

	msgList, buf, err := snrpc.Unpack(buf)
	if err != nil {
		return nil, err
	}

	if len(buf) > 0 {
		s.connCachedBuf.Store(conn, buf)
	} else {
		s.connCachedBuf.Delete(conn)
	}

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
