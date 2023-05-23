package errs

import (
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func GRPCErr(code ErrCode, msg string) error {
	if msg == "" {
		msg = code.String()
	}
	return status.Error(codes.Code(code), msg)
}

func ToRPCErr(err error) (*RPCError, bool) {
	if rpcErr, ok := err.(*RPCError); ok {
		return rpcErr, false
	}
	return nil, false
}

func RPCErr(code ErrCode, msg string) *RPCError {
	if msg == "" {
		msg = code.String()
	}
	return &RPCError{
		Code: code,
		Msg:  msg,
	}
}

func (m *RPCError) Error() string {
	return fmt.Sprintf("code:%d message:%s", m.Code, m.Msg)
}
