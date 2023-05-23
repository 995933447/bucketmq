package snrpc

import "errors"

var (
	ErrCallbackReqTimeout = errors.New("callback request timeout")
	ErrServerExited       = errors.New("server exited")
)
