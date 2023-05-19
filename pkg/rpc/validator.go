package rpc

import (
	"github.com/995933447/bucketmq/pkg/rpc/errs"
)

type Validator interface {
	Validate() *errs.RPCError
}
