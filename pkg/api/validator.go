package api

import (
	"github.com/995933447/bucketmq/pkg/api/errs"
)

type Validator interface {
	Validate() *errs.RPCError
}
