package middleware

import (
	"context"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/rpc"
	"google.golang.org/grpc"
)

func Recover() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		defer util.StackRecover()
		resp, err = handler(ctx, req)
		return
	}
}

func AutoValidate() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		if validator, ok := req.(rpc.Validator); ok {
			err = validator.Validate()
			if err != nil {
				return
			}
		}
		resp, err = handler(ctx, req)
		return
	}
}
