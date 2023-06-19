package middleware

import (
	"context"
	"github.com/995933447/bucketmq/internal/util"
	"github.com/995933447/bucketmq/pkg/rpc"
	"google.golang.org/grpc"
)

func Recover() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		defer util.StackRecover()
		return handler(ctx, req)
	}
}

func AutoValidate() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if validator, ok := req.(rpc.Validator); ok {
			err := validator.Validate()
			if err != nil {
				util.Logger.Errorf(nil, "err is %v", err)
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}
