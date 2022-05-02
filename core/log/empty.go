package log

import "context"

type EmptyLogger struct {}

func (*EmptyLogger) Info(_ context.Context, _ interface{}) {
}

func (*EmptyLogger) Infof(_ context.Context, _ interface{}, _ ...interface{}) {
}

func (*EmptyLogger) Debug(_ context.Context, _ interface{}) {
}

func (*EmptyLogger) Debugf(_ context.Context, _ interface{}, _ ...interface{}) {
}

func (*EmptyLogger) Warn(_ context.Context, _ interface{}) {
}

func (*EmptyLogger) Warnf(_ context.Context, _ interface{}, _ ...interface{}) {
}

func (*EmptyLogger) Error(_ context.Context, _ interface{}) {
}

func (*EmptyLogger) Errorf(_ context.Context, _ interface{}, _ ...interface{}) {
}