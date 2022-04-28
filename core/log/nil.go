package log

import "context"

type NilLogger struct {}

func (*NilLogger) Info(_ context.Context, _ string) {

}

func (*NilLogger) Infof(_ context.Context, _ string, _ ...interface{}) {

}

func (*NilLogger) Debug(_ context.Context, _ string) {

}

func (*NilLogger) Debugf(_ context.Context, _ string, _ ...interface{}) {

}

func (*NilLogger) Warn(_ context.Context, _ string) {

}

func (*NilLogger) Warnf(_ context.Context, _ string, _ ...interface{}) {

}

func (*NilLogger) Error(_ context.Context, _ interface{}) {

}

func (*NilLogger) Errorf(_ context.Context, _ string, _ ...interface{}) {

}