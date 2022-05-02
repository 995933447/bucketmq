package log

import "context"

type Logger interface {
	Info(context.Context, interface{})
	Infof(context.Context, interface{}, ...interface{})
	Debug(context.Context, interface{})
	Debugf(context.Context, interface{}, ...interface{})
	Warn(context.Context, interface{})
	Warnf(context.Context, interface{}, ...interface{})
	Error(context.Context, interface{})
	Errorf(context.Context, interface{}, ...interface{})
}

var DefaultLogger Logger = &EmptyLogger{}