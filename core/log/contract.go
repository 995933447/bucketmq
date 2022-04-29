package log

import "context"

type Logger interface {
	Info(context.Context, string)
	Infof(context.Context, string, ...interface{})
	Debug(context.Context, string)
	Debugf(context.Context, string, ...interface{})
	Warn(context.Context, string)
	Warnf(context.Context, string, ...interface{})
	Error(context.Context, interface{})
	Errorf(context.Context, string, ...interface{})
}

// 等有真正好用的logger再替换
var DefaultLogger Logger = &NilLogger{}