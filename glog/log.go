package glog

type Printer interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
}

type Logger interface {
	Printer
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
}
