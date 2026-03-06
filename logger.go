package tstorage

// TODO: 考虑另一种抽象方式

// Logger 是一个日志记录接口
type Logger interface {
	Printf(format string, v ...interface{})
}

type nopLogger struct{}

func (l *nopLogger) Printf(_ string, _ ...interface{}) {
	// 什么都不做
	return
}
