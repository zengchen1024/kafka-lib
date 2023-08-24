package mq

import "fmt"

func NewLogger() Logger {
	return logImpl{}
}

type logImpl struct{}

func (logImpl) Info(args ...interface{}) {
	fmt.Print(args...)
}

func (logImpl) Warn(args ...interface{}) {
	fmt.Print(args...)
}

func (logImpl) Error(args ...interface{}) {
	fmt.Print(args...)
}

func (logImpl) Errorf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

func (logImpl) Infof(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}
