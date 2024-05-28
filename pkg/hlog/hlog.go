package hlog

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
)

var logger *logrus.Logger

type Fields = logrus.Fields

func Initialize() error {
	logger = logrus.New()
	logger.Out = os.Stdout

	// Default log level to error
	logLevel := "ERROR"

	// If log level in environment, use it
	if l := os.Getenv("HYPHADB_LOG_LEVEL"); l != "" {
		logLevel = l
	}

	// If log level passed in flag, prefer it
	//TODO

	// Set loglevel
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		return fmt.Errorf("invalid log level: %v", err)
	}
	logger.SetLevel(level)

	return nil
}

func Debug(args ...interface{}) {
	logger.Debug(args...)
}

func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

func Warn(args ...interface{}) {
	logger.Warn(args...)
}

func Warnf(format string, args ...interface{}) {
	logger.Warnf(format, args...)
}
func Info(args ...interface{}) {
	logger.Info(args...)
}

func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

func Error(args ...interface{}) {
	logger.Error(args...)
}

func Errorf(format string, args ...interface{}) {
	logger.Errorf(format, args...)
}

func Fatal(args ...interface{}) {
	logger.Fatal(args...)
}

func Fatalf(format string, args ...interface{}) {
	logger.Fatalf(format, args...)
}

func WithFields(fields logrus.Fields) *logrus.Entry {
	return logger.WithFields(fields)
}

func WithError(err error) *logrus.Entry {
	return logger.WithError(err)
}
