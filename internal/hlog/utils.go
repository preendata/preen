package hlog

import (
	"runtime"
)

// getCaller returns the file and line number of the caller.
func getCaller() (string, int) {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		return "unknown", 0
	}

	return file, line
}
