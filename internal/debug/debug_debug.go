//go:build debug
// +build debug

package debug

import "log"
import "os"
import "strconv"

var (
	debugLevel = 1
	logger     = log.New(os.Stderr, "", log.Lmicroseconds)
)

func init() {
	level, err := strconv.Atoi(os.Getenv("DEBUG_LEVEL"))
	if err != nil {
		return
	}

	debugLevel = level
}

// Log writes the formatted string to stderr.
// Level indicates the verbosity of the messages to log.
// The greater the value, the more verbose messages will be logged.
func Log(level int, format string, v ...interface{}) {
	if level <= debugLevel {
		logger.Printf(format, v...)
	}
}
