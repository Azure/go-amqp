package debug

import (
	"context"
	"log/slog"
)

var (
	logger = slog.New(noOp{})
)

func RegisterLogger(h slog.Handler) {
	logger = slog.New(h)
}

// Log writes the log message to the configured log handler.
// Level indicates the verbosity of the messages to log, as defined in log/slog.
// Arguments can be added as required, preferably as a set of slog.Attr.
func Log(ctx context.Context, level slog.Level, msg string, args ...any) {
	logger.Log(ctx, level, msg, args...)
}

// Assert registers an error-level log message if the specified condition is false, optionally alongside
// any meaningful (set of) slog.Attr(s).
func Assert(ctx context.Context, condition bool, args ...any) {
	if !condition {
		logger.Log(ctx, slog.LevelError, "assertion failed", args...)
	}
}
