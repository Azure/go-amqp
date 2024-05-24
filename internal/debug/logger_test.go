package debug

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogLevel(t *testing.T) {
	for _, testcase := range []struct {
		name  string
		level slog.Level
		wants int
	}{
		{
			name:  "UnfilteredLevel",
			level: slog.LevelDebug,
			wants: 4,
		},
		{
			name:  "DefaultLevelInfo",
			level: slog.LevelInfo,
			wants: 3,
		},
		{
			name:  "ErrorOnly",
			level: slog.LevelError,
			wants: 1,
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			ctx := context.Background()
			buf := bytes.NewBuffer(nil)

			RegisterLogger(slog.NewJSONHandler(buf, &slog.HandlerOptions{
				Level: testcase.level,
			}))

			Log(ctx, slog.LevelDebug, "debug")
			Log(ctx, slog.LevelInfo, "info")
			Log(ctx, slog.LevelWarn, "warn")
			Log(ctx, slog.LevelError, "error")

			require.Equal(t, testcase.wants, strings.Count(buf.String(), "\n"))
		})
	}
}

func TestAssert(t *testing.T) {
	for _, testcase := range []struct {
		name       string
		comparison bool
		wants      bool
	}{
		{
			name:       "ComparisonIsTrue",
			comparison: true, // 0 == 0
			wants:      false,
		},
		{
			name:       "ComparisonIsTrue",
			comparison: false, // 0 == 1
			wants:      true,
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			ctx := context.Background()
			buf := bytes.NewBuffer(nil)

			RegisterLogger(slog.NewJSONHandler(buf, &slog.HandlerOptions{}))

			Assert(ctx, testcase.comparison)

			require.Equal(t, testcase.wants, buf.Len() > 0)
		})
	}
}
