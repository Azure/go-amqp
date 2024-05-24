package debug

import (
	"context"
	"log/slog"
)

type noOp struct{}

func (noOp) Enabled(context.Context, slog.Level) bool  { return false }
func (noOp) Handle(context.Context, slog.Record) error { return nil }
func (h noOp) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h noOp) WithGroup(string) slog.Handler           { return h }
