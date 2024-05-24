package amqp

import (
	"log/slog"

	"github.com/Azure/go-amqp/internal/debug"
)

// RegisterLogger configures the library's debug logger with the input slog.Handler h.
//
// By default, the debug logger uses a no-op handler and doesn't produce any log events.
func RegisterLogger(h slog.Handler) {
	debug.RegisterLogger(h)
}
