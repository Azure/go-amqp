package amqp_test

import (
	"errors"
	"testing"

	amqp "github.com/Azure/go-amqp"
	"github.com/stretchr/testify/require"
)

func TestErrorUnwrap(t *testing.T) {
	// In the majority of common use cases, the LinkError, ConnError and SessionError will contain an amqp.Error.
	// It's simpler, for callers, if they can simply check errors.As(&amqp.Error) so they can write general error
	// handling, rather than having to check the envelope type each time.
	t.Run("LinkError", func(t *testing.T) {
		var amqpErr *amqp.Error

		le := &amqp.LinkError{}
		require.False(t, errors.As(le, &amqpErr))

		le.RemoteErr = &amqp.Error{Condition: amqp.ErrCondConnectionForced}
		require.ErrorAs(t, le, &amqpErr)
	})

	t.Run("ConnError", func(t *testing.T) {
		var amqpErr *amqp.Error

		ce := &amqp.ConnError{}
		require.False(t, errors.As(ce, &amqpErr))

		ce.RemoteErr = &amqp.Error{Condition: amqp.ErrCondConnectionForced}
		require.ErrorAs(t, ce, &amqpErr)
	})

	t.Run("SessionError", func(t *testing.T) {
		var amqpErr *amqp.Error

		se := &amqp.ConnError{}
		require.False(t, errors.As(se, &amqpErr))

		se.RemoteErr = &amqp.Error{Condition: amqp.ErrCondConnectionForced}
		require.ErrorAs(t, se, &amqpErr)
	})
}
