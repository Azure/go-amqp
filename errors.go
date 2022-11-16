package amqp

import (
	"errors"
	"fmt"

	"github.com/Azure/go-amqp/internal/encoding"
)

// ErrCond is an AMQP defined error condition.
// See http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-amqp-error for info on their meaning.
type ErrCond = encoding.ErrCond

// Error Conditions
const (
	// AMQP Errors
	ErrCondInternalError         ErrCond = "amqp:internal-error"
	ErrCondNotFound              ErrCond = "amqp:not-found"
	ErrCondUnauthorizedAccess    ErrCond = "amqp:unauthorized-access"
	ErrCondDecodeError           ErrCond = "amqp:decode-error"
	ErrCondResourceLimitExceeded ErrCond = "amqp:resource-limit-exceeded"
	ErrCondNotAllowed            ErrCond = "amqp:not-allowed"
	ErrCondInvalidField          ErrCond = "amqp:invalid-field"
	ErrCondNotImplemented        ErrCond = "amqp:not-implemented"
	ErrCondResourceLocked        ErrCond = "amqp:resource-locked"
	ErrCondPreconditionFailed    ErrCond = "amqp:precondition-failed"
	ErrCondResourceDeleted       ErrCond = "amqp:resource-deleted"
	ErrCondIllegalState          ErrCond = "amqp:illegal-state"
	ErrCondFrameSizeTooSmall     ErrCond = "amqp:frame-size-too-small"

	// Connection Errors
	ErrCondConnectionForced   ErrCond = "amqp:connection:forced"
	ErrCondFramingError       ErrCond = "amqp:connection:framing-error"
	ErrCondConnectionRedirect ErrCond = "amqp:connection:redirect"

	// Session Errors
	ErrCondWindowViolation  ErrCond = "amqp:session:window-violation"
	ErrCondErrantLink       ErrCond = "amqp:session:errant-link"
	ErrCondHandleInUse      ErrCond = "amqp:session:handle-in-use"
	ErrCondUnattachedHandle ErrCond = "amqp:session:unattached-handle"

	// Link Errors
	ErrCondDetachForced          ErrCond = "amqp:link:detach-forced"
	ErrCondTransferLimitExceeded ErrCond = "amqp:link:transfer-limit-exceeded"
	ErrCondMessageSizeExceeded   ErrCond = "amqp:link:message-size-exceeded"
	ErrCondLinkRedirect          ErrCond = "amqp:link:redirect"
	ErrCondStolen                ErrCond = "amqp:link:stolen"
)

type Error = encoding.Error

// DetachError is returned by a link (Receiver/Sender) when a detach frame is received.
//
// RemoteError will be nil if the link was detached gracefully.
type DetachError struct {
	RemoteError *Error
}

func (e *DetachError) Error() string {
	return fmt.Sprintf("link detached, reason: %+v", e.RemoteError)
}

// Errors
var (
	// ErrSessionClosed is propagated to Sender/Receivers
	// when Session.Close() is called.
	ErrSessionClosed = errors.New("amqp: session closed")

	// ErrLinkClosed is returned by send and receive operations when
	// Sender.Close() or Receiver.Close() are called.
	ErrLinkClosed = errors.New("amqp: link closed")
)

// ConnectionError is propagated to Session and Senders/Receivers
// when the connection has been closed or is no longer functional.
type ConnectionError struct {
	inner error
}

func (c *ConnectionError) Error() string {
	if c.inner == nil {
		return "amqp: connection closed"
	}
	return c.inner.Error()
}
