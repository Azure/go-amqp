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
	ErrCondDecodeError           ErrCond = "amqp:decode-error"
	ErrCondFrameSizeTooSmall     ErrCond = "amqp:frame-size-too-small"
	ErrCondIllegalState          ErrCond = "amqp:illegal-state"
	ErrCondInternalError         ErrCond = "amqp:internal-error"
	ErrCondInvalidField          ErrCond = "amqp:invalid-field"
	ErrCondNotAllowed            ErrCond = "amqp:not-allowed"
	ErrCondNotFound              ErrCond = "amqp:not-found"
	ErrCondNotImplemented        ErrCond = "amqp:not-implemented"
	ErrCondPreconditionFailed    ErrCond = "amqp:precondition-failed"
	ErrCondResourceDeleted       ErrCond = "amqp:resource-deleted"
	ErrCondResourceLimitExceeded ErrCond = "amqp:resource-limit-exceeded"
	ErrCondResourceLocked        ErrCond = "amqp:resource-locked"
	ErrCondUnauthorizedAccess    ErrCond = "amqp:unauthorized-access"

	// Connection Errors
	ErrCondConnectionForced   ErrCond = "amqp:connection:forced"
	ErrCondConnectionRedirect ErrCond = "amqp:connection:redirect"
	ErrCondFramingError       ErrCond = "amqp:connection:framing-error"

	// Session Errors
	ErrCondErrantLink       ErrCond = "amqp:session:errant-link"
	ErrCondHandleInUse      ErrCond = "amqp:session:handle-in-use"
	ErrCondUnattachedHandle ErrCond = "amqp:session:unattached-handle"
	ErrCondWindowViolation  ErrCond = "amqp:session:window-violation"

	// Link Errors
	ErrCondDetachForced          ErrCond = "amqp:link:detach-forced"
	ErrCondLinkRedirect          ErrCond = "amqp:link:redirect"
	ErrCondMessageSizeExceeded   ErrCond = "amqp:link:message-size-exceeded"
	ErrCondStolen                ErrCond = "amqp:link:stolen"
	ErrCondTransferLimitExceeded ErrCond = "amqp:link:transfer-limit-exceeded"
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
