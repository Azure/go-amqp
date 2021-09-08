package amqp

import (
	"fmt"

	"github.com/Azure/go-amqp/internal/buffer"
)

// Sender Settlement Modes
const (
	// Sender will send all deliveries initially unsettled to the receiver.
	ModeUnsettled SenderSettleMode = 0

	// Sender will send all deliveries settled to the receiver.
	ModeSettled SenderSettleMode = 1

	// Sender MAY send a mixture of settled and unsettled deliveries to the receiver.
	ModeMixed SenderSettleMode = 2
)

// SenderSettleMode specifies how the sender will settle messages.
type SenderSettleMode uint8

func (m *SenderSettleMode) String() string {
	if m == nil {
		return "<nil>"
	}

	switch *m {
	case ModeUnsettled:
		return "unsettled"

	case ModeSettled:
		return "settled"

	case ModeMixed:
		return "mixed"

	default:
		return fmt.Sprintf("unknown sender mode %d", uint8(*m))
	}
}

func (m SenderSettleMode) marshal(wr *buffer.Buffer) error {
	return marshal(wr, uint8(m))
}

func (m *SenderSettleMode) unmarshal(r *buffer.Buffer) error {
	n, err := readUbyte(r)
	*m = SenderSettleMode(n)
	return err
}

func (m *SenderSettleMode) value() SenderSettleMode {
	if m == nil {
		return ModeMixed
	}
	return *m
}

// Receiver Settlement Modes
const (
	// Receiver will spontaneously settle all incoming transfers.
	ModeFirst ReceiverSettleMode = 0

	// Receiver will only settle after sending the disposition to the
	// sender and receiving a disposition indicating settlement of
	// the delivery from the sender.
	ModeSecond ReceiverSettleMode = 1
)

// ReceiverSettleMode specifies how the receiver will settle messages.
type ReceiverSettleMode uint8

func (m *ReceiverSettleMode) String() string {
	if m == nil {
		return "<nil>"
	}

	switch *m {
	case ModeFirst:
		return "first"

	case ModeSecond:
		return "second"

	default:
		return fmt.Sprintf("unknown receiver mode %d", uint8(*m))
	}
}

func (m ReceiverSettleMode) marshal(wr *buffer.Buffer) error {
	return marshal(wr, uint8(m))
}

func (m *ReceiverSettleMode) unmarshal(r *buffer.Buffer) error {
	n, err := readUbyte(r)
	*m = ReceiverSettleMode(n)
	return err
}

func (m *ReceiverSettleMode) value() ReceiverSettleMode {
	if m == nil {
		return ModeFirst
	}
	return *m
}

// Durability Policies
const (
	// No terminus state is retained durably.
	DurabilityNone Durability = 0

	// Only the existence and configuration of the terminus is
	// retained durably.
	DurabilityConfiguration Durability = 1

	// In addition to the existence and configuration of the
	// terminus, the unsettled state for durable messages is
	// retained durably.
	DurabilityUnsettledState Durability = 2
)

// Durability specifies the durability of a link.
type Durability uint32

func (d *Durability) String() string {
	if d == nil {
		return "<nil>"
	}

	switch *d {
	case DurabilityNone:
		return "none"
	case DurabilityConfiguration:
		return "configuration"
	case DurabilityUnsettledState:
		return "unsettled-state"
	default:
		return fmt.Sprintf("unknown durability %d", *d)
	}
}

func (d Durability) marshal(wr *buffer.Buffer) error {
	return marshal(wr, uint32(d))
}

func (d *Durability) unmarshal(r *buffer.Buffer) error {
	return unmarshal(r, (*uint32)(d))
}

// Expiry Policies
const (
	// The expiry timer starts when terminus is detached.
	ExpiryLinkDetach ExpiryPolicy = "link-detach"

	// The expiry timer starts when the most recently
	// associated session is ended.
	ExpirySessionEnd ExpiryPolicy = "session-end"

	// The expiry timer starts when most recently associated
	// connection is closed.
	ExpiryConnectionClose ExpiryPolicy = "connection-close"

	// The terminus never expires.
	ExpiryNever ExpiryPolicy = "never"
)

// ExpiryPolicy specifies when the expiry timer of a terminus
// starts counting down from the timeout value.
//
// If the link is subsequently re-attached before the terminus is expired,
// then the count down is aborted. If the conditions for the
// terminus-expiry-policy are subsequently re-met, the expiry timer restarts
// from its originally configured timeout value.
type ExpiryPolicy symbol

func (e ExpiryPolicy) validate() error {
	switch e {
	case ExpiryLinkDetach,
		ExpirySessionEnd,
		ExpiryConnectionClose,
		ExpiryNever:
		return nil
	default:
		return fmt.Errorf("unknown expiry-policy %q", e)
	}
}

func (e ExpiryPolicy) marshal(wr *buffer.Buffer) error {
	return symbol(e).marshal(wr)
}

func (e *ExpiryPolicy) unmarshal(r *buffer.Buffer) error {
	err := unmarshal(r, (*symbol)(e))
	if err != nil {
		return err
	}
	return e.validate()
}

func (e *ExpiryPolicy) String() string {
	if e == nil {
		return "<nil>"
	}
	return string(*e)
}
