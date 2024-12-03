package encoding

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"

	"github.com/Azure/go-amqp/internal/buffer"
)

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

func (d Durability) Marshal(wr *buffer.Buffer) error {
	return Marshal(wr, uint32(d))
}

func (d *Durability) Unmarshal(r *buffer.Buffer) error {
	return Unmarshal(r, (*uint32)(d))
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
type ExpiryPolicy Symbol

func ValidateExpiryPolicy(e ExpiryPolicy) error {
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

func (e ExpiryPolicy) Marshal(wr *buffer.Buffer) error {
	return Symbol(e).Marshal(wr)
}

func (e *ExpiryPolicy) Unmarshal(r *buffer.Buffer) error {
	err := Unmarshal(r, (*Symbol)(e))
	if err != nil {
		return err
	}
	return ValidateExpiryPolicy(*e)
}

func (e *ExpiryPolicy) String() string {
	if e == nil {
		return "<nil>"
	}
	return string(*e)
}

// Sender Settlement Modes
const (
	// Sender will send all deliveries initially unsettled to the receiver.
	SenderSettleModeUnsettled SenderSettleMode = 0

	// Sender will send all deliveries settled to the receiver.
	SenderSettleModeSettled SenderSettleMode = 1

	// Sender MAY send a mixture of settled and unsettled deliveries to the receiver.
	SenderSettleModeMixed SenderSettleMode = 2
)

// SenderSettleMode specifies how the sender will settle messages.
type SenderSettleMode uint8

func (m SenderSettleMode) Ptr() *SenderSettleMode {
	return &m
}

func (m *SenderSettleMode) String() string {
	if m == nil {
		return "<nil>"
	}

	switch *m {
	case SenderSettleModeUnsettled:
		return "unsettled"

	case SenderSettleModeSettled:
		return "settled"

	case SenderSettleModeMixed:
		return "mixed"

	default:
		return fmt.Sprintf("unknown sender mode %d", uint8(*m))
	}
}

func (m SenderSettleMode) Marshal(wr *buffer.Buffer) error {
	return Marshal(wr, uint8(m))
}

func (m *SenderSettleMode) Unmarshal(r *buffer.Buffer) error {
	n, err := ReadUbyte(r)
	*m = SenderSettleMode(n)
	return err
}

// Receiver Settlement Modes
const (
	// Receiver will spontaneously settle all incoming transfers.
	ReceiverSettleModeFirst ReceiverSettleMode = 0

	// Receiver will only settle after sending the disposition to the
	// sender and receiving a disposition indicating settlement of
	// the delivery from the sender.
	ReceiverSettleModeSecond ReceiverSettleMode = 1
)

// ReceiverSettleMode specifies how the receiver will settle messages.
type ReceiverSettleMode uint8

func (m ReceiverSettleMode) Ptr() *ReceiverSettleMode {
	return &m
}

func (m *ReceiverSettleMode) String() string {
	if m == nil {
		return "<nil>"
	}

	switch *m {
	case ReceiverSettleModeFirst:
		return "first"

	case ReceiverSettleModeSecond:
		return "second"

	default:
		return fmt.Sprintf("unknown receiver mode %d", uint8(*m))
	}
}

func (m ReceiverSettleMode) Marshal(wr *buffer.Buffer) error {
	return Marshal(wr, uint8(m))
}

func (m *ReceiverSettleMode) Unmarshal(r *buffer.Buffer) error {
	n, err := ReadUbyte(r)
	*m = ReceiverSettleMode(n)
	return err
}

type Filter map[Symbol]*DescribedType

func (f Filter) Marshal(wr *buffer.Buffer) error {
	return writeMap(wr, f)
}

func (f *Filter) Unmarshal(r *buffer.Buffer) error {
	count, err := readMapHeader(r)
	if err != nil {
		return err
	}

	m := make(Filter, count/2)
	for i := uint32(0); i < count; i += 2 {
		key, err := ReadString(r)
		if err != nil {
			return err
		}
		var value DescribedType
		err = Unmarshal(r, &value)
		if err != nil {
			return err
		}
		m[Symbol(key)] = &value
	}
	*f = m
	return nil
}

// Annotations keys must be of type string, int, or int64.
//
// String keys are encoded as AMQP Symbols.
type Annotations map[any]any

func (a Annotations) Marshal(wr *buffer.Buffer) error {
	return writeMap(wr, a)
}

func (a *Annotations) Unmarshal(r *buffer.Buffer) error {
	count, err := readMapHeader(r)
	if err != nil {
		return err
	}

	m := make(Annotations, count/2)
	for i := uint32(0); i < count; i += 2 {
		key, err := ReadAny(r)
		if err != nil {
			return err
		}
		value, err := ReadAny(r)
		if err != nil {
			return err
		}
		m[key] = value
	}
	*a = m
	return nil
}

// ErrCond is one of the error conditions defined in the AMQP spec.
type ErrCond string

func (ec ErrCond) Marshal(wr *buffer.Buffer) error {
	return (Symbol)(ec).Marshal(wr)
}

func (ec *ErrCond) Unmarshal(r *buffer.Buffer) error {
	s, err := ReadString(r)
	*ec = ErrCond(s)
	return err
}

/*
<type name="error" class="composite" source="list">
    <descriptor name="amqp:error:list" code="0x00000000:0x0000001d"/>
    <field name="condition" type="symbol" requires="error-condition" mandatory="true"/>
    <field name="description" type="string"/>
    <field name="info" type="fields"/>
</type>
*/

// Error is an AMQP error.
type Error struct {
	// A symbolic value indicating the error condition.
	Condition ErrCond

	// descriptive text about the error condition
	//
	// This text supplies any supplementary details not indicated by the condition field.
	// This text can be logged as an aid to resolving issues.
	Description string

	// map carrying information about the error condition
	Info map[string]any
}

func (e *Error) Marshal(wr *buffer.Buffer) error {
	return MarshalComposite(wr, TypeCodeError, []MarshalField{
		{Value: &e.Condition, Omit: false},
		{Value: &e.Description, Omit: e.Description == ""},
		{Value: e.Info, Omit: len(e.Info) == 0},
	})
}

func (e *Error) Unmarshal(r *buffer.Buffer) error {
	return UnmarshalComposite(r, TypeCodeError, []UnmarshalField{
		{Field: &e.Condition, HandleNull: func() error { return errors.New("Error.Condition is required") }},
		{Field: &e.Description},
		{Field: &e.Info},
	}...)
}

func (e *Error) String() string {
	if e == nil {
		return "*Error(nil)"
	}
	return fmt.Sprintf("*Error{Condition: %s, Description: %s, Info: %v}",
		e.Condition,
		e.Description,
		e.Info,
	)
}

func (e *Error) Error() string {
	return e.String()
}

// symbol is an AMQP symbolic string.
type Symbol string

func (s Symbol) Marshal(wr *buffer.Buffer) error {
	l := len(s)
	switch {
	// Sym8
	case l < 256:
		wr.Append([]byte{
			byte(TypeCodeSym8),
			byte(l),
		})
		wr.AppendString(string(s))

	// Sym32
	case uint(l) < math.MaxUint32:
		wr.AppendByte(uint8(TypeCodeSym32))
		wr.AppendUint32(uint32(l))
		wr.AppendString(string(s))
	default:
		return errors.New("too long")
	}
	return nil
}

// UUID is a 128 bit identifier as defined in RFC 4122.
type UUID [16]byte

// String returns the hex encoded representation described in RFC 4122, Section 3.
func (u UUID) String() string {
	var buf [36]byte
	hex.Encode(buf[:8], u[:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:], u[10:])
	return string(buf[:])
}

func (u UUID) Marshal(wr *buffer.Buffer) error {
	wr.AppendByte(byte(TypeCodeUUID))
	wr.Append(u[:])
	return nil
}

func (u *UUID) Unmarshal(r *buffer.Buffer) error {
	un, err := readUUID(r)
	*u = un
	return err
}

type DescribedType struct {
	Descriptor any
	Value      any
}

func (t DescribedType) Marshal(wr *buffer.Buffer) error {
	wr.AppendByte(0x0) // descriptor constructor
	err := Marshal(wr, t.Descriptor)
	if err != nil {
		return err
	}
	return Marshal(wr, t.Value)
}

func (t *DescribedType) Unmarshal(r *buffer.Buffer) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}

	if b != 0x0 {
		return fmt.Errorf("invalid described type header %02x", b)
	}

	err = Unmarshal(r, &t.Descriptor)
	if err != nil {
		return err
	}
	return Unmarshal(r, &t.Value)
}

func (t DescribedType) String() string {
	return fmt.Sprintf("DescribedType{descriptor: %v, value: %v}",
		t.Descriptor,
		t.Value,
	)
}
