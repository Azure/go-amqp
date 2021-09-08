package amqp

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"
	"unicode/utf8"

	"github.com/Azure/go-amqp/internal/buffer"
)

type amqpType uint8

// Type codes
const (
	typeCodeNull amqpType = 0x40

	// Bool
	typeCodeBool      amqpType = 0x56 // boolean with the octet 0x00 being false and octet 0x01 being true
	typeCodeBoolTrue  amqpType = 0x41
	typeCodeBoolFalse amqpType = 0x42

	// Unsigned
	typeCodeUbyte      amqpType = 0x50 // 8-bit unsigned integer (1)
	typeCodeUshort     amqpType = 0x60 // 16-bit unsigned integer in network byte order (2)
	typeCodeUint       amqpType = 0x70 // 32-bit unsigned integer in network byte order (4)
	typeCodeSmallUint  amqpType = 0x52 // unsigned integer value in the range 0 to 255 inclusive (1)
	typeCodeUint0      amqpType = 0x43 // the uint value 0 (0)
	typeCodeUlong      amqpType = 0x80 // 64-bit unsigned integer in network byte order (8)
	typeCodeSmallUlong amqpType = 0x53 // unsigned long value in the range 0 to 255 inclusive (1)
	typeCodeUlong0     amqpType = 0x44 // the ulong value 0 (0)

	// Signed
	typeCodeByte      amqpType = 0x51 // 8-bit two's-complement integer (1)
	typeCodeShort     amqpType = 0x61 // 16-bit two's-complement integer in network byte order (2)
	typeCodeInt       amqpType = 0x71 // 32-bit two's-complement integer in network byte order (4)
	typeCodeSmallint  amqpType = 0x54 // 8-bit two's-complement integer (1)
	typeCodeLong      amqpType = 0x81 // 64-bit two's-complement integer in network byte order (8)
	typeCodeSmalllong amqpType = 0x55 // 8-bit two's-complement integer

	// Decimal
	typeCodeFloat      amqpType = 0x72 // IEEE 754-2008 binary32 (4)
	typeCodeDouble     amqpType = 0x82 // IEEE 754-2008 binary64 (8)
	typeCodeDecimal32  amqpType = 0x74 // IEEE 754-2008 decimal32 using the Binary Integer Decimal encoding (4)
	typeCodeDecimal64  amqpType = 0x84 // IEEE 754-2008 decimal64 using the Binary Integer Decimal encoding (8)
	typeCodeDecimal128 amqpType = 0x94 // IEEE 754-2008 decimal128 using the Binary Integer Decimal encoding (16)

	// Other
	typeCodeChar      amqpType = 0x73 // a UTF-32BE encoded Unicode character (4)
	typeCodeTimestamp amqpType = 0x83 // 64-bit two's-complement integer representing milliseconds since the unix epoch
	typeCodeUUID      amqpType = 0x98 // UUID as defined in section 4.1.2 of RFC-4122

	// Variable Length
	typeCodeVbin8  amqpType = 0xa0 // up to 2^8 - 1 octets of binary data (1 + variable)
	typeCodeVbin32 amqpType = 0xb0 // up to 2^32 - 1 octets of binary data (4 + variable)
	typeCodeStr8   amqpType = 0xa1 // up to 2^8 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (1 + variable)
	typeCodeStr32  amqpType = 0xb1 // up to 2^32 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (4 +variable)
	typeCodeSym8   amqpType = 0xa3 // up to 2^8 - 1 seven bit ASCII characters representing a symbolic value (1 + variable)
	typeCodeSym32  amqpType = 0xb3 // up to 2^32 - 1 seven bit ASCII characters representing a symbolic value (4 + variable)

	// Compound
	typeCodeList0   amqpType = 0x45 // the empty list (i.e. the list with no elements) (0)
	typeCodeList8   amqpType = 0xc0 // up to 2^8 - 1 list elements with total size less than 2^8 octets (1 + compound)
	typeCodeList32  amqpType = 0xd0 // up to 2^32 - 1 list elements with total size less than 2^32 octets (4 + compound)
	typeCodeMap8    amqpType = 0xc1 // up to 2^8 - 1 octets of encoded map data (1 + compound)
	typeCodeMap32   amqpType = 0xd1 // up to 2^32 - 1 octets of encoded map data (4 + compound)
	typeCodeArray8  amqpType = 0xe0 // up to 2^8 - 1 array elements with total size less than 2^8 octets (1 + array)
	typeCodeArray32 amqpType = 0xf0 // up to 2^32 - 1 array elements with total size less than 2^32 octets (4 + array)

	// Composites
	typeCodeOpen        amqpType = 0x10
	typeCodeBegin       amqpType = 0x11
	typeCodeAttach      amqpType = 0x12
	typeCodeFlow        amqpType = 0x13
	typeCodeTransfer    amqpType = 0x14
	typeCodeDisposition amqpType = 0x15
	typeCodeDetach      amqpType = 0x16
	typeCodeEnd         amqpType = 0x17
	typeCodeClose       amqpType = 0x18

	typeCodeSource amqpType = 0x28
	typeCodeTarget amqpType = 0x29
	typeCodeError  amqpType = 0x1d

	typeCodeMessageHeader         amqpType = 0x70
	typeCodeDeliveryAnnotations   amqpType = 0x71
	typeCodeMessageAnnotations    amqpType = 0x72
	typeCodeMessageProperties     amqpType = 0x73
	typeCodeApplicationProperties amqpType = 0x74
	typeCodeApplicationData       amqpType = 0x75
	typeCodeAMQPSequence          amqpType = 0x76
	typeCodeAMQPValue             amqpType = 0x77
	typeCodeFooter                amqpType = 0x78

	typeCodeStateReceived amqpType = 0x23
	typeCodeStateAccepted amqpType = 0x24
	typeCodeStateRejected amqpType = 0x25
	typeCodeStateReleased amqpType = 0x26
	typeCodeStateModified amqpType = 0x27

	typeCodeSASLMechanism amqpType = 0x40
	typeCodeSASLInit      amqpType = 0x41
	typeCodeSASLChallenge amqpType = 0x42
	typeCodeSASLResponse  amqpType = 0x43
	typeCodeSASLOutcome   amqpType = 0x44

	typeCodeDeleteOnClose             amqpType = 0x2b
	typeCodeDeleteOnNoLinks           amqpType = 0x2c
	typeCodeDeleteOnNoMessages        amqpType = 0x2d
	typeCodeDeleteOnNoLinksOrMessages amqpType = 0x2e
)

// Frame structure:
//
//     header (8 bytes)
//       0-3: SIZE (total size, at least 8 bytes for header, uint32)
//       4:   DOFF (data offset,at least 2, count of 4 bytes words, uint8)
//       5:   TYPE (frame type)
//                0x0: AMQP
//                0x1: SASL
//       6-7: type dependent (channel for AMQP)
//     extended header (opt)
//     body (opt)

// frameHeader in a structure appropriate for use with binary.Read()
type frameHeader struct {
	// size: an unsigned 32-bit integer that MUST contain the total frame size of the frame header,
	// extended header, and frame body. The frame is malformed if the size is less than the size of
	// the frame header (8 bytes).
	Size uint32
	// doff: gives the position of the body within the frame. The value of the data offset is an
	// unsigned, 8-bit integer specifying a count of 4-byte words. Due to the mandatory 8-byte
	// frame header, the frame is malformed if the value is less than 2.
	DataOffset uint8
	FrameType  uint8
	Channel    uint16
}

const (
	frameTypeAMQP = 0x0
	frameTypeSASL = 0x1

	frameHeaderSize = 8
)

type protoHeader struct {
	ProtoID  protoID
	Major    uint8
	Minor    uint8
	Revision uint8
}

type role bool

const (
	roleSender   role = false
	roleReceiver role = true
)

func (rl role) String() string {
	if rl {
		return "Receiver"
	}
	return "Sender"
}

func (rl *role) unmarshal(r *buffer.Buffer) error {
	b, err := readBool(r)
	*rl = role(b)
	return err
}

func (rl role) marshal(wr *buffer.Buffer) error {
	return marshal(wr, (bool)(rl))
}

type deliveryState interface{} // TODO: http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html#type-declared

type unsettled map[string]deliveryState

func (u unsettled) marshal(wr *buffer.Buffer) error {
	return writeMap(wr, u)
}

func (u *unsettled) unmarshal(r *buffer.Buffer) error {
	count, err := readMapHeader(r)
	if err != nil {
		return err
	}

	m := make(unsettled, count/2)
	for i := uint32(0); i < count; i += 2 {
		key, err := readString(r)
		if err != nil {
			return err
		}
		var value deliveryState
		err = unmarshal(r, &value)
		if err != nil {
			return err
		}
		m[key] = value
	}
	*u = m
	return nil
}

type filter map[symbol]*describedType

func (f filter) marshal(wr *buffer.Buffer) error {
	return writeMap(wr, f)
}

func (f *filter) unmarshal(r *buffer.Buffer) error {
	count, err := readMapHeader(r)
	if err != nil {
		return err
	}

	m := make(filter, count/2)
	for i := uint32(0); i < count; i += 2 {
		key, err := readString(r)
		if err != nil {
			return err
		}
		var value describedType
		err = unmarshal(r, &value)
		if err != nil {
			return err
		}
		m[symbol(key)] = &value
	}
	*f = m
	return nil
}

/*
<type name="source" class="composite" source="list" provides="source">
    <descriptor name="amqp:source:list" code="0x00000000:0x00000028"/>
    <field name="address" type="*" requires="address"/>
    <field name="durable" type="terminus-durability" default="none"/>
    <field name="expiry-policy" type="terminus-expiry-policy" default="session-end"/>
    <field name="timeout" type="seconds" default="0"/>
    <field name="dynamic" type="boolean" default="false"/>
    <field name="dynamic-node-properties" type="node-properties"/>
    <field name="distribution-mode" type="symbol" requires="distribution-mode"/>
    <field name="filter" type="filter-set"/>
    <field name="default-outcome" type="*" requires="outcome"/>
    <field name="outcomes" type="symbol" multiple="true"/>
    <field name="capabilities" type="symbol" multiple="true"/>
</type>
*/
type source struct {
	// the address of the source
	//
	// The address of the source MUST NOT be set when sent on a attach frame sent by
	// the receiving link endpoint where the dynamic flag is set to true (that is where
	// the receiver is requesting the sender to create an addressable node).
	//
	// The address of the source MUST be set when sent on a attach frame sent by the
	// sending link endpoint where the dynamic flag is set to true (that is where the
	// sender has created an addressable node at the request of the receiver and is now
	// communicating the address of that created node). The generated name of the address
	// SHOULD include the link name and the container-id of the remote container to allow
	// for ease of identification.
	Address string

	// indicates the durability of the terminus
	//
	// Indicates what state of the terminus will be retained durably: the state of durable
	// messages, only existence and configuration of the terminus, or no state at all.
	//
	// 0: none
	// 1: configuration
	// 2: unsettled-state
	Durable Durability

	// the expiry policy of the source
	//
	// link-detach: The expiry timer starts when terminus is detached.
	// session-end: The expiry timer starts when the most recently associated session is
	//              ended.
	// connection-close: The expiry timer starts when most recently associated connection
	//                   is closed.
	// never: The terminus never expires.
	ExpiryPolicy ExpiryPolicy

	// duration that an expiring source will be retained
	//
	// The source starts expiring as indicated by the expiry-policy.
	Timeout uint32 // seconds

	// request dynamic creation of a remote node
	//
	// When set to true by the receiving link endpoint, this field constitutes a request
	// for the sending peer to dynamically create a node at the source. In this case the
	// address field MUST NOT be set.
	//
	// When set to true by the sending link endpoint this field indicates creation of a
	// dynamically created node. In this case the address field will contain the address
	// of the created node. The generated address SHOULD include the link name and other
	// available information on the initiator of the request (such as the remote
	// container-id) in some recognizable form for ease of traceability.
	Dynamic bool

	// properties of the dynamically created node
	//
	// If the dynamic field is not set to true this field MUST be left unset.
	//
	// When set by the receiving link endpoint, this field contains the desired
	// properties of the node the receiver wishes to be created. When set by the
	// sending link endpoint this field contains the actual properties of the
	// dynamically created node. See subsection 3.5.9 for standard node properties.
	// http://www.amqp.org/specification/1.0/node-properties
	//
	// lifetime-policy: The lifetime of a dynamically generated node.
	//					Definitionally, the lifetime will never be less than the lifetime
	//					of the link which caused its creation, however it is possible to
	//					extend the lifetime of dynamically created node using a lifetime
	//					policy. The value of this entry MUST be of a type which provides
	//					the lifetime-policy archetype. The following standard
	//					lifetime-policies are defined below: delete-on-close,
	//					delete-on-no-links, delete-on-no-messages or
	//					delete-on-no-links-or-messages.
	// supported-dist-modes: The distribution modes that the node supports.
	//					The value of this entry MUST be one or more symbols which are valid
	//					distribution-modes. That is, the value MUST be of the same type as
	//					would be valid in a field defined with the following attributes:
	//						type="symbol" multiple="true" requires="distribution-mode"
	DynamicNodeProperties map[symbol]interface{} // TODO: implement custom type with validation

	// the distribution mode of the link
	//
	// This field MUST be set by the sending end of the link if the endpoint supports more
	// than one distribution-mode. This field MAY be set by the receiving end of the link
	// to indicate a preference when a node supports multiple distribution modes.
	DistributionMode symbol

	// a set of predicates to filter the messages admitted onto the link
	//
	// The receiving endpoint sets its desired filter, the sending endpoint sets the filter
	// actually in place (including any filters defaulted at the node). The receiving
	// endpoint MUST check that the filter in place meets its needs and take responsibility
	// for detaching if it does not.
	Filter filter

	// default outcome for unsettled transfers
	//
	// Indicates the outcome to be used for transfers that have not reached a terminal
	// state at the receiver when the transfer is settled, including when the source
	// is destroyed. The value MUST be a valid outcome (e.g., released or rejected).
	DefaultOutcome interface{}

	// descriptors for the outcomes that can be chosen on this link
	//
	// The values in this field are the symbolic descriptors of the outcomes that can
	// be chosen on this link. This field MAY be empty, indicating that the default-outcome
	// will be assumed for all message transfers (if the default-outcome is not set, and no
	// outcomes are provided, then the accepted outcome MUST be supported by the source).
	//
	// When present, the values MUST be a symbolic descriptor of a valid outcome,
	// e.g., "amqp:accepted:list".
	Outcomes multiSymbol

	// the extension capabilities the sender supports/desires
	//
	// http://www.amqp.org/specification/1.0/source-capabilities
	Capabilities multiSymbol
}

func (s *source) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeSource, []marshalField{
		{value: &s.Address, omit: s.Address == ""},
		{value: &s.Durable, omit: s.Durable == DurabilityNone},
		{value: &s.ExpiryPolicy, omit: s.ExpiryPolicy == "" || s.ExpiryPolicy == ExpirySessionEnd},
		{value: &s.Timeout, omit: s.Timeout == 0},
		{value: &s.Dynamic, omit: !s.Dynamic},
		{value: s.DynamicNodeProperties, omit: len(s.DynamicNodeProperties) == 0},
		{value: &s.DistributionMode, omit: s.DistributionMode == ""},
		{value: s.Filter, omit: len(s.Filter) == 0},
		{value: &s.DefaultOutcome, omit: s.DefaultOutcome == nil},
		{value: &s.Outcomes, omit: len(s.Outcomes) == 0},
		{value: &s.Capabilities, omit: len(s.Capabilities) == 0},
	})
}

func (s *source) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeSource, []unmarshalField{
		{field: &s.Address},
		{field: &s.Durable},
		{field: &s.ExpiryPolicy, handleNull: func() error { s.ExpiryPolicy = ExpirySessionEnd; return nil }},
		{field: &s.Timeout},
		{field: &s.Dynamic},
		{field: &s.DynamicNodeProperties},
		{field: &s.DistributionMode},
		{field: &s.Filter},
		{field: &s.DefaultOutcome},
		{field: &s.Outcomes},
		{field: &s.Capabilities},
	}...)
}

func (s source) String() string {
	return fmt.Sprintf("source{Address: %s, Durable: %d, ExpiryPolicy: %s, Timeout: %d, "+
		"Dynamic: %t, DynamicNodeProperties: %v, DistributionMode: %s, Filter: %v, DefaultOutcome: %v"+
		"Outcomes: %v, Capabilities: %v}",
		s.Address,
		s.Durable,
		s.ExpiryPolicy,
		s.Timeout,
		s.Dynamic,
		s.DynamicNodeProperties,
		s.DistributionMode,
		s.Filter,
		s.DefaultOutcome,
		s.Outcomes,
		s.Capabilities,
	)
}

/*
<type name="target" class="composite" source="list" provides="target">
    <descriptor name="amqp:target:list" code="0x00000000:0x00000029"/>
    <field name="address" type="*" requires="address"/>
    <field name="durable" type="terminus-durability" default="none"/>
    <field name="expiry-policy" type="terminus-expiry-policy" default="session-end"/>
    <field name="timeout" type="seconds" default="0"/>
    <field name="dynamic" type="boolean" default="false"/>
    <field name="dynamic-node-properties" type="node-properties"/>
    <field name="capabilities" type="symbol" multiple="true"/>
</type>
*/
type target struct {
	// the address of the target
	//
	// The address of the target MUST NOT be set when sent on a attach frame sent by
	// the sending link endpoint where the dynamic flag is set to true (that is where
	// the sender is requesting the receiver to create an addressable node).
	//
	// The address of the source MUST be set when sent on a attach frame sent by the
	// receiving link endpoint where the dynamic flag is set to true (that is where
	// the receiver has created an addressable node at the request of the sender and
	// is now communicating the address of that created node). The generated name of
	// the address SHOULD include the link name and the container-id of the remote
	// container to allow for ease of identification.
	Address string

	// indicates the durability of the terminus
	//
	// Indicates what state of the terminus will be retained durably: the state of durable
	// messages, only existence and configuration of the terminus, or no state at all.
	//
	// 0: none
	// 1: configuration
	// 2: unsettled-state
	Durable Durability

	// the expiry policy of the target
	//
	// link-detach: The expiry timer starts when terminus is detached.
	// session-end: The expiry timer starts when the most recently associated session is
	//              ended.
	// connection-close: The expiry timer starts when most recently associated connection
	//                   is closed.
	// never: The terminus never expires.
	ExpiryPolicy ExpiryPolicy

	// duration that an expiring target will be retained
	//
	// The target starts expiring as indicated by the expiry-policy.
	Timeout uint32 // seconds

	// request dynamic creation of a remote node
	//
	// When set to true by the sending link endpoint, this field constitutes a request
	// for the receiving peer to dynamically create a node at the target. In this case
	// the address field MUST NOT be set.
	//
	// When set to true by the receiving link endpoint this field indicates creation of
	// a dynamically created node. In this case the address field will contain the
	// address of the created node. The generated address SHOULD include the link name
	// and other available information on the initiator of the request (such as the
	// remote container-id) in some recognizable form for ease of traceability.
	Dynamic bool

	// properties of the dynamically created node
	//
	// If the dynamic field is not set to true this field MUST be left unset.
	//
	// When set by the sending link endpoint, this field contains the desired
	// properties of the node the sender wishes to be created. When set by the
	// receiving link endpoint this field contains the actual properties of the
	// dynamically created node. See subsection 3.5.9 for standard node properties.
	// http://www.amqp.org/specification/1.0/node-properties
	//
	// lifetime-policy: The lifetime of a dynamically generated node.
	//					Definitionally, the lifetime will never be less than the lifetime
	//					of the link which caused its creation, however it is possible to
	//					extend the lifetime of dynamically created node using a lifetime
	//					policy. The value of this entry MUST be of a type which provides
	//					the lifetime-policy archetype. The following standard
	//					lifetime-policies are defined below: delete-on-close,
	//					delete-on-no-links, delete-on-no-messages or
	//					delete-on-no-links-or-messages.
	// supported-dist-modes: The distribution modes that the node supports.
	//					The value of this entry MUST be one or more symbols which are valid
	//					distribution-modes. That is, the value MUST be of the same type as
	//					would be valid in a field defined with the following attributes:
	//						type="symbol" multiple="true" requires="distribution-mode"
	DynamicNodeProperties map[symbol]interface{} // TODO: implement custom type with validation

	// the extension capabilities the sender supports/desires
	//
	// http://www.amqp.org/specification/1.0/target-capabilities
	Capabilities multiSymbol
}

func (t *target) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeTarget, []marshalField{
		{value: &t.Address, omit: t.Address == ""},
		{value: &t.Durable, omit: t.Durable == DurabilityNone},
		{value: &t.ExpiryPolicy, omit: t.ExpiryPolicy == "" || t.ExpiryPolicy == ExpirySessionEnd},
		{value: &t.Timeout, omit: t.Timeout == 0},
		{value: &t.Dynamic, omit: !t.Dynamic},
		{value: t.DynamicNodeProperties, omit: len(t.DynamicNodeProperties) == 0},
		{value: &t.Capabilities, omit: len(t.Capabilities) == 0},
	})
}

func (t *target) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeTarget, []unmarshalField{
		{field: &t.Address},
		{field: &t.Durable},
		{field: &t.ExpiryPolicy, handleNull: func() error { t.ExpiryPolicy = ExpirySessionEnd; return nil }},
		{field: &t.Timeout},
		{field: &t.Dynamic},
		{field: &t.DynamicNodeProperties},
		{field: &t.Capabilities},
	}...)
}

func (t target) String() string {
	return fmt.Sprintf("source{Address: %s, Durable: %d, ExpiryPolicy: %s, Timeout: %d, "+
		"Dynamic: %t, DynamicNodeProperties: %v, Capabilities: %v}",
		t.Address,
		t.Durable,
		t.ExpiryPolicy,
		t.Timeout,
		t.Dynamic,
		t.DynamicNodeProperties,
		t.Capabilities,
	)
}

const maxDeliveryTagLength = 32

// peekMessageType reads the message type without
// modifying any data.
func peekMessageType(buf []byte) (uint8, error) {
	if len(buf) < 3 {
		return 0, errors.New("invalid message")
	}

	if buf[0] != 0 {
		return 0, fmt.Errorf("invalid composite header %02x", buf[0])
	}

	// copied from readUlong to avoid allocations
	t := amqpType(buf[1])
	if t == typeCodeUlong0 {
		return 0, nil
	}

	if t == typeCodeSmallUlong {
		if len(buf[2:]) == 0 {
			return 0, errors.New("invalid ulong")
		}
		return buf[2], nil
	}

	if t != typeCodeUlong {
		return 0, fmt.Errorf("invalid type for uint32 %02x", t)
	}

	if len(buf[2:]) < 8 {
		return 0, errors.New("invalid ulong")
	}
	v := binary.BigEndian.Uint64(buf[2:10])

	return uint8(v), nil
}

func tryReadNull(r *buffer.Buffer) bool {
	if r.Len() > 0 && amqpType(r.Bytes()[0]) == typeCodeNull {
		r.Skip(1)
		return true
	}
	return false
}

// Annotations keys must be of type string, int, or int64.
//
// String keys are encoded as AMQP Symbols.
type Annotations map[interface{}]interface{}

func (a Annotations) marshal(wr *buffer.Buffer) error {
	return writeMap(wr, a)
}

func (a *Annotations) unmarshal(r *buffer.Buffer) error {
	count, err := readMapHeader(r)
	if err != nil {
		return err
	}

	m := make(Annotations, count/2)
	for i := uint32(0); i < count; i += 2 {
		key, err := readAny(r)
		if err != nil {
			return err
		}
		value, err := readAny(r)
		if err != nil {
			return err
		}
		m[key] = value
	}
	*a = m
	return nil
}

/*
<type name="received" class="composite" source="list" provides="delivery-state">
    <descriptor name="amqp:received:list" code="0x00000000:0x00000023"/>
    <field name="section-number" type="uint" mandatory="true"/>
    <field name="section-offset" type="ulong" mandatory="true"/>
</type>
*/

type stateReceived struct {
	// When sent by the sender this indicates the first section of the message
	// (with section-number 0 being the first section) for which data can be resent.
	// Data from sections prior to the given section cannot be retransmitted for
	// this delivery.
	//
	// When sent by the receiver this indicates the first section of the message
	// for which all data might not yet have been received.
	SectionNumber uint32

	// When sent by the sender this indicates the first byte of the encoded section
	// data of the section given by section-number for which data can be resent
	// (with section-offset 0 being the first byte). Bytes from the same section
	// prior to the given offset section cannot be retransmitted for this delivery.
	//
	// When sent by the receiver this indicates the first byte of the given section
	// which has not yet been received. Note that if a receiver has received all of
	// section number X (which contains N bytes of data), but none of section number
	// X + 1, then it can indicate this by sending either Received(section-number=X,
	// section-offset=N) or Received(section-number=X+1, section-offset=0). The state
	// Received(section-number=0, section-offset=0) indicates that no message data
	// at all has been transferred.
	SectionOffset uint64
}

func (sr *stateReceived) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeStateReceived, []marshalField{
		{value: &sr.SectionNumber, omit: false},
		{value: &sr.SectionOffset, omit: false},
	})
}

func (sr *stateReceived) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeStateReceived, []unmarshalField{
		{field: &sr.SectionNumber, handleNull: func() error { return errors.New("StateReceiver.SectionNumber is required") }},
		{field: &sr.SectionOffset, handleNull: func() error { return errors.New("StateReceiver.SectionOffset is required") }},
	}...)
}

/*
<type name="accepted" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:accepted:list" code="0x00000000:0x00000024"/>
</type>
*/

type stateAccepted struct{}

func (sa *stateAccepted) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeStateAccepted, nil)
}

func (sa *stateAccepted) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeStateAccepted)
}

func (sa *stateAccepted) String() string {
	return "Accepted"
}

/*
<type name="rejected" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:rejected:list" code="0x00000000:0x00000025"/>
    <field name="error" type="error"/>
</type>
*/

type stateRejected struct {
	Error *Error
}

func (sr *stateRejected) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeStateRejected, []marshalField{
		{value: sr.Error, omit: sr.Error == nil},
	})
}

func (sr *stateRejected) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeStateRejected,
		unmarshalField{field: &sr.Error},
	)
}

func (sr *stateRejected) String() string {
	return fmt.Sprintf("Rejected{Error: %v}", sr.Error)
}

/*
<type name="released" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:released:list" code="0x00000000:0x00000026"/>
</type>
*/

type stateReleased struct{}

func (sr *stateReleased) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeStateReleased, nil)
}

func (sr *stateReleased) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeStateReleased)
}

func (sr *stateReleased) String() string {
	return "Released"
}

/*
<type name="modified" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:modified:list" code="0x00000000:0x00000027"/>
    <field name="delivery-failed" type="boolean"/>
    <field name="undeliverable-here" type="boolean"/>
    <field name="message-annotations" type="fields"/>
</type>
*/

type stateModified struct {
	// count the transfer as an unsuccessful delivery attempt
	//
	// If the delivery-failed flag is set, any messages modified
	// MUST have their delivery-count incremented.
	DeliveryFailed bool

	// prevent redelivery
	//
	// If the undeliverable-here is set, then any messages released MUST NOT
	// be redelivered to the modifying link endpoint.
	UndeliverableHere bool

	// message attributes
	// Map containing attributes to combine with the existing message-annotations
	// held in the message's header section. Where the existing message-annotations
	// of the message contain an entry with the same key as an entry in this field,
	// the value in this field associated with that key replaces the one in the
	// existing headers; where the existing message-annotations has no such value,
	// the value in this map is added.
	MessageAnnotations Annotations
}

func (sm *stateModified) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeStateModified, []marshalField{
		{value: &sm.DeliveryFailed, omit: !sm.DeliveryFailed},
		{value: &sm.UndeliverableHere, omit: !sm.UndeliverableHere},
		{value: sm.MessageAnnotations, omit: sm.MessageAnnotations == nil},
	})
}

func (sm *stateModified) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeStateModified, []unmarshalField{
		{field: &sm.DeliveryFailed},
		{field: &sm.UndeliverableHere},
		{field: &sm.MessageAnnotations},
	}...)
}

func (sm *stateModified) String() string {
	return fmt.Sprintf("Modified{DeliveryFailed: %t, UndeliverableHere: %t, MessageAnnotations: %v}", sm.DeliveryFailed, sm.UndeliverableHere, sm.MessageAnnotations)
}

// symbol is an AMQP symbolic string.
type symbol string

func (s symbol) marshal(wr *buffer.Buffer) error {
	l := len(s)
	switch {
	// Sym8
	case l < 256:
		wr.Append([]byte{
			byte(typeCodeSym8),
			byte(l),
		})
		wr.AppendString(string(s))

	// Sym32
	case uint(l) < math.MaxUint32:
		wr.AppendByte(uint8(typeCodeSym32))
		wr.AppendUint32(uint32(l))
		wr.AppendString(string(s))
	default:
		return errors.New("too long")
	}
	return nil
}

type milliseconds time.Duration

func (m milliseconds) marshal(wr *buffer.Buffer) error {
	writeUint32(wr, uint32(m/milliseconds(time.Millisecond)))
	return nil
}

func (m *milliseconds) unmarshal(r *buffer.Buffer) error {
	n, err := readUint(r)
	*m = milliseconds(time.Duration(n) * time.Millisecond)
	return err
}

// mapAnyAny is used to decode AMQP maps who's keys are undefined or
// inconsistently typed.
type mapAnyAny map[interface{}]interface{}

func (m mapAnyAny) marshal(wr *buffer.Buffer) error {
	return writeMap(wr, map[interface{}]interface{}(m))
}

func (m *mapAnyAny) unmarshal(r *buffer.Buffer) error {
	count, err := readMapHeader(r)
	if err != nil {
		return err
	}

	mm := make(mapAnyAny, count/2)
	for i := uint32(0); i < count; i += 2 {
		key, err := readAny(r)
		if err != nil {
			return err
		}
		value, err := readAny(r)
		if err != nil {
			return err
		}

		// https://golang.org/ref/spec#Map_types:
		// The comparison operators == and != must be fully defined
		// for operands of the key type; thus the key type must not
		// be a function, map, or slice.
		switch reflect.ValueOf(key).Kind() {
		case reflect.Slice, reflect.Func, reflect.Map:
			return errors.New("invalid map key")
		}

		mm[key] = value
	}
	*m = mm
	return nil
}

// mapStringAny is used to decode AMQP maps that have string keys
type mapStringAny map[string]interface{}

func (m mapStringAny) marshal(wr *buffer.Buffer) error {
	return writeMap(wr, map[string]interface{}(m))
}

func (m *mapStringAny) unmarshal(r *buffer.Buffer) error {
	count, err := readMapHeader(r)
	if err != nil {
		return err
	}

	mm := make(mapStringAny, count/2)
	for i := uint32(0); i < count; i += 2 {
		key, err := readString(r)
		if err != nil {
			return err
		}
		value, err := readAny(r)
		if err != nil {
			return err
		}
		mm[key] = value
	}
	*m = mm

	return nil
}

// mapStringAny is used to decode AMQP maps that have Symbol keys
type mapSymbolAny map[symbol]interface{}

func (m mapSymbolAny) marshal(wr *buffer.Buffer) error {
	return writeMap(wr, map[symbol]interface{}(m))
}

func (m *mapSymbolAny) unmarshal(r *buffer.Buffer) error {
	count, err := readMapHeader(r)
	if err != nil {
		return err
	}

	mm := make(mapSymbolAny, count/2)
	for i := uint32(0); i < count; i += 2 {
		key, err := readString(r)
		if err != nil {
			return err
		}
		value, err := readAny(r)
		if err != nil {
			return err
		}
		mm[symbol(key)] = value
	}
	*m = mm
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

func (u UUID) marshal(wr *buffer.Buffer) error {
	wr.AppendByte(byte(typeCodeUUID))
	wr.Append(u[:])
	return nil
}

func (u *UUID) unmarshal(r *buffer.Buffer) error {
	un, err := readUUID(r)
	*u = un
	return err
}

type lifetimePolicy uint8

const (
	deleteOnClose             = lifetimePolicy(typeCodeDeleteOnClose)
	deleteOnNoLinks           = lifetimePolicy(typeCodeDeleteOnNoLinks)
	deleteOnNoMessages        = lifetimePolicy(typeCodeDeleteOnNoMessages)
	deleteOnNoLinksOrMessages = lifetimePolicy(typeCodeDeleteOnNoLinksOrMessages)
)

func (p lifetimePolicy) marshal(wr *buffer.Buffer) error {
	wr.Append([]byte{
		0x0,
		byte(typeCodeSmallUlong),
		byte(p),
		byte(typeCodeList0),
	})
	return nil
}

func (p *lifetimePolicy) unmarshal(r *buffer.Buffer) error {
	typ, fields, err := readCompositeHeader(r)
	if err != nil {
		return err
	}
	if fields != 0 {
		return fmt.Errorf("invalid size %d for lifetime-policy", fields)
	}
	*p = lifetimePolicy(typ)
	return nil
}

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

type describedType struct {
	descriptor interface{}
	value      interface{}
}

func (t describedType) marshal(wr *buffer.Buffer) error {
	wr.AppendByte(0x0) // descriptor constructor
	err := marshal(wr, t.descriptor)
	if err != nil {
		return err
	}
	return marshal(wr, t.value)
}

func (t *describedType) unmarshal(r *buffer.Buffer) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}

	if b != 0x0 {
		return fmt.Errorf("invalid described type header %02x", b)
	}

	err = unmarshal(r, &t.descriptor)
	if err != nil {
		return err
	}
	return unmarshal(r, &t.value)
}

func (t describedType) String() string {
	return fmt.Sprintf("describedType{descriptor: %v, value: %v}",
		t.descriptor,
		t.value,
	)
}

// SLICES

// ArrayUByte allows encoding []uint8/[]byte as an array
// rather than binary data.
type ArrayUByte []uint8

func (a ArrayUByte) marshal(wr *buffer.Buffer) error {
	const typeSize = 1

	writeArrayHeader(wr, len(a), typeSize, typeCodeUbyte)
	wr.Append(a)

	return nil
}

func (a *ArrayUByte) unmarshal(r *buffer.Buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	type_, err := readType(r)
	if err != nil {
		return err
	}
	if type_ != typeCodeUbyte {
		return fmt.Errorf("invalid type for []uint16 %02x", type_)
	}

	buf, ok := r.Next(length)
	if !ok {
		return fmt.Errorf("invalid length %d", length)
	}
	*a = append([]byte(nil), buf...)

	return nil
}

type arrayInt8 []int8

func (a arrayInt8) marshal(wr *buffer.Buffer) error {
	const typeSize = 1

	writeArrayHeader(wr, len(a), typeSize, typeCodeByte)

	for _, value := range a {
		wr.AppendByte(uint8(value))
	}

	return nil
}

func (a *arrayInt8) unmarshal(r *buffer.Buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	type_, err := readType(r)
	if err != nil {
		return err
	}
	if type_ != typeCodeByte {
		return fmt.Errorf("invalid type for []uint16 %02x", type_)
	}

	buf, ok := r.Next(length)
	if !ok {
		return fmt.Errorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]int8, length)
	} else {
		aa = aa[:length]
	}

	for i, value := range buf {
		aa[i] = int8(value)
	}

	*a = aa
	return nil
}

type arrayUint16 []uint16

func (a arrayUint16) marshal(wr *buffer.Buffer) error {
	const typeSize = 2

	writeArrayHeader(wr, len(a), typeSize, typeCodeUshort)

	for _, element := range a {
		wr.AppendUint16(element)
	}

	return nil
}

func (a *arrayUint16) unmarshal(r *buffer.Buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	type_, err := readType(r)
	if err != nil {
		return err
	}
	if type_ != typeCodeUshort {
		return fmt.Errorf("invalid type for []uint16 %02x", type_)
	}

	const typeSize = 2
	buf, ok := r.Next(length * typeSize)
	if !ok {
		return fmt.Errorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]uint16, length)
	} else {
		aa = aa[:length]
	}

	var bufIdx int
	for i := range aa {
		aa[i] = binary.BigEndian.Uint16(buf[bufIdx:])
		bufIdx += 2
	}

	*a = aa
	return nil
}

type arrayInt16 []int16

func (a arrayInt16) marshal(wr *buffer.Buffer) error {
	const typeSize = 2

	writeArrayHeader(wr, len(a), typeSize, typeCodeShort)

	for _, element := range a {
		wr.AppendUint16(uint16(element))
	}

	return nil
}

func (a *arrayInt16) unmarshal(r *buffer.Buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	type_, err := readType(r)
	if err != nil {
		return err
	}
	if type_ != typeCodeShort {
		return fmt.Errorf("invalid type for []uint16 %02x", type_)
	}

	const typeSize = 2
	buf, ok := r.Next(length * typeSize)
	if !ok {
		return fmt.Errorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]int16, length)
	} else {
		aa = aa[:length]
	}

	var bufIdx int
	for i := range aa {
		aa[i] = int16(binary.BigEndian.Uint16(buf[bufIdx : bufIdx+2]))
		bufIdx += 2
	}

	*a = aa
	return nil
}

type arrayUint32 []uint32

func (a arrayUint32) marshal(wr *buffer.Buffer) error {
	var (
		typeSize = 1
		typeCode = typeCodeSmallUint
	)
	for _, n := range a {
		if n > math.MaxUint8 {
			typeSize = 4
			typeCode = typeCodeUint
			break
		}
	}

	writeArrayHeader(wr, len(a), typeSize, typeCode)

	if typeCode == typeCodeUint {
		for _, element := range a {
			wr.AppendUint32(element)
		}
	} else {
		for _, element := range a {
			wr.AppendByte(byte(element))
		}
	}

	return nil
}

func (a *arrayUint32) unmarshal(r *buffer.Buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	aa := (*a)[:0]

	type_, err := readType(r)
	if err != nil {
		return err
	}
	switch type_ {
	case typeCodeUint0:
		if int64(cap(aa)) < length {
			aa = make([]uint32, length)
		} else {
			aa = aa[:length]
			for i := range aa {
				aa[i] = 0
			}
		}
	case typeCodeSmallUint:
		buf, ok := r.Next(length)
		if !ok {
			return errors.New("invalid length")
		}

		if int64(cap(aa)) < length {
			aa = make([]uint32, length)
		} else {
			aa = aa[:length]
		}

		for i, n := range buf {
			aa[i] = uint32(n)
		}
	case typeCodeUint:
		const typeSize = 4
		buf, ok := r.Next(length * typeSize)
		if !ok {
			return fmt.Errorf("invalid length %d", length)
		}

		if int64(cap(aa)) < length {
			aa = make([]uint32, length)
		} else {
			aa = aa[:length]
		}

		var bufIdx int
		for i := range aa {
			aa[i] = binary.BigEndian.Uint32(buf[bufIdx : bufIdx+4])
			bufIdx += 4
		}
	default:
		return fmt.Errorf("invalid type for []uint32 %02x", type_)
	}

	*a = aa
	return nil
}

type arrayInt32 []int32

func (a arrayInt32) marshal(wr *buffer.Buffer) error {
	var (
		typeSize = 1
		typeCode = typeCodeSmallint
	)
	for _, n := range a {
		if n > math.MaxInt8 {
			typeSize = 4
			typeCode = typeCodeInt
			break
		}
	}

	writeArrayHeader(wr, len(a), typeSize, typeCode)

	if typeCode == typeCodeInt {
		for _, element := range a {
			wr.AppendUint32(uint32(element))
		}
	} else {
		for _, element := range a {
			wr.AppendByte(byte(element))
		}
	}

	return nil
}

func (a *arrayInt32) unmarshal(r *buffer.Buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	aa := (*a)[:0]

	type_, err := readType(r)
	if err != nil {
		return err
	}
	switch type_ {
	case typeCodeSmallint:
		buf, ok := r.Next(length)
		if !ok {
			return errors.New("invalid length")
		}

		if int64(cap(aa)) < length {
			aa = make([]int32, length)
		} else {
			aa = aa[:length]
		}

		for i, n := range buf {
			aa[i] = int32(int8(n))
		}
	case typeCodeInt:
		const typeSize = 4
		buf, ok := r.Next(length * typeSize)
		if !ok {
			return fmt.Errorf("invalid length %d", length)
		}

		if int64(cap(aa)) < length {
			aa = make([]int32, length)
		} else {
			aa = aa[:length]
		}

		var bufIdx int
		for i := range aa {
			aa[i] = int32(binary.BigEndian.Uint32(buf[bufIdx:]))
			bufIdx += 4
		}
	default:
		return fmt.Errorf("invalid type for []int32 %02x", type_)
	}

	*a = aa
	return nil
}

type arrayUint64 []uint64

func (a arrayUint64) marshal(wr *buffer.Buffer) error {
	var (
		typeSize = 1
		typeCode = typeCodeSmallUlong
	)
	for _, n := range a {
		if n > math.MaxUint8 {
			typeSize = 8
			typeCode = typeCodeUlong
			break
		}
	}

	writeArrayHeader(wr, len(a), typeSize, typeCode)

	if typeCode == typeCodeUlong {
		for _, element := range a {
			wr.AppendUint64(element)
		}
	} else {
		for _, element := range a {
			wr.AppendByte(byte(element))
		}
	}

	return nil
}

func (a *arrayUint64) unmarshal(r *buffer.Buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	aa := (*a)[:0]

	type_, err := readType(r)
	if err != nil {
		return err
	}
	switch type_ {
	case typeCodeUlong0:
		if int64(cap(aa)) < length {
			aa = make([]uint64, length)
		} else {
			aa = aa[:length]
			for i := range aa {
				aa[i] = 0
			}
		}
	case typeCodeSmallUlong:
		buf, ok := r.Next(length)
		if !ok {
			return errors.New("invalid length")
		}

		if int64(cap(aa)) < length {
			aa = make([]uint64, length)
		} else {
			aa = aa[:length]
		}

		for i, n := range buf {
			aa[i] = uint64(n)
		}
	case typeCodeUlong:
		const typeSize = 8
		buf, ok := r.Next(length * typeSize)
		if !ok {
			return errors.New("invalid length")
		}

		if int64(cap(aa)) < length {
			aa = make([]uint64, length)
		} else {
			aa = aa[:length]
		}

		var bufIdx int
		for i := range aa {
			aa[i] = binary.BigEndian.Uint64(buf[bufIdx : bufIdx+8])
			bufIdx += 8
		}
	default:
		return fmt.Errorf("invalid type for []uint64 %02x", type_)
	}

	*a = aa
	return nil
}

type arrayInt64 []int64

func (a arrayInt64) marshal(wr *buffer.Buffer) error {
	var (
		typeSize = 1
		typeCode = typeCodeSmalllong
	)
	for _, n := range a {
		if n > math.MaxInt8 {
			typeSize = 8
			typeCode = typeCodeLong
			break
		}
	}

	writeArrayHeader(wr, len(a), typeSize, typeCode)

	if typeCode == typeCodeLong {
		for _, element := range a {
			wr.AppendUint64(uint64(element))
		}
	} else {
		for _, element := range a {
			wr.AppendByte(byte(element))
		}
	}

	return nil
}

func (a *arrayInt64) unmarshal(r *buffer.Buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	aa := (*a)[:0]

	type_, err := readType(r)
	if err != nil {
		return err
	}
	switch type_ {
	case typeCodeSmalllong:
		buf, ok := r.Next(length)
		if !ok {
			return errors.New("invalid length")
		}

		if int64(cap(aa)) < length {
			aa = make([]int64, length)
		} else {
			aa = aa[:length]
		}

		for i, n := range buf {
			aa[i] = int64(int8(n))
		}
	case typeCodeLong:
		const typeSize = 8
		buf, ok := r.Next(length * typeSize)
		if !ok {
			return errors.New("invalid length")
		}

		if int64(cap(aa)) < length {
			aa = make([]int64, length)
		} else {
			aa = aa[:length]
		}

		var bufIdx int
		for i := range aa {
			aa[i] = int64(binary.BigEndian.Uint64(buf[bufIdx:]))
			bufIdx += 8
		}
	default:
		return fmt.Errorf("invalid type for []uint64 %02x", type_)
	}

	*a = aa
	return nil
}

type arrayFloat []float32

func (a arrayFloat) marshal(wr *buffer.Buffer) error {
	const typeSize = 4

	writeArrayHeader(wr, len(a), typeSize, typeCodeFloat)

	for _, element := range a {
		wr.AppendUint32(math.Float32bits(element))
	}

	return nil
}

func (a *arrayFloat) unmarshal(r *buffer.Buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	type_, err := readType(r)
	if err != nil {
		return err
	}
	if type_ != typeCodeFloat {
		return fmt.Errorf("invalid type for []float32 %02x", type_)
	}

	const typeSize = 4
	buf, ok := r.Next(length * typeSize)
	if !ok {
		return fmt.Errorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]float32, length)
	} else {
		aa = aa[:length]
	}

	var bufIdx int
	for i := range aa {
		bits := binary.BigEndian.Uint32(buf[bufIdx:])
		aa[i] = math.Float32frombits(bits)
		bufIdx += typeSize
	}

	*a = aa
	return nil
}

type arrayDouble []float64

func (a arrayDouble) marshal(wr *buffer.Buffer) error {
	const typeSize = 8

	writeArrayHeader(wr, len(a), typeSize, typeCodeDouble)

	for _, element := range a {
		wr.AppendUint64(math.Float64bits(element))
	}

	return nil
}

func (a *arrayDouble) unmarshal(r *buffer.Buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	type_, err := readType(r)
	if err != nil {
		return err
	}
	if type_ != typeCodeDouble {
		return fmt.Errorf("invalid type for []float64 %02x", type_)
	}

	const typeSize = 8
	buf, ok := r.Next(length * typeSize)
	if !ok {
		return fmt.Errorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]float64, length)
	} else {
		aa = aa[:length]
	}

	var bufIdx int
	for i := range aa {
		bits := binary.BigEndian.Uint64(buf[bufIdx:])
		aa[i] = math.Float64frombits(bits)
		bufIdx += typeSize
	}

	*a = aa
	return nil
}

type arrayBool []bool

func (a arrayBool) marshal(wr *buffer.Buffer) error {
	const typeSize = 1

	writeArrayHeader(wr, len(a), typeSize, typeCodeBool)

	for _, element := range a {
		value := byte(0)
		if element {
			value = 1
		}
		wr.AppendByte(value)
	}

	return nil
}

func (a *arrayBool) unmarshal(r *buffer.Buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]bool, length)
	} else {
		aa = aa[:length]
	}

	type_, err := readType(r)
	if err != nil {
		return err
	}
	switch type_ {
	case typeCodeBool:
		buf, ok := r.Next(length)
		if !ok {
			return errors.New("invalid length")
		}

		for i, value := range buf {
			if value == 0 {
				aa[i] = false
			} else {
				aa[i] = true
			}
		}

	case typeCodeBoolTrue:
		for i := range aa {
			aa[i] = true
		}
	case typeCodeBoolFalse:
		for i := range aa {
			aa[i] = false
		}
	default:
		return fmt.Errorf("invalid type for []bool %02x", type_)
	}

	*a = aa
	return nil
}

type arrayString []string

func (a arrayString) marshal(wr *buffer.Buffer) error {
	var (
		elementType       = typeCodeStr8
		elementsSizeTotal int
	)
	for _, element := range a {
		if !utf8.ValidString(element) {
			return errors.New("not a valid UTF-8 string")
		}

		elementsSizeTotal += len(element)

		if len(element) > math.MaxUint8 {
			elementType = typeCodeStr32
		}
	}

	writeVariableArrayHeader(wr, len(a), elementsSizeTotal, elementType)

	if elementType == typeCodeStr32 {
		for _, element := range a {
			wr.AppendUint32(uint32(len(element)))
			wr.AppendString(element)
		}
	} else {
		for _, element := range a {
			wr.AppendByte(byte(len(element)))
			wr.AppendString(element)
		}
	}

	return nil
}

func (a *arrayString) unmarshal(r *buffer.Buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	const typeSize = 2 // assume all strings are at least 2 bytes
	if length*typeSize > int64(r.Len()) {
		return fmt.Errorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]string, length)
	} else {
		aa = aa[:length]
	}

	type_, err := readType(r)
	if err != nil {
		return err
	}
	switch type_ {
	case typeCodeStr8:
		for i := range aa {
			size, err := r.ReadByte()
			if err != nil {
				return err
			}

			buf, ok := r.Next(int64(size))
			if !ok {
				return errors.New("invalid length")
			}

			aa[i] = string(buf)
		}
	case typeCodeStr32:
		for i := range aa {
			buf, ok := r.Next(4)
			if !ok {
				return errors.New("invalid length")
			}
			size := int64(binary.BigEndian.Uint32(buf))

			buf, ok = r.Next(size)
			if !ok {
				return errors.New("invalid length")
			}
			aa[i] = string(buf)
		}
	default:
		return fmt.Errorf("invalid type for []string %02x", type_)
	}

	*a = aa
	return nil
}

type arraySymbol []symbol

func (a arraySymbol) marshal(wr *buffer.Buffer) error {
	var (
		elementType       = typeCodeSym8
		elementsSizeTotal int
	)
	for _, element := range a {
		elementsSizeTotal += len(element)

		if len(element) > math.MaxUint8 {
			elementType = typeCodeSym32
		}
	}

	writeVariableArrayHeader(wr, len(a), elementsSizeTotal, elementType)

	if elementType == typeCodeSym32 {
		for _, element := range a {
			wr.AppendUint32(uint32(len(element)))
			wr.AppendString(string(element))
		}
	} else {
		for _, element := range a {
			wr.AppendByte(byte(len(element)))
			wr.AppendString(string(element))
		}
	}

	return nil
}

func (a *arraySymbol) unmarshal(r *buffer.Buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	const typeSize = 2 // assume all symbols are at least 2 bytes
	if length*typeSize > int64(r.Len()) {
		return fmt.Errorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]symbol, length)
	} else {
		aa = aa[:length]
	}

	type_, err := readType(r)
	if err != nil {
		return err
	}
	switch type_ {
	case typeCodeSym8:
		for i := range aa {
			size, err := r.ReadByte()
			if err != nil {
				return err
			}

			buf, ok := r.Next(int64(size))
			if !ok {
				return errors.New("invalid length")
			}
			aa[i] = symbol(buf)
		}
	case typeCodeSym32:
		for i := range aa {
			buf, ok := r.Next(4)
			if !ok {
				return errors.New("invalid length")
			}
			size := int64(binary.BigEndian.Uint32(buf))

			buf, ok = r.Next(size)
			if !ok {
				return errors.New("invalid length")
			}
			aa[i] = symbol(buf)
		}
	default:
		return fmt.Errorf("invalid type for []symbol %02x", type_)
	}

	*a = aa
	return nil
}

type arrayBinary [][]byte

func (a arrayBinary) marshal(wr *buffer.Buffer) error {
	var (
		elementType       = typeCodeVbin8
		elementsSizeTotal int
	)
	for _, element := range a {
		elementsSizeTotal += len(element)

		if len(element) > math.MaxUint8 {
			elementType = typeCodeVbin32
		}
	}

	writeVariableArrayHeader(wr, len(a), elementsSizeTotal, elementType)

	if elementType == typeCodeVbin32 {
		for _, element := range a {
			wr.AppendUint32(uint32(len(element)))
			wr.Append(element)
		}
	} else {
		for _, element := range a {
			wr.AppendByte(byte(len(element)))
			wr.Append(element)
		}
	}

	return nil
}

func (a *arrayBinary) unmarshal(r *buffer.Buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	const typeSize = 2 // assume all binary is at least 2 bytes
	if length*typeSize > int64(r.Len()) {
		return fmt.Errorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([][]byte, length)
	} else {
		aa = aa[:length]
	}

	type_, err := readType(r)
	if err != nil {
		return err
	}
	switch type_ {
	case typeCodeVbin8:
		for i := range aa {
			size, err := r.ReadByte()
			if err != nil {
				return err
			}

			buf, ok := r.Next(int64(size))
			if !ok {
				return fmt.Errorf("invalid length %d", length)
			}
			aa[i] = append([]byte(nil), buf...)
		}
	case typeCodeVbin32:
		for i := range aa {
			buf, ok := r.Next(4)
			if !ok {
				return errors.New("invalid length")
			}
			size := binary.BigEndian.Uint32(buf)

			buf, ok = r.Next(int64(size))
			if !ok {
				return errors.New("invalid length")
			}
			aa[i] = append([]byte(nil), buf...)
		}
	default:
		return fmt.Errorf("invalid type for [][]byte %02x", type_)
	}

	*a = aa
	return nil
}

type arrayTimestamp []time.Time

func (a arrayTimestamp) marshal(wr *buffer.Buffer) error {
	const typeSize = 8

	writeArrayHeader(wr, len(a), typeSize, typeCodeTimestamp)

	for _, element := range a {
		ms := element.UnixNano() / int64(time.Millisecond)
		wr.AppendUint64(uint64(ms))
	}

	return nil
}

func (a *arrayTimestamp) unmarshal(r *buffer.Buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	type_, err := readType(r)
	if err != nil {
		return err
	}
	if type_ != typeCodeTimestamp {
		return fmt.Errorf("invalid type for []time.Time %02x", type_)
	}

	const typeSize = 8
	buf, ok := r.Next(length * typeSize)
	if !ok {
		return fmt.Errorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]time.Time, length)
	} else {
		aa = aa[:length]
	}

	var bufIdx int
	for i := range aa {
		ms := int64(binary.BigEndian.Uint64(buf[bufIdx:]))
		bufIdx += typeSize
		aa[i] = time.Unix(ms/1000, (ms%1000)*1000000).UTC()
	}

	*a = aa
	return nil
}

type arrayUUID []UUID

func (a arrayUUID) marshal(wr *buffer.Buffer) error {
	const typeSize = 16

	writeArrayHeader(wr, len(a), typeSize, typeCodeUUID)

	for _, element := range a {
		wr.Append(element[:])
	}

	return nil
}

func (a *arrayUUID) unmarshal(r *buffer.Buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	type_, err := readType(r)
	if err != nil {
		return err
	}
	if type_ != typeCodeUUID {
		return fmt.Errorf("invalid type for []UUID %#02x", type_)
	}

	const typeSize = 16
	buf, ok := r.Next(length * typeSize)
	if !ok {
		return fmt.Errorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]UUID, length)
	} else {
		aa = aa[:length]
	}

	var bufIdx int
	for i := range aa {
		copy(aa[i][:], buf[bufIdx:bufIdx+16])
		bufIdx += 16
	}

	*a = aa
	return nil
}

// LIST

type list []interface{}

func (l list) marshal(wr *buffer.Buffer) error {
	length := len(l)

	// type
	if length == 0 {
		wr.AppendByte(byte(typeCodeList0))
		return nil
	}
	wr.AppendByte(byte(typeCodeList32))

	// size
	sizeIdx := wr.Len()
	wr.Append([]byte{0, 0, 0, 0})

	// length
	wr.AppendUint32(uint32(length))

	for _, element := range l {
		err := marshal(wr, element)
		if err != nil {
			return err
		}
	}

	// overwrite size
	binary.BigEndian.PutUint32(wr.Bytes()[sizeIdx:], uint32(wr.Len()-(sizeIdx+4)))

	return nil
}

func (l *list) unmarshal(r *buffer.Buffer) error {
	length, err := readListHeader(r)
	if err != nil {
		return err
	}

	// assume that all types are at least 1 byte
	if length > int64(r.Len()) {
		return fmt.Errorf("invalid length %d", length)
	}

	ll := *l
	if int64(cap(ll)) < length {
		ll = make([]interface{}, length)
	} else {
		ll = ll[:length]
	}

	for i := range ll {
		ll[i], err = readAny(r)
		if err != nil {
			return err
		}
	}

	*l = ll
	return nil
}

// multiSymbol can decode a single symbol or an array.
type multiSymbol []symbol

func (ms multiSymbol) marshal(wr *buffer.Buffer) error {
	return marshal(wr, []symbol(ms))
}

func (ms *multiSymbol) unmarshal(r *buffer.Buffer) error {
	type_, err := peekType(r)
	if err != nil {
		return err
	}

	if type_ == typeCodeSym8 || type_ == typeCodeSym32 {
		s, err := readString(r)
		if err != nil {
			return err
		}

		*ms = []symbol{symbol(s)}
		return nil
	}

	return unmarshal(r, (*[]symbol)(ms))
}
