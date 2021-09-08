package amqp

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Azure/go-amqp/internal/buffer"
)

// frame is the decoded representation of a frame
type frame struct {
	type_   uint8     // AMQP/SASL
	channel uint16    // channel this frame is for
	body    frameBody // body of the frame

	// optional channel which will be closed after net transmit
	done chan deliveryState
}

// frameBody adds some type safety to frame encoding
type frameBody interface {
	frameBody()
}

/*
<type name="open" class="composite" source="list" provides="frame">
    <descriptor name="amqp:open:list" code="0x00000000:0x00000010"/>
    <field name="container-id" type="string" mandatory="true"/>
    <field name="hostname" type="string"/>
    <field name="max-frame-size" type="uint" default="4294967295"/>
    <field name="channel-max" type="ushort" default="65535"/>
    <field name="idle-time-out" type="milliseconds"/>
    <field name="outgoing-locales" type="ietf-language-tag" multiple="true"/>
    <field name="incoming-locales" type="ietf-language-tag" multiple="true"/>
    <field name="offered-capabilities" type="symbol" multiple="true"/>
    <field name="desired-capabilities" type="symbol" multiple="true"/>
    <field name="properties" type="fields"/>
</type>
*/

type performOpen struct {
	ContainerID         string // required
	Hostname            string
	MaxFrameSize        uint32        // default: 4294967295
	ChannelMax          uint16        // default: 65535
	IdleTimeout         time.Duration // from milliseconds
	OutgoingLocales     multiSymbol
	IncomingLocales     multiSymbol
	OfferedCapabilities multiSymbol
	DesiredCapabilities multiSymbol
	Properties          map[symbol]interface{}
}

func (o *performOpen) frameBody() {}

func (o *performOpen) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeOpen, []marshalField{
		{value: &o.ContainerID, omit: false},
		{value: &o.Hostname, omit: o.Hostname == ""},
		{value: &o.MaxFrameSize, omit: o.MaxFrameSize == 4294967295},
		{value: &o.ChannelMax, omit: o.ChannelMax == 65535},
		{value: (*milliseconds)(&o.IdleTimeout), omit: o.IdleTimeout == 0},
		{value: &o.OutgoingLocales, omit: len(o.OutgoingLocales) == 0},
		{value: &o.IncomingLocales, omit: len(o.IncomingLocales) == 0},
		{value: &o.OfferedCapabilities, omit: len(o.OfferedCapabilities) == 0},
		{value: &o.DesiredCapabilities, omit: len(o.DesiredCapabilities) == 0},
		{value: o.Properties, omit: len(o.Properties) == 0},
	})
}

func (o *performOpen) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeOpen, []unmarshalField{
		{field: &o.ContainerID, handleNull: func() error { return errors.New("Open.ContainerID is required") }},
		{field: &o.Hostname},
		{field: &o.MaxFrameSize, handleNull: func() error { o.MaxFrameSize = 4294967295; return nil }},
		{field: &o.ChannelMax, handleNull: func() error { o.ChannelMax = 65535; return nil }},
		{field: (*milliseconds)(&o.IdleTimeout)},
		{field: &o.OutgoingLocales},
		{field: &o.IncomingLocales},
		{field: &o.OfferedCapabilities},
		{field: &o.DesiredCapabilities},
		{field: &o.Properties},
	}...)
}

func (o *performOpen) String() string {
	return fmt.Sprintf("Open{ContainerID : %s, Hostname: %s, MaxFrameSize: %d, "+
		"ChannelMax: %d, IdleTimeout: %v, "+
		"OutgoingLocales: %v, IncomingLocales: %v, "+
		"OfferedCapabilities: %v, DesiredCapabilities: %v, "+
		"Properties: %v}",
		o.ContainerID,
		o.Hostname,
		o.MaxFrameSize,
		o.ChannelMax,
		o.IdleTimeout,
		o.OutgoingLocales,
		o.IncomingLocales,
		o.OfferedCapabilities,
		o.DesiredCapabilities,
		o.Properties,
	)
}

/*
<type name="begin" class="composite" source="list" provides="frame">
    <descriptor name="amqp:begin:list" code="0x00000000:0x00000011"/>
    <field name="remote-channel" type="ushort"/>
    <field name="next-outgoing-id" type="transfer-number" mandatory="true"/>
    <field name="incoming-window" type="uint" mandatory="true"/>
    <field name="outgoing-window" type="uint" mandatory="true"/>
    <field name="handle-max" type="handle" default="4294967295"/>
    <field name="offered-capabilities" type="symbol" multiple="true"/>
    <field name="desired-capabilities" type="symbol" multiple="true"/>
    <field name="properties" type="fields"/>
</type>
*/
type performBegin struct {
	// the remote channel for this session
	// If a session is locally initiated, the remote-channel MUST NOT be set.
	// When an endpoint responds to a remotely initiated session, the remote-channel
	// MUST be set to the channel on which the remote session sent the begin.
	RemoteChannel *uint16

	// the transfer-id of the first transfer id the sender will send
	NextOutgoingID uint32 // required, sequence number http://www.ietf.org/rfc/rfc1982.txt

	// the initial incoming-window of the sender
	IncomingWindow uint32 // required

	// the initial outgoing-window of the sender
	OutgoingWindow uint32 // required

	// the maximum handle value that can be used on the session
	// The handle-max value is the highest handle value that can be
	// used on the session. A peer MUST NOT attempt to attach a link
	// using a handle value outside the range that its partner can handle.
	// A peer that receives a handle outside the supported range MUST
	// close the connection with the framing-error error-code.
	HandleMax uint32 // default 4294967295

	// the extension capabilities the sender supports
	// http://www.amqp.org/specification/1.0/session-capabilities
	OfferedCapabilities multiSymbol

	// the extension capabilities the sender can use if the receiver supports them
	// The sender MUST NOT attempt to use any capability other than those it
	// has declared in desired-capabilities field.
	DesiredCapabilities multiSymbol

	// session properties
	// http://www.amqp.org/specification/1.0/session-properties
	Properties map[symbol]interface{}
}

func (b *performBegin) frameBody() {}

func (b *performBegin) String() string {
	return fmt.Sprintf("Begin{RemoteChannel: %v, NextOutgoingID: %d, IncomingWindow: %d, "+
		"OutgoingWindow: %d, HandleMax: %d, OfferedCapabilities: %v, DesiredCapabilities: %v, "+
		"Properties: %v}",
		formatUint16Ptr(b.RemoteChannel),
		b.NextOutgoingID,
		b.IncomingWindow,
		b.OutgoingWindow,
		b.HandleMax,
		b.OfferedCapabilities,
		b.DesiredCapabilities,
		b.Properties,
	)
}

func formatUint16Ptr(p *uint16) string {
	if p == nil {
		return "<nil>"
	}
	return strconv.FormatUint(uint64(*p), 10)
}

func (b *performBegin) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeBegin, []marshalField{
		{value: b.RemoteChannel, omit: b.RemoteChannel == nil},
		{value: &b.NextOutgoingID, omit: false},
		{value: &b.IncomingWindow, omit: false},
		{value: &b.OutgoingWindow, omit: false},
		{value: &b.HandleMax, omit: b.HandleMax == 4294967295},
		{value: &b.OfferedCapabilities, omit: len(b.OfferedCapabilities) == 0},
		{value: &b.DesiredCapabilities, omit: len(b.DesiredCapabilities) == 0},
		{value: b.Properties, omit: b.Properties == nil},
	})
}

func (b *performBegin) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeBegin, []unmarshalField{
		{field: &b.RemoteChannel},
		{field: &b.NextOutgoingID, handleNull: func() error { return errors.New("Begin.NextOutgoingID is required") }},
		{field: &b.IncomingWindow, handleNull: func() error { return errors.New("Begin.IncomingWindow is required") }},
		{field: &b.OutgoingWindow, handleNull: func() error { return errors.New("Begin.OutgoingWindow is required") }},
		{field: &b.HandleMax, handleNull: func() error { b.HandleMax = 4294967295; return nil }},
		{field: &b.OfferedCapabilities},
		{field: &b.DesiredCapabilities},
		{field: &b.Properties},
	}...)
}

/*
<type name="attach" class="composite" source="list" provides="frame">
    <descriptor name="amqp:attach:list" code="0x00000000:0x00000012"/>
    <field name="name" type="string" mandatory="true"/>
    <field name="handle" type="handle" mandatory="true"/>
    <field name="role" type="role" mandatory="true"/>
    <field name="snd-settle-mode" type="sender-settle-mode" default="mixed"/>
    <field name="rcv-settle-mode" type="receiver-settle-mode" default="first"/>
    <field name="source" type="*" requires="source"/>
    <field name="target" type="*" requires="target"/>
    <field name="unsettled" type="map"/>
    <field name="incomplete-unsettled" type="boolean" default="false"/>
    <field name="initial-delivery-count" type="sequence-no"/>
    <field name="max-message-size" type="ulong"/>
    <field name="offered-capabilities" type="symbol" multiple="true"/>
    <field name="desired-capabilities" type="symbol" multiple="true"/>
    <field name="properties" type="fields"/>
</type>
*/
type performAttach struct {
	// the name of the link
	//
	// This name uniquely identifies the link from the container of the source
	// to the container of the target node, e.g., if the container of the source
	// node is A, and the container of the target node is B, the link MAY be
	// globally identified by the (ordered) tuple (A,B,<name>).
	Name string // required

	// the handle for the link while attached
	//
	// The numeric handle assigned by the the peer as a shorthand to refer to the
	// link in all performatives that reference the link until the it is detached.
	//
	// The handle MUST NOT be used for other open links. An attempt to attach using
	// a handle which is already associated with a link MUST be responded to with
	// an immediate close carrying a handle-in-use session-error.
	//
	// To make it easier to monitor AMQP link attach frames, it is RECOMMENDED that
	// implementations always assign the lowest available handle to this field.
	//
	// The two endpoints MAY potentially use different handles to refer to the same link.
	// Link handles MAY be reused once a link is closed for both send and receive.
	Handle uint32 // required

	// role of the link endpoint
	//
	// The role being played by the peer, i.e., whether the peer is the sender or the
	// receiver of messages on the link.
	Role role

	// settlement policy for the sender
	//
	// The delivery settlement policy for the sender. When set at the receiver this
	// indicates the desired value for the settlement mode at the sender. When set
	// at the sender this indicates the actual settlement mode in use. The sender
	// SHOULD respect the receiver's desired settlement mode if the receiver initiates
	// the attach exchange and the sender supports the desired mode.
	//
	// 0: unsettled - The sender will send all deliveries initially unsettled to the receiver.
	// 1: settled - The sender will send all deliveries settled to the receiver.
	// 2: mixed - The sender MAY send a mixture of settled and unsettled deliveries to the receiver.
	SenderSettleMode *SenderSettleMode

	// the settlement policy of the receiver
	//
	// The delivery settlement policy for the receiver. When set at the sender this
	// indicates the desired value for the settlement mode at the receiver.
	// When set at the receiver this indicates the actual settlement mode in use.
	// The receiver SHOULD respect the sender's desired settlement mode if the sender
	// initiates the attach exchange and the receiver supports the desired mode.
	//
	// 0: first - The receiver will spontaneously settle all incoming transfers.
	// 1: second - The receiver will only settle after sending the disposition to
	//             the sender and receiving a disposition indicating settlement of
	//             the delivery from the sender.
	ReceiverSettleMode *ReceiverSettleMode

	// the source for messages
	//
	// If no source is specified on an outgoing link, then there is no source currently
	// attached to the link. A link with no source will never produce outgoing messages.
	Source *source

	// the target for messages
	//
	// If no target is specified on an incoming link, then there is no target currently
	// attached to the link. A link with no target will never permit incoming messages.
	Target *target

	// unsettled delivery state
	//
	// This is used to indicate any unsettled delivery states when a suspended link is
	// resumed. The map is keyed by delivery-tag with values indicating the delivery state.
	// The local and remote delivery states for a given delivery-tag MUST be compared to
	// resolve any in-doubt deliveries. If necessary, deliveries MAY be resent, or resumed
	// based on the outcome of this comparison. See subsection 2.6.13.
	//
	// If the local unsettled map is too large to be encoded within a frame of the agreed
	// maximum frame size then the session MAY be ended with the frame-size-too-small error.
	// The endpoint SHOULD make use of the ability to send an incomplete unsettled map
	// (see below) to avoid sending an error.
	//
	// The unsettled map MUST NOT contain null valued keys.
	//
	// When reattaching (as opposed to resuming), the unsettled map MUST be null.
	Unsettled unsettled

	// If set to true this field indicates that the unsettled map provided is not complete.
	// When the map is incomplete the recipient of the map cannot take the absence of a
	// delivery tag from the map as evidence of settlement. On receipt of an incomplete
	// unsettled map a sending endpoint MUST NOT send any new deliveries (i.e. deliveries
	// where resume is not set to true) to its partner (and a receiving endpoint which sent
	// an incomplete unsettled map MUST detach with an error on receiving a transfer which
	// does not have the resume flag set to true).
	//
	// Note that if this flag is set to true then the endpoints MUST detach and reattach at
	// least once in order to send new deliveries. This flag can be useful when there are
	// too many entries in the unsettled map to fit within a single frame. An endpoint can
	// attach, resume, settle, and detach until enough unsettled state has been cleared for
	// an attach where this flag is set to false.
	IncompleteUnsettled bool // default: false

	// the sender's initial value for delivery-count
	//
	// This MUST NOT be null if role is sender, and it is ignored if the role is receiver.
	InitialDeliveryCount uint32 // sequence number

	// the maximum message size supported by the link endpoint
	//
	// This field indicates the maximum message size supported by the link endpoint.
	// Any attempt to deliver a message larger than this results in a message-size-exceeded
	// link-error. If this field is zero or unset, there is no maximum size imposed by the
	// link endpoint.
	MaxMessageSize uint64

	// the extension capabilities the sender supports
	// http://www.amqp.org/specification/1.0/link-capabilities
	OfferedCapabilities multiSymbol

	// the extension capabilities the sender can use if the receiver supports them
	//
	// The sender MUST NOT attempt to use any capability other than those it
	// has declared in desired-capabilities field.
	DesiredCapabilities multiSymbol

	// link properties
	// http://www.amqp.org/specification/1.0/link-properties
	Properties map[symbol]interface{}
}

func (a *performAttach) frameBody() {}

func (a performAttach) String() string {
	return fmt.Sprintf("Attach{Name: %s, Handle: %d, Role: %s, SenderSettleMode: %s, ReceiverSettleMode: %s, "+
		"Source: %v, Target: %v, Unsettled: %v, IncompleteUnsettled: %t, InitialDeliveryCount: %d, MaxMessageSize: %d, "+
		"OfferedCapabilities: %v, DesiredCapabilities: %v, Properties: %v}",
		a.Name,
		a.Handle,
		a.Role,
		a.SenderSettleMode,
		a.ReceiverSettleMode,
		a.Source,
		a.Target,
		a.Unsettled,
		a.IncompleteUnsettled,
		a.InitialDeliveryCount,
		a.MaxMessageSize,
		a.OfferedCapabilities,
		a.DesiredCapabilities,
		a.Properties,
	)
}

func (a *performAttach) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeAttach, []marshalField{
		{value: &a.Name, omit: false},
		{value: &a.Handle, omit: false},
		{value: &a.Role, omit: false},
		{value: a.SenderSettleMode, omit: a.SenderSettleMode == nil},
		{value: a.ReceiverSettleMode, omit: a.ReceiverSettleMode == nil},
		{value: a.Source, omit: a.Source == nil},
		{value: a.Target, omit: a.Target == nil},
		{value: a.Unsettled, omit: len(a.Unsettled) == 0},
		{value: &a.IncompleteUnsettled, omit: !a.IncompleteUnsettled},
		{value: &a.InitialDeliveryCount, omit: a.Role == roleReceiver},
		{value: &a.MaxMessageSize, omit: a.MaxMessageSize == 0},
		{value: &a.OfferedCapabilities, omit: len(a.OfferedCapabilities) == 0},
		{value: &a.DesiredCapabilities, omit: len(a.DesiredCapabilities) == 0},
		{value: a.Properties, omit: len(a.Properties) == 0},
	})
}

func (a *performAttach) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeAttach, []unmarshalField{
		{field: &a.Name, handleNull: func() error { return errors.New("Attach.Name is required") }},
		{field: &a.Handle, handleNull: func() error { return errors.New("Attach.Handle is required") }},
		{field: &a.Role, handleNull: func() error { return errors.New("Attach.Role is required") }},
		{field: &a.SenderSettleMode},
		{field: &a.ReceiverSettleMode},
		{field: &a.Source},
		{field: &a.Target},
		{field: &a.Unsettled},
		{field: &a.IncompleteUnsettled},
		{field: &a.InitialDeliveryCount},
		{field: &a.MaxMessageSize},
		{field: &a.OfferedCapabilities},
		{field: &a.DesiredCapabilities},
		{field: &a.Properties},
	}...)
}

/*
<type name="flow" class="composite" source="list" provides="frame">
    <descriptor name="amqp:flow:list" code="0x00000000:0x00000013"/>
    <field name="next-incoming-id" type="transfer-number"/>
    <field name="incoming-window" type="uint" mandatory="true"/>
    <field name="next-outgoing-id" type="transfer-number" mandatory="true"/>
    <field name="outgoing-window" type="uint" mandatory="true"/>
    <field name="handle" type="handle"/>
    <field name="delivery-count" type="sequence-no"/>
    <field name="link-credit" type="uint"/>
    <field name="available" type="uint"/>
    <field name="drain" type="boolean" default="false"/>
    <field name="echo" type="boolean" default="false"/>
    <field name="properties" type="fields"/>
</type>
*/
type performFlow struct {
	// Identifies the expected transfer-id of the next incoming transfer frame.
	// This value MUST be set if the peer has received the begin frame for the
	// session, and MUST NOT be set if it has not. See subsection 2.5.6 for more details.
	NextIncomingID *uint32 // sequence number

	// Defines the maximum number of incoming transfer frames that the endpoint
	// can currently receive. See subsection 2.5.6 for more details.
	IncomingWindow uint32 // required

	// The transfer-id that will be assigned to the next outgoing transfer frame.
	// See subsection 2.5.6 for more details.
	NextOutgoingID uint32 // sequence number

	// Defines the maximum number of outgoing transfer frames that the endpoint
	// could potentially currently send, if it was not constrained by restrictions
	// imposed by its peer's incoming-window. See subsection 2.5.6 for more details.
	OutgoingWindow uint32

	// If set, indicates that the flow frame carries flow state information for the local
	// link endpoint associated with the given handle. If not set, the flow frame is
	// carrying only information pertaining to the session endpoint.
	//
	// If set to a handle that is not currently associated with an attached link,
	// the recipient MUST respond by ending the session with an unattached-handle
	// session error.
	Handle *uint32

	// The delivery-count is initialized by the sender when a link endpoint is created,
	// and is incremented whenever a message is sent. Only the sender MAY independently
	// modify this field. The receiver's value is calculated based on the last known
	// value from the sender and any subsequent messages received on the link. Note that,
	// despite its name, the delivery-count is not a count but a sequence number
	// initialized at an arbitrary point by the sender.
	//
	// When the handle field is not set, this field MUST NOT be set.
	//
	// When the handle identifies that the flow state is being sent from the sender link
	// endpoint to receiver link endpoint this field MUST be set to the current
	// delivery-count of the link endpoint.
	//
	// When the flow state is being sent from the receiver endpoint to the sender endpoint
	// this field MUST be set to the last known value of the corresponding sending endpoint.
	// In the event that the receiving link endpoint has not yet seen the initial attach
	// frame from the sender this field MUST NOT be set.
	DeliveryCount *uint32 // sequence number

	// the current maximum number of messages that can be received
	//
	// The current maximum number of messages that can be handled at the receiver endpoint
	// of the link. Only the receiver endpoint can independently set this value. The sender
	// endpoint sets this to the last known value seen from the receiver.
	// See subsection 2.6.7 for more details.
	//
	// When the handle field is not set, this field MUST NOT be set.
	LinkCredit *uint32

	// the number of available messages
	//
	// The number of messages awaiting credit at the link sender endpoint. Only the sender
	// can independently set this value. The receiver sets this to the last known value seen
	// from the sender. See subsection 2.6.7 for more details.
	//
	// When the handle field is not set, this field MUST NOT be set.
	Available *uint32

	// indicates drain mode
	//
	// When flow state is sent from the sender to the receiver, this field contains the
	// actual drain mode of the sender. When flow state is sent from the receiver to the
	// sender, this field contains the desired drain mode of the receiver.
	// See subsection 2.6.7 for more details.
	//
	// When the handle field is not set, this field MUST NOT be set.
	Drain bool

	// request state from partner
	//
	// If set to true then the receiver SHOULD send its state at the earliest convenient
	// opportunity.
	//
	// If set to true, and the handle field is not set, then the sender only requires
	// session endpoint state to be echoed, however, the receiver MAY fulfil this requirement
	// by sending a flow performative carrying link-specific state (since any such flow also
	// carries session state).
	//
	// If a sender makes multiple requests for the same state before the receiver can reply,
	// the receiver MAY send only one flow in return.
	//
	// Note that if a peer responds to echo requests with flows which themselves have the
	// echo field set to true, an infinite loop could result if its partner adopts the same
	// policy (therefore such a policy SHOULD be avoided).
	Echo bool

	// link state properties
	// http://www.amqp.org/specification/1.0/link-state-properties
	Properties map[symbol]interface{}
}

func (f *performFlow) frameBody() {}

func (f *performFlow) String() string {
	return fmt.Sprintf("Flow{NextIncomingID: %s, IncomingWindow: %d, NextOutgoingID: %d, OutgoingWindow: %d, "+
		"Handle: %s, DeliveryCount: %s, LinkCredit: %s, Available: %s, Drain: %t, Echo: %t, Properties: %+v}",
		formatUint32Ptr(f.NextIncomingID),
		f.IncomingWindow,
		f.NextOutgoingID,
		f.OutgoingWindow,
		formatUint32Ptr(f.Handle),
		formatUint32Ptr(f.DeliveryCount),
		formatUint32Ptr(f.LinkCredit),
		formatUint32Ptr(f.Available),
		f.Drain,
		f.Echo,
		f.Properties,
	)
}

func formatUint32Ptr(p *uint32) string {
	if p == nil {
		return "<nil>"
	}
	return strconv.FormatUint(uint64(*p), 10)
}

func (f *performFlow) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeFlow, []marshalField{
		{value: f.NextIncomingID, omit: f.NextIncomingID == nil},
		{value: &f.IncomingWindow, omit: false},
		{value: &f.NextOutgoingID, omit: false},
		{value: &f.OutgoingWindow, omit: false},
		{value: f.Handle, omit: f.Handle == nil},
		{value: f.DeliveryCount, omit: f.DeliveryCount == nil},
		{value: f.LinkCredit, omit: f.LinkCredit == nil},
		{value: f.Available, omit: f.Available == nil},
		{value: &f.Drain, omit: !f.Drain},
		{value: &f.Echo, omit: !f.Echo},
		{value: f.Properties, omit: len(f.Properties) == 0},
	})
}

func (f *performFlow) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeFlow, []unmarshalField{
		{field: &f.NextIncomingID},
		{field: &f.IncomingWindow, handleNull: func() error { return errors.New("Flow.IncomingWindow is required") }},
		{field: &f.NextOutgoingID, handleNull: func() error { return errors.New("Flow.NextOutgoingID is required") }},
		{field: &f.OutgoingWindow, handleNull: func() error { return errors.New("Flow.OutgoingWindow is required") }},
		{field: &f.Handle},
		{field: &f.DeliveryCount},
		{field: &f.LinkCredit},
		{field: &f.Available},
		{field: &f.Drain},
		{field: &f.Echo},
		{field: &f.Properties},
	}...)
}

/*
<type name="transfer" class="composite" source="list" provides="frame">
    <descriptor name="amqp:transfer:list" code="0x00000000:0x00000014"/>
    <field name="handle" type="handle" mandatory="true"/>
    <field name="delivery-id" type="delivery-number"/>
    <field name="delivery-tag" type="delivery-tag"/>
    <field name="message-format" type="message-format"/>
    <field name="settled" type="boolean"/>
    <field name="more" type="boolean" default="false"/>
    <field name="rcv-settle-mode" type="receiver-settle-mode"/>
    <field name="state" type="*" requires="delivery-state"/>
    <field name="resume" type="boolean" default="false"/>
    <field name="aborted" type="boolean" default="false"/>
    <field name="batchable" type="boolean" default="false"/>
</type>
*/
type performTransfer struct {
	// Specifies the link on which the message is transferred.
	Handle uint32 // required

	// The delivery-id MUST be supplied on the first transfer of a multi-transfer
	// delivery. On continuation transfers the delivery-id MAY be omitted. It is
	// an error if the delivery-id on a continuation transfer differs from the
	// delivery-id on the first transfer of a delivery.
	DeliveryID *uint32 // sequence number

	// Uniquely identifies the delivery attempt for a given message on this link.
	// This field MUST be specified for the first transfer of a multi-transfer
	// message and can only be omitted for continuation transfers. It is an error
	// if the delivery-tag on a continuation transfer differs from the delivery-tag
	// on the first transfer of a delivery.
	DeliveryTag []byte // up to 32 bytes

	// This field MUST be specified for the first transfer of a multi-transfer message
	// and can only be omitted for continuation transfers. It is an error if the
	// message-format on a continuation transfer differs from the message-format on
	// the first transfer of a delivery.
	//
	// The upper three octets of a message format code identify a particular message
	// format. The lowest octet indicates the version of said message format. Any given
	// version of a format is forwards compatible with all higher versions.
	MessageFormat *uint32

	// If not set on the first (or only) transfer for a (multi-transfer) delivery,
	// then the settled flag MUST be interpreted as being false. For subsequent
	// transfers in a multi-transfer delivery if the settled flag is left unset then
	// it MUST be interpreted as true if and only if the value of the settled flag on
	// any of the preceding transfers was true; if no preceding transfer was sent with
	// settled being true then the value when unset MUST be taken as false.
	//
	// If the negotiated value for snd-settle-mode at attachment is settled, then this
	// field MUST be true on at least one transfer frame for a delivery (i.e., the
	// delivery MUST be settled at the sender at the point the delivery has been
	// completely transferred).
	//
	// If the negotiated value for snd-settle-mode at attachment is unsettled, then this
	// field MUST be false (or unset) on every transfer frame for a delivery (unless the
	// delivery is aborted).
	Settled bool

	// indicates that the message has more content
	//
	// Note that if both the more and aborted fields are set to true, the aborted flag
	// takes precedence. That is, a receiver SHOULD ignore the value of the more field
	// if the transfer is marked as aborted. A sender SHOULD NOT set the more flag to
	// true if it also sets the aborted flag to true.
	More bool

	// If first, this indicates that the receiver MUST settle the delivery once it has
	// arrived without waiting for the sender to settle first.
	//
	// If second, this indicates that the receiver MUST NOT settle until sending its
	// disposition to the sender and receiving a settled disposition from the sender.
	//
	// If not set, this value is defaulted to the value negotiated on link attach.
	//
	// If the negotiated link value is first, then it is illegal to set this field
	// to second.
	//
	// If the message is being sent settled by the sender, the value of this field
	// is ignored.
	//
	// The (implicit or explicit) value of this field does not form part of the
	// transfer state, and is not retained if a link is suspended and subsequently resumed.
	//
	// 0: first - The receiver will spontaneously settle all incoming transfers.
	// 1: second - The receiver will only settle after sending the disposition to
	//             the sender and receiving a disposition indicating settlement of
	//             the delivery from the sender.
	ReceiverSettleMode *ReceiverSettleMode

	// the state of the delivery at the sender
	//
	// When set this informs the receiver of the state of the delivery at the sender.
	// This is particularly useful when transfers of unsettled deliveries are resumed
	// after resuming a link. Setting the state on the transfer can be thought of as
	// being equivalent to sending a disposition immediately before the transfer
	// performative, i.e., it is the state of the delivery (not the transfer) that
	// existed at the point the frame was sent.
	//
	// Note that if the transfer performative (or an earlier disposition performative
	// referring to the delivery) indicates that the delivery has attained a terminal
	// state, then no future transfer or disposition sent by the sender can alter that
	// terminal state.
	State deliveryState

	// indicates a resumed delivery
	//
	// If true, the resume flag indicates that the transfer is being used to reassociate
	// an unsettled delivery from a dissociated link endpoint. See subsection 2.6.13
	// for more details.
	//
	// The receiver MUST ignore resumed deliveries that are not in its local unsettled map.
	// The sender MUST NOT send resumed transfers for deliveries not in its local
	// unsettled map.
	//
	// If a resumed delivery spans more than one transfer performative, then the resume
	// flag MUST be set to true on the first transfer of the resumed delivery. For
	// subsequent transfers for the same delivery the resume flag MAY be set to true,
	// or MAY be omitted.
	//
	// In the case where the exchange of unsettled maps makes clear that all message
	// data has been successfully transferred to the receiver, and that only the final
	// state (and potentially settlement) at the sender needs to be conveyed, then a
	// resumed delivery MAY carry no payload and instead act solely as a vehicle for
	// carrying the terminal state of the delivery at the sender.
	Resume bool

	// indicates that the message is aborted
	//
	// Aborted messages SHOULD be discarded by the recipient (any payload within the
	// frame carrying the performative MUST be ignored). An aborted message is
	// implicitly settled.
	Aborted bool

	// batchable hint
	//
	// If true, then the issuer is hinting that there is no need for the peer to urgently
	// communicate updated delivery state. This hint MAY be used to artificially increase
	// the amount of batching an implementation uses when communicating delivery states,
	// and thereby save bandwidth.
	//
	// If the message being delivered is too large to fit within a single frame, then the
	// setting of batchable to true on any of the transfer performatives for the delivery
	// is equivalent to setting batchable to true for all the transfer performatives for
	// the delivery.
	//
	// The batchable value does not form part of the transfer state, and is not retained
	// if a link is suspended and subsequently resumed.
	Batchable bool

	Payload []byte

	// optional channel to indicate to sender that transfer has completed
	//
	// Settled=true: closed when the transferred on network.
	// Settled=false: closed when the receiver has confirmed settlement.
	done chan deliveryState
}

func (t *performTransfer) frameBody() {}

func (t performTransfer) String() string {
	deliveryTag := "<nil>"
	if t.DeliveryTag != nil {
		deliveryTag = fmt.Sprintf("%q", t.DeliveryTag)
	}

	return fmt.Sprintf("Transfer{Handle: %d, DeliveryID: %s, DeliveryTag: %s, MessageFormat: %s, "+
		"Settled: %t, More: %t, ReceiverSettleMode: %s, State: %v, Resume: %t, Aborted: %t, "+
		"Batchable: %t, Payload [size]: %d}",
		t.Handle,
		formatUint32Ptr(t.DeliveryID),
		deliveryTag,
		formatUint32Ptr(t.MessageFormat),
		t.Settled,
		t.More,
		t.ReceiverSettleMode,
		t.State,
		t.Resume,
		t.Aborted,
		t.Batchable,
		len(t.Payload),
	)
}

func (t *performTransfer) marshal(wr *buffer.Buffer) error {
	err := marshalComposite(wr, typeCodeTransfer, []marshalField{
		{value: &t.Handle},
		{value: t.DeliveryID, omit: t.DeliveryID == nil},
		{value: &t.DeliveryTag, omit: len(t.DeliveryTag) == 0},
		{value: t.MessageFormat, omit: t.MessageFormat == nil},
		{value: &t.Settled, omit: !t.Settled},
		{value: &t.More, omit: !t.More},
		{value: t.ReceiverSettleMode, omit: t.ReceiverSettleMode == nil},
		{value: t.State, omit: t.State == nil},
		{value: &t.Resume, omit: !t.Resume},
		{value: &t.Aborted, omit: !t.Aborted},
		{value: &t.Batchable, omit: !t.Batchable},
	})
	if err != nil {
		return err
	}

	wr.Append(t.Payload)
	return nil
}

func (t *performTransfer) unmarshal(r *buffer.Buffer) error {
	err := unmarshalComposite(r, typeCodeTransfer, []unmarshalField{
		{field: &t.Handle, handleNull: func() error { return errors.New("Transfer.Handle is required") }},
		{field: &t.DeliveryID},
		{field: &t.DeliveryTag},
		{field: &t.MessageFormat},
		{field: &t.Settled},
		{field: &t.More},
		{field: &t.ReceiverSettleMode},
		{field: &t.State},
		{field: &t.Resume},
		{field: &t.Aborted},
		{field: &t.Batchable},
	}...)
	if err != nil {
		return err
	}

	t.Payload = append([]byte(nil), r.Bytes()...)

	return err
}

/*
<type name="disposition" class="composite" source="list" provides="frame">
    <descriptor name="amqp:disposition:list" code="0x00000000:0x00000015"/>
    <field name="role" type="role" mandatory="true"/>
    <field name="first" type="delivery-number" mandatory="true"/>
    <field name="last" type="delivery-number"/>
    <field name="settled" type="boolean" default="false"/>
    <field name="state" type="*" requires="delivery-state"/>
    <field name="batchable" type="boolean" default="false"/>
</type>
*/
type performDisposition struct {
	// directionality of disposition
	//
	// The role identifies whether the disposition frame contains information about
	// sending link endpoints or receiving link endpoints.
	Role role

	// lower bound of deliveries
	//
	// Identifies the lower bound of delivery-ids for the deliveries in this set.
	First uint32 // required, sequence number

	// upper bound of deliveries
	//
	// Identifies the upper bound of delivery-ids for the deliveries in this set.
	// If not set, this is taken to be the same as first.
	Last *uint32 // sequence number

	// indicates deliveries are settled
	//
	// If true, indicates that the referenced deliveries are considered settled by
	// the issuing endpoint.
	Settled bool

	// indicates state of deliveries
	//
	// Communicates the state of all the deliveries referenced by this disposition.
	State deliveryState

	// batchable hint
	//
	// If true, then the issuer is hinting that there is no need for the peer to
	// urgently communicate the impact of the updated delivery states. This hint
	// MAY be used to artificially increase the amount of batching an implementation
	// uses when communicating delivery states, and thereby save bandwidth.
	Batchable bool
}

func (d *performDisposition) frameBody() {}

func (d performDisposition) String() string {
	return fmt.Sprintf("Disposition{Role: %s, First: %d, Last: %s, Settled: %t, State: %s, Batchable: %t}",
		d.Role,
		d.First,
		formatUint32Ptr(d.Last),
		d.Settled,
		d.State,
		d.Batchable,
	)
}

func (d *performDisposition) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeDisposition, []marshalField{
		{value: &d.Role, omit: false},
		{value: &d.First, omit: false},
		{value: d.Last, omit: d.Last == nil},
		{value: &d.Settled, omit: !d.Settled},
		{value: d.State, omit: d.State == nil},
		{value: &d.Batchable, omit: !d.Batchable},
	})
}

func (d *performDisposition) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeDisposition, []unmarshalField{
		{field: &d.Role, handleNull: func() error { return errors.New("Disposition.Role is required") }},
		{field: &d.First, handleNull: func() error { return errors.New("Disposition.Handle is required") }},
		{field: &d.Last},
		{field: &d.Settled},
		{field: &d.State},
		{field: &d.Batchable},
	}...)
}

/*
<type name="detach" class="composite" source="list" provides="frame">
    <descriptor name="amqp:detach:list" code="0x00000000:0x00000016"/>
    <field name="handle" type="handle" mandatory="true"/>
    <field name="closed" type="boolean" default="false"/>
    <field name="error" type="error"/>
</type>
*/
type performDetach struct {
	// the local handle of the link to be detached
	Handle uint32 //required

	// if true then the sender has closed the link
	Closed bool

	// error causing the detach
	//
	// If set, this field indicates that the link is being detached due to an error
	// condition. The value of the field SHOULD contain details on the cause of the error.
	Error *Error
}

func (d *performDetach) frameBody() {}

func (d performDetach) String() string {
	return fmt.Sprintf("Detach{Handle: %d, Closed: %t, Error: %v}",
		d.Handle,
		d.Closed,
		d.Error,
	)
}

func (d *performDetach) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeDetach, []marshalField{
		{value: &d.Handle, omit: false},
		{value: &d.Closed, omit: !d.Closed},
		{value: d.Error, omit: d.Error == nil},
	})
}

func (d *performDetach) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeDetach, []unmarshalField{
		{field: &d.Handle, handleNull: func() error { return errors.New("Detach.Handle is required") }},
		{field: &d.Closed},
		{field: &d.Error},
	}...)
}

/*
<type name="end" class="composite" source="list" provides="frame">
    <descriptor name="amqp:end:list" code="0x00000000:0x00000017"/>
    <field name="error" type="error"/>
</type>
*/
type performEnd struct {
	// error causing the end
	//
	// If set, this field indicates that the session is being ended due to an error
	// condition. The value of the field SHOULD contain details on the cause of the error.
	Error *Error
}

func (e *performEnd) frameBody() {}

func (e *performEnd) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeEnd, []marshalField{
		{value: e.Error, omit: e.Error == nil},
	})
}

func (e *performEnd) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeEnd,
		unmarshalField{field: &e.Error},
	)
}

/*
<type name="close" class="composite" source="list" provides="frame">
    <descriptor name="amqp:close:list" code="0x00000000:0x00000018"/>
    <field name="error" type="error"/>
</type>
*/
type performClose struct {
	// error causing the close
	//
	// If set, this field indicates that the session is being closed due to an error
	// condition. The value of the field SHOULD contain details on the cause of the error.
	Error *Error
}

func (c *performClose) frameBody() {}

func (c *performClose) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeClose, []marshalField{
		{value: c.Error, omit: c.Error == nil},
	})
}

func (c *performClose) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeClose,
		unmarshalField{field: &c.Error},
	)
}

func (c *performClose) String() string {
	return fmt.Sprintf("Close{Error: %s}", c.Error)
}

/*
<type name="sasl-init" class="composite" source="list" provides="sasl-frame">
    <descriptor name="amqp:sasl-init:list" code="0x00000000:0x00000041"/>
    <field name="mechanism" type="symbol" mandatory="true"/>
    <field name="initial-response" type="binary"/>
    <field name="hostname" type="string"/>
</type>
*/

type saslInit struct {
	Mechanism       symbol
	InitialResponse []byte
	Hostname        string
}

func (si *saslInit) frameBody() {}

func (si *saslInit) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeSASLInit, []marshalField{
		{value: &si.Mechanism, omit: false},
		{value: &si.InitialResponse, omit: len(si.InitialResponse) == 0},
		{value: &si.Hostname, omit: len(si.Hostname) == 0},
	})
}

func (si *saslInit) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeSASLInit, []unmarshalField{
		{field: &si.Mechanism, handleNull: func() error { return errors.New("saslInit.Mechanism is required") }},
		{field: &si.InitialResponse},
		{field: &si.Hostname},
	}...)
}

func (si *saslInit) String() string {
	// Elide the InitialResponse as it may contain a plain text secret.
	return fmt.Sprintf("SaslInit{Mechanism : %s, InitialResponse: ********, Hostname: %s}",
		si.Mechanism,
		si.Hostname,
	)
}

/*
<type name="sasl-mechanisms" class="composite" source="list" provides="sasl-frame">
    <descriptor name="amqp:sasl-mechanisms:list" code="0x00000000:0x00000040"/>
    <field name="sasl-server-mechanisms" type="symbol" multiple="true" mandatory="true"/>
</type>
*/

type saslMechanisms struct {
	Mechanisms multiSymbol
}

func (sm *saslMechanisms) frameBody() {}

func (sm *saslMechanisms) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeSASLMechanism, []marshalField{
		{value: &sm.Mechanisms, omit: false},
	})
}

func (sm *saslMechanisms) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeSASLMechanism,
		unmarshalField{field: &sm.Mechanisms, handleNull: func() error { return errors.New("saslMechanisms.Mechanisms is required") }},
	)
}

func (sm *saslMechanisms) String() string {
	return fmt.Sprintf("SaslMechanisms{Mechanisms : %v}",
		sm.Mechanisms,
	)
}

/*
<type class="composite" name="sasl-challenge" source="list" provides="sasl-frame" label="security mechanism challenge">
    <descriptor name="amqp:sasl-challenge:list" code="0x00000000:0x00000042"/>
    <field name="challenge" type="binary" label="security challenge data" mandatory="true"/>
</type>
*/

type saslChallenge struct {
	Challenge []byte
}

func (sc *saslChallenge) String() string {
	return "Challenge{Challenge: ********}"
}

func (sc *saslChallenge) frameBody() {}

func (sc *saslChallenge) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeSASLChallenge, []marshalField{
		{value: &sc.Challenge, omit: false},
	})
}

func (sc *saslChallenge) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeSASLChallenge, []unmarshalField{
		{field: &sc.Challenge, handleNull: func() error { return errors.New("saslChallenge.Challenge is required") }},
	}...)
}

/*
<type class="composite" name="sasl-response" source="list" provides="sasl-frame" label="security mechanism response">
    <descriptor name="amqp:sasl-response:list" code="0x00000000:0x00000043"/>
    <field name="response" type="binary" label="security response data" mandatory="true"/>
</type>
*/

type saslResponse struct {
	Response []byte
}

func (sr *saslResponse) String() string {
	return "Response{Response: ********}"
}

func (sr *saslResponse) frameBody() {}

func (sr *saslResponse) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeSASLResponse, []marshalField{
		{value: &sr.Response, omit: false},
	})
}

func (sr *saslResponse) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeSASLResponse, []unmarshalField{
		{field: &sr.Response, handleNull: func() error { return errors.New("saslResponse.Response is required") }},
	}...)
}

/*
<type name="sasl-outcome" class="composite" source="list" provides="sasl-frame">
    <descriptor name="amqp:sasl-outcome:list" code="0x00000000:0x00000044"/>
    <field name="code" type="sasl-code" mandatory="true"/>
    <field name="additional-data" type="binary"/>
</type>
*/

type saslOutcome struct {
	Code           saslCode
	AdditionalData []byte
}

func (so *saslOutcome) frameBody() {}

func (so *saslOutcome) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeSASLOutcome, []marshalField{
		{value: &so.Code, omit: false},
		{value: &so.AdditionalData, omit: len(so.AdditionalData) == 0},
	})
}

func (so *saslOutcome) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeSASLOutcome, []unmarshalField{
		{field: &so.Code, handleNull: func() error { return errors.New("saslOutcome.AdditionalData is required") }},
		{field: &so.AdditionalData},
	}...)
}

func (so *saslOutcome) String() string {
	return fmt.Sprintf("SaslOutcome{Code : %v, AdditionalData: %v}",
		so.Code,
		so.AdditionalData,
	)
}
