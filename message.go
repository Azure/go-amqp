package amqp

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/go-amqp/internal/buffer"
)

// Message is an AMQP message.
type Message struct {
	// Message format code.
	//
	// The upper three octets of a message format code identify a particular message
	// format. The lowest octet indicates the version of said message format. Any
	// given version of a format is forwards compatible with all higher versions.
	Format uint32

	// The DeliveryTag can be up to 32 octets of binary data.
	// Note that when mode one is enabled there will be no delivery tag.
	DeliveryTag []byte

	// The header section carries standard delivery details about the transfer
	// of a message through the AMQP network.
	Header *MessageHeader
	// If the header section is omitted the receiver MUST assume the appropriate
	// default values (or the meaning implied by no value being set) for the
	// fields within the header unless other target or node specific defaults
	// have otherwise been set.

	// The delivery-annotations section is used for delivery-specific non-standard
	// properties at the head of the message. Delivery annotations convey information
	// from the sending peer to the receiving peer.
	DeliveryAnnotations Annotations
	// If the recipient does not understand the annotation it cannot be acted upon
	// and its effects (such as any implied propagation) cannot be acted upon.
	// Annotations might be specific to one implementation, or common to multiple
	// implementations. The capabilities negotiated on link attach and on the source
	// and target SHOULD be used to establish which annotations a peer supports. A
	// registry of defined annotations and their meanings is maintained [AMQPDELANN].
	// The symbolic key "rejected" is reserved for the use of communicating error
	// information regarding rejected messages. Any values associated with the
	// "rejected" key MUST be of type error.
	//
	// If the delivery-annotations section is omitted, it is equivalent to a
	// delivery-annotations section containing an empty map of annotations.

	// The message-annotations section is used for properties of the message which
	// are aimed at the infrastructure.
	Annotations Annotations
	// The message-annotations section is used for properties of the message which
	// are aimed at the infrastructure and SHOULD be propagated across every
	// delivery step. Message annotations convey information about the message.
	// Intermediaries MUST propagate the annotations unless the annotations are
	// explicitly augmented or modified (e.g., by the use of the modified outcome).
	//
	// The capabilities negotiated on link attach and on the source and target can
	// be used to establish which annotations a peer understands; however, in a
	// network of AMQP intermediaries it might not be possible to know if every
	// intermediary will understand the annotation. Note that for some annotations
	// it might not be necessary for the intermediary to understand their purpose,
	// i.e., they could be used purely as an attribute which can be filtered on.
	//
	// A registry of defined annotations and their meanings is maintained [AMQPMESSANN].
	//
	// If the message-annotations section is omitted, it is equivalent to a
	// message-annotations section containing an empty map of annotations.

	// The properties section is used for a defined set of standard properties of
	// the message.
	Properties *MessageProperties
	// The properties section is part of the bare message; therefore,
	// if retransmitted by an intermediary, it MUST remain unaltered.

	// The application-properties section is a part of the bare message used for
	// structured application data. Intermediaries can use the data within this
	// structure for the purposes of filtering or routing.
	ApplicationProperties map[string]interface{}
	// The keys of this map are restricted to be of type string (which excludes
	// the possibility of a null key) and the values are restricted to be of
	// simple types only, that is, excluding map, list, and array types.

	// Data payloads.
	Data [][]byte
	// A data section contains opaque binary data.
	// TODO: this could be data(s), amqp-sequence(s), amqp-value rather than single data:
	// "The body consists of one of the following three choices: one or more data
	//  sections, one or more amqp-sequence sections, or a single amqp-value section."

	// Value payload.
	Value interface{}
	// An amqp-value section contains a single AMQP value.

	// The footer section is used for details about the message or delivery which
	// can only be calculated or evaluated once the whole bare message has been
	// constructed or seen (for example message hashes, HMACs, signatures and
	// encryption details).
	Footer Annotations

	// Mark the message as settled when LinkSenderSettle is ModeMixed.
	//
	// This field is ignored when LinkSenderSettle is not ModeMixed.
	SendSettled bool

	receiver   *Receiver // Receiver the message was received from
	deliveryID uint32    // used when sending disposition
	settled    bool      // whether transfer was settled by sender

	// doneSignal is a channel that indicate when a message is considered acted upon by downstream handler
	doneSignal chan struct{}
}

// NewMessage returns a *Message with data as the payload.
//
// This constructor is intended as a helper for basic Messages with a
// single data payload. It is valid to construct a Message directly for
// more complex usages.
func NewMessage(data []byte) *Message {
	return &Message{
		Data:       [][]byte{data},
		doneSignal: make(chan struct{}),
	}
}

// done closes the internal doneSignal channel to let the receiver know that this message has been acted upon
func (m *Message) done() {
	// TODO: move initialization in ctor and use ctor everywhere?
	if m.doneSignal != nil {
		close(m.doneSignal)
	}
}

// GetData returns the first []byte from the Data field
// or nil if Data is empty.
func (m *Message) GetData() []byte {
	if len(m.Data) < 1 {
		return nil
	}
	return m.Data[0]
}

// GetLinkName returns associated link name or empty string if receiver or link is not defined.
func (m *Message) GetLinkName() string {
	if m.receiver != nil && m.receiver.link != nil {
		return m.receiver.link.key.name
	}
	return ""
}

// Accept notifies the server that the message has been
// accepted and does not require redelivery.
func (m *Message) Accept(ctx context.Context) error {
	if !m.shouldSendDisposition() {
		return nil
	}
	defer m.done()
	return m.receiver.messageDisposition(ctx, m.deliveryID, &stateAccepted{})
}

// Reject notifies the server that the message is invalid.
//
// Rejection error is optional.
func (m *Message) Reject(ctx context.Context, e *Error) error {
	if !m.shouldSendDisposition() {
		return nil
	}
	defer m.done()
	return m.receiver.messageDisposition(ctx, m.deliveryID, &stateRejected{Error: e})
}

// Release releases the message back to the server. The message
// may be redelivered to this or another consumer.
func (m *Message) Release(ctx context.Context) error {
	if !m.shouldSendDisposition() {
		return nil
	}
	defer m.done()
	return m.receiver.messageDisposition(ctx, m.deliveryID, &stateReleased{})
}

// Modify notifies the server that the message was not acted upon
// and should be modifed.
//
// deliveryFailed indicates that the server must consider this and
// unsuccessful delivery attempt and increment the delivery count.
//
// undeliverableHere indicates that the server must not redeliver
// the message to this link.
//
// messageAnnotations is an optional annotation map to be merged
// with the existing message annotations, overwriting existing keys
// if necessary.
func (m *Message) Modify(ctx context.Context, deliveryFailed, undeliverableHere bool, messageAnnotations Annotations) error {
	if !m.shouldSendDisposition() {
		return nil
	}
	defer m.done()
	return m.receiver.messageDisposition(ctx,
		m.deliveryID, &stateModified{
			DeliveryFailed:     deliveryFailed,
			UndeliverableHere:  undeliverableHere,
			MessageAnnotations: messageAnnotations,
		})
}

// Ignore notifies the amqp message pump that the message has been handled
// without any disposition. It frees the amqp receiver to get the next message
// this is implicitly done after calling message dispositions (Accept/Release/Reject/Modify)
func (m *Message) Ignore() {
	if m.shouldSendDisposition() {
		m.done()
	}
}

// MarshalBinary encodes the message into binary form.
func (m *Message) MarshalBinary() ([]byte, error) {
	buf := &buffer.Buffer{}
	err := m.marshal(buf)
	return buf.Detach(), err
}

func (m *Message) shouldSendDisposition() bool {
	return !m.settled
}

func (m *Message) marshal(wr *buffer.Buffer) error {
	if m.Header != nil {
		err := m.Header.marshal(wr)
		if err != nil {
			return err
		}
	}

	if m.DeliveryAnnotations != nil {
		writeDescriptor(wr, typeCodeDeliveryAnnotations)
		err := marshal(wr, m.DeliveryAnnotations)
		if err != nil {
			return err
		}
	}

	if m.Annotations != nil {
		writeDescriptor(wr, typeCodeMessageAnnotations)
		err := marshal(wr, m.Annotations)
		if err != nil {
			return err
		}
	}

	if m.Properties != nil {
		err := marshal(wr, m.Properties)
		if err != nil {
			return err
		}
	}

	if m.ApplicationProperties != nil {
		writeDescriptor(wr, typeCodeApplicationProperties)
		err := marshal(wr, m.ApplicationProperties)
		if err != nil {
			return err
		}
	}

	for _, data := range m.Data {
		writeDescriptor(wr, typeCodeApplicationData)
		err := writeBinary(wr, data)
		if err != nil {
			return err
		}
	}

	if m.Value != nil {
		writeDescriptor(wr, typeCodeAMQPValue)
		err := marshal(wr, m.Value)
		if err != nil {
			return err
		}
	}

	if m.Footer != nil {
		writeDescriptor(wr, typeCodeFooter)
		err := marshal(wr, m.Footer)
		if err != nil {
			return err
		}
	}

	return nil
}

// UnmarshalBinary decodes the message from binary form.
func (m *Message) UnmarshalBinary(data []byte) error {
	buf := buffer.New(data)
	return m.unmarshal(buf)
}

func (m *Message) unmarshal(r *buffer.Buffer) error {
	// loop, decoding sections until bytes have been consumed
	for r.Len() > 0 {
		// determine type
		type_, err := peekMessageType(r.Bytes())
		if err != nil {
			return err
		}

		var (
			section interface{}
			// section header is read from r before
			// unmarshaling section is set to true
			discardHeader = true
		)
		switch amqpType(type_) {

		case typeCodeMessageHeader:
			discardHeader = false
			section = &m.Header

		case typeCodeDeliveryAnnotations:
			section = &m.DeliveryAnnotations

		case typeCodeMessageAnnotations:
			section = &m.Annotations

		case typeCodeMessageProperties:
			discardHeader = false
			section = &m.Properties

		case typeCodeApplicationProperties:
			section = &m.ApplicationProperties

		case typeCodeApplicationData:
			r.Skip(3)

			var data []byte
			err = unmarshal(r, &data)
			if err != nil {
				return err
			}

			m.Data = append(m.Data, data)
			continue

		case typeCodeFooter:
			section = &m.Footer

		case typeCodeAMQPValue:
			section = &m.Value

		default:
			return fmt.Errorf("unknown message section %#02x", type_)
		}

		if discardHeader {
			r.Skip(3)
		}

		err = unmarshal(r, section)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
<type name="header" class="composite" source="list" provides="section">
    <descriptor name="amqp:header:list" code="0x00000000:0x00000070"/>
    <field name="durable" type="boolean" default="false"/>
    <field name="priority" type="ubyte" default="4"/>
    <field name="ttl" type="milliseconds"/>
    <field name="first-acquirer" type="boolean" default="false"/>
    <field name="delivery-count" type="uint" default="0"/>
</type>
*/

// MessageHeader carries standard delivery details about the transfer
// of a message.
type MessageHeader struct {
	Durable       bool
	Priority      uint8
	TTL           time.Duration // from milliseconds
	FirstAcquirer bool
	DeliveryCount uint32
}

func (h *MessageHeader) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeMessageHeader, []marshalField{
		{value: &h.Durable, omit: !h.Durable},
		{value: &h.Priority, omit: h.Priority == 4},
		{value: (*milliseconds)(&h.TTL), omit: h.TTL == 0},
		{value: &h.FirstAcquirer, omit: !h.FirstAcquirer},
		{value: &h.DeliveryCount, omit: h.DeliveryCount == 0},
	})
}

func (h *MessageHeader) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeMessageHeader, []unmarshalField{
		{field: &h.Durable},
		{field: &h.Priority, handleNull: func() error { h.Priority = 4; return nil }},
		{field: (*milliseconds)(&h.TTL)},
		{field: &h.FirstAcquirer},
		{field: &h.DeliveryCount},
	}...)
}

/*
<type name="properties" class="composite" source="list" provides="section">
    <descriptor name="amqp:properties:list" code="0x00000000:0x00000073"/>
    <field name="message-id" type="*" requires="message-id"/>
    <field name="user-id" type="binary"/>
    <field name="to" type="*" requires="address"/>
    <field name="subject" type="string"/>
    <field name="reply-to" type="*" requires="address"/>
    <field name="correlation-id" type="*" requires="message-id"/>
    <field name="content-type" type="symbol"/>
    <field name="content-encoding" type="symbol"/>
    <field name="absolute-expiry-time" type="timestamp"/>
    <field name="creation-time" type="timestamp"/>
    <field name="group-id" type="string"/>
    <field name="group-sequence" type="sequence-no"/>
    <field name="reply-to-group-id" type="string"/>
</type>
*/

// MessageProperties is the defined set of properties for AMQP messages.
type MessageProperties struct {
	// Message-id, if set, uniquely identifies a message within the message system.
	// The message producer is usually responsible for setting the message-id in
	// such a way that it is assured to be globally unique. A broker MAY discard a
	// message as a duplicate if the value of the message-id matches that of a
	// previously received message sent to the same node.
	MessageID interface{} // uint64, UUID, []byte, or string

	// The identity of the user responsible for producing the message.
	// The client sets this value, and it MAY be authenticated by intermediaries.
	UserID []byte

	// The to field identifies the node that is the intended destination of the message.
	// On any given transfer this might not be the node at the receiving end of the link.
	To string

	// A common field for summary information about the message content and purpose.
	Subject string

	// The address of the node to send replies to.
	ReplyTo string

	// This is a client-specific id that can be used to mark or identify messages
	// between clients.
	CorrelationID interface{} // uint64, UUID, []byte, or string

	// The RFC-2046 [RFC2046] MIME type for the message's application-data section
	// (body). As per RFC-2046 [RFC2046] this can contain a charset parameter defining
	// the character encoding used: e.g., 'text/plain; charset="utf-8"'.
	//
	// For clarity, as per section 7.2.1 of RFC-2616 [RFC2616], where the content type
	// is unknown the content-type SHOULD NOT be set. This allows the recipient the
	// opportunity to determine the actual type. Where the section is known to be truly
	// opaque binary data, the content-type SHOULD be set to application/octet-stream.
	//
	// When using an application-data section with a section code other than data,
	// content-type SHOULD NOT be set.
	ContentType string

	// The content-encoding property is used as a modifier to the content-type.
	// When present, its value indicates what additional content encodings have been
	// applied to the application-data, and thus what decoding mechanisms need to be
	// applied in order to obtain the media-type referenced by the content-type header
	// field.
	//
	// Content-encoding is primarily used to allow a document to be compressed without
	// losing the identity of its underlying content type.
	//
	// Content-encodings are to be interpreted as per section 3.5 of RFC 2616 [RFC2616].
	// Valid content-encodings are registered at IANA [IANAHTTPPARAMS].
	//
	// The content-encoding MUST NOT be set when the application-data section is other
	// than data. The binary representation of all other application-data section types
	// is defined completely in terms of the AMQP type system.
	//
	// Implementations MUST NOT use the identity encoding. Instead, implementations
	// SHOULD NOT set this property. Implementations SHOULD NOT use the compress encoding,
	// except as to remain compatible with messages originally sent with other protocols,
	// e.g. HTTP or SMTP.
	//
	// Implementations SHOULD NOT specify multiple content-encoding values except as to
	// be compatible with messages originally sent with other protocols, e.g. HTTP or SMTP.
	ContentEncoding string

	// An absolute time when this message is considered to be expired.
	AbsoluteExpiryTime time.Time

	// An absolute time when this message was created.
	CreationTime time.Time

	// Identifies the group the message belongs to.
	GroupID string

	// The relative position of this message within its group.
	GroupSequence uint32 // RFC-1982 sequence number

	// This is a client-specific id that is used so that client can send replies to this
	// message to a specific group.
	ReplyToGroupID string
}

func (p *MessageProperties) marshal(wr *buffer.Buffer) error {
	return marshalComposite(wr, typeCodeMessageProperties, []marshalField{
		{value: p.MessageID, omit: p.MessageID == nil},
		{value: &p.UserID, omit: len(p.UserID) == 0},
		{value: &p.To, omit: p.To == ""},
		{value: &p.Subject, omit: p.Subject == ""},
		{value: &p.ReplyTo, omit: p.ReplyTo == ""},
		{value: p.CorrelationID, omit: p.CorrelationID == nil},
		{value: (*symbol)(&p.ContentType), omit: p.ContentType == ""},
		{value: (*symbol)(&p.ContentEncoding), omit: p.ContentEncoding == ""},
		{value: &p.AbsoluteExpiryTime, omit: p.AbsoluteExpiryTime.IsZero()},
		{value: &p.CreationTime, omit: p.CreationTime.IsZero()},
		{value: &p.GroupID, omit: p.GroupID == ""},
		{value: &p.GroupSequence},
		{value: &p.ReplyToGroupID, omit: p.ReplyToGroupID == ""},
	})
}

func (p *MessageProperties) unmarshal(r *buffer.Buffer) error {
	return unmarshalComposite(r, typeCodeMessageProperties, []unmarshalField{
		{field: &p.MessageID},
		{field: &p.UserID},
		{field: &p.To},
		{field: &p.Subject},
		{field: &p.ReplyTo},
		{field: &p.CorrelationID},
		{field: &p.ContentType},
		{field: &p.ContentEncoding},
		{field: &p.AbsoluteExpiryTime},
		{field: &p.CreationTime},
		{field: &p.GroupID},
		{field: &p.GroupSequence},
		{field: &p.ReplyToGroupID},
	}...)
}
