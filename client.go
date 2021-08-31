package amqp

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/url"
	"sync"
	"time"
)

var (
	// ErrSessionClosed is propagated to Sender/Receivers
	// when Session.Close() is called.
	ErrSessionClosed = errors.New("amqp: session closed")

	// ErrLinkClosed is returned by send and receive operations when
	// Sender.Close() or Receiver.Close() are called.
	ErrLinkClosed = errors.New("amqp: link closed")

	// ErrLinkDetached is returned by operations when the
	// link is in a detached state.
	ErrLinkDetached = errors.New("amqp: link detached")
)

// Client is an AMQP client connection.
type Client struct {
	conn *conn
}

// Dial connects to an AMQP server.
//
// If the addr includes a scheme, it must be "amqp" or "amqps".
// If no port is provided, 5672 will be used for "amqp" and 5671 for "amqps".
//
// If username and password information is not empty it's used as SASL PLAIN
// credentials, equal to passing ConnSASLPlain option.
func Dial(addr string, opts ...ConnOption) (*Client, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	host, port := u.Hostname(), u.Port()
	if port == "" {
		port = "5672"
		if u.Scheme == "amqps" {
			port = "5671"
		}
	}

	// prepend SASL credentials when the user/pass segment is not empty
	if u.User != nil {
		pass, _ := u.User.Password()
		opts = append([]ConnOption{
			ConnSASLPlain(u.User.Username(), pass),
		}, opts...)
	}

	// append default options so user specified can overwrite
	opts = append([]ConnOption{
		ConnServerHostname(host),
	}, opts...)

	c, err := newConn(nil, opts...)
	if err != nil {
		return nil, err
	}

	dialer := &net.Dialer{Timeout: c.connectTimeout}
	switch u.Scheme {
	case "amqp", "":
		c.net, err = dialer.Dial("tcp", net.JoinHostPort(host, port))
	case "amqps":
		c.initTLSConfig()
		c.tlsNegotiation = false
		c.net, err = tls.DialWithDialer(dialer, "tcp", net.JoinHostPort(host, port), c.tlsConfig)
	default:
		return nil, errorErrorf("unsupported scheme %q", u.Scheme)
	}
	if err != nil {
		return nil, err
	}
	err = c.start()
	return &Client{conn: c}, err
}

// New establishes an AMQP client connection over conn.
func New(conn net.Conn, opts ...ConnOption) (*Client, error) {
	c, err := newConn(conn, opts...)
	if err != nil {
		return nil, err
	}
	err = c.start()
	return &Client{conn: c}, err
}

// Close disconnects the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// NewSession opens a new AMQP session to the server.
func (c *Client) NewSession(opts ...SessionOption) (*Session, error) {
	// get a session allocated by Client.mux
	var sResp newSessionResp
	select {
	case <-c.conn.done:
		return nil, c.conn.getErr()
	case sResp = <-c.conn.newSession:
	}

	if sResp.err != nil {
		return nil, sResp.err
	}
	s := sResp.session

	for _, opt := range opts {
		err := opt(s)
		if err != nil {
			_ = s.Close(context.Background()) // deallocate session on error
			return nil, err
		}
	}

	// send Begin to server
	begin := &performBegin{
		NextOutgoingID: 0,
		IncomingWindow: s.incomingWindow,
		OutgoingWindow: s.outgoingWindow,
		HandleMax:      s.handleMax,
	}
	debug(1, "TX: %s", begin)
	s.txFrame(begin, nil)

	// wait for response
	var fr frame
	select {
	case <-c.conn.done:
		return nil, c.conn.getErr()
	case fr = <-s.rx:
	}
	debug(1, "RX: %s", fr.body)

	begin, ok := fr.body.(*performBegin)
	if !ok {
		_ = s.Close(context.Background()) // deallocate session on error
		return nil, errorErrorf("unexpected begin response: %+v", fr.body)
	}

	// start Session multiplexor
	go s.mux(begin)

	return s, nil
}

// Default session options
const (
	DefaultMaxLinks = 4294967296
	DefaultWindow   = 1000
)

// SessionOption is an function for configuring an AMQP session.
type SessionOption func(*Session) error

// SessionIncomingWindow sets the maximum number of unacknowledged
// transfer frames the server can send.
func SessionIncomingWindow(window uint32) SessionOption {
	return func(s *Session) error {
		s.incomingWindow = window
		return nil
	}
}

// SessionOutgoingWindow sets the maximum number of unacknowledged
// transfer frames the client can send.
func SessionOutgoingWindow(window uint32) SessionOption {
	return func(s *Session) error {
		s.outgoingWindow = window
		return nil
	}
}

// SessionMaxLinks sets the maximum number of links (Senders/Receivers)
// allowed on the session.
//
// n must be in the range 1 to 4294967296.
//
// Default: 4294967296.
func SessionMaxLinks(n int) SessionOption {
	return func(s *Session) error {
		if n < 1 {
			return errorNew("max sessions cannot be less than 1")
		}
		if int64(n) > 4294967296 {
			return errorNew("max sessions cannot be greater than 4294967296")
		}
		s.handleMax = uint32(n - 1)
		return nil
	}
}

// lockedRand provides a rand source that is safe for concurrent use.
type lockedRand struct {
	mu  sync.Mutex
	src *rand.Rand
}

func (r *lockedRand) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.src.Read(p)
}

// package scoped rand source to avoid any issues with seeding
// of the global source.
var pkgRand = &lockedRand{
	src: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// randBytes returns a base64 encoded string of n bytes.
func randString(n int) string {
	b := make([]byte, n)
	pkgRand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// DetachError is returned by a link (Receiver/Sender) when a detach frame is received.
//
// RemoteError will be nil if the link was detached gracefully.
type DetachError struct {
	RemoteError *Error
}

func (e *DetachError) Error() string {
	return fmt.Sprintf("link detached, reason: %+v", e.RemoteError)
}

// Default link options
const (
	DefaultLinkCredit      = 1
	DefaultLinkBatching    = false
	DefaultLinkBatchMaxAge = 5 * time.Second
)

// linkKey uniquely identifies a link on a connection by name and direction.
//
// A link can be identified uniquely by the ordered tuple
//     (source-container-id, target-container-id, name)
// On a single connection the container ID pairs can be abbreviated
// to a boolean flag indicating the direction of the link.
type linkKey struct {
	name string
	role role // Local role: sender/receiver
}

// LinkOption is a function for configuring an AMQP link.
//
// A link may be a Sender or a Receiver.
type LinkOption func(*link) error

// LinkAddress sets the link address.
//
// For a Receiver this configures the source address.
// For a Sender this configures the target address.
//
// Deprecated: use LinkSourceAddress or LinkTargetAddress instead.
func LinkAddress(source string) LinkOption {
	return func(l *link) error {
		if l.receiver != nil {
			return LinkSourceAddress(source)(l)
		}
		return LinkTargetAddress(source)(l)
	}
}

// LinkProperty sets an entry in the link properties map sent to the server.
//
// This option can be used multiple times.
func LinkProperty(key, value string) LinkOption {
	return linkProperty(key, value)
}

// LinkPropertyInt64 sets an entry in the link properties map sent to the server.
//
// This option can be used multiple times.
func LinkPropertyInt64(key string, value int64) LinkOption {
	return linkProperty(key, value)
}

// LinkPropertyInt32 sets an entry in the link properties map sent to the server.
//
// This option can be set multiple times.
func LinkPropertyInt32(key string, value int32) LinkOption {
	return linkProperty(key, value)
}

func linkProperty(key string, value interface{}) LinkOption {
	return func(l *link) error {
		if key == "" {
			return errorNew("link property key must not be empty")
		}
		if l.properties == nil {
			l.properties = make(map[symbol]interface{})
		}
		l.properties[symbol(key)] = value
		return nil
	}
}

// LinkName sets the name of the link.
//
// The link names must be unique per-connection and direction.
//
// Default: randomly generated.
func LinkName(name string) LinkOption {
	return func(l *link) error {
		l.key.name = name
		return nil
	}
}

// LinkSourceCapabilities sets the source capabilities.
func LinkSourceCapabilities(capabilities ...string) LinkOption {
	return func(l *link) error {
		if l.source == nil {
			l.source = new(source)
		}

		// Convert string to symbol
		symbolCapabilities := make([]symbol, len(capabilities))
		for i, v := range capabilities {
			symbolCapabilities[i] = symbol(v)
		}

		l.source.Capabilities = append(l.source.Capabilities, symbolCapabilities...)
		return nil
	}
}

// LinkSourceAddress sets the source address.
func LinkSourceAddress(addr string) LinkOption {
	return func(l *link) error {
		if l.source == nil {
			l.source = new(source)
		}
		l.source.Address = addr
		return nil
	}
}

// LinkTargetAddress sets the target address.
func LinkTargetAddress(addr string) LinkOption {
	return func(l *link) error {
		if l.target == nil {
			l.target = new(target)
		}
		l.target.Address = addr
		return nil
	}
}

// LinkAddressDynamic requests a dynamically created address from the server.
func LinkAddressDynamic() LinkOption {
	return func(l *link) error {
		l.dynamicAddr = true
		return nil
	}
}

// LinkCredit specifies the maximum number of unacknowledged messages
// the sender can transmit.
func LinkCredit(credit uint32) LinkOption {
	return func(l *link) error {
		if l.receiver == nil {
			return errorNew("LinkCredit is not valid for Sender")
		}

		l.receiver.maxCredit = credit
		return nil
	}
}

// LinkBatching toggles batching of message disposition.
//
// When enabled, accepting a message does not send the disposition
// to the server until the batch is equal to link credit or the
// batch max age expires.
func LinkBatching(enable bool) LinkOption {
	return func(l *link) error {
		l.receiver.batching = enable
		return nil
	}
}

// LinkBatchMaxAge sets the maximum time between the start
// of a disposition batch and sending the batch to the server.
func LinkBatchMaxAge(d time.Duration) LinkOption {
	return func(l *link) error {
		l.receiver.batchMaxAge = d
		return nil
	}
}

// LinkSenderSettle sets the requested sender settlement mode.
//
// If a settlement mode is explicitly set and the server does not
// honor it an error will be returned during link attachment.
//
// Default: Accept the settlement mode set by the server, commonly ModeMixed.
func LinkSenderSettle(mode SenderSettleMode) LinkOption {
	return func(l *link) error {
		if mode > ModeMixed {
			return errorErrorf("invalid SenderSettlementMode %d", mode)
		}
		l.senderSettleMode = &mode
		return nil
	}
}

// LinkReceiverSettle sets the requested receiver settlement mode.
//
// If a settlement mode is explicitly set and the server does not
// honor it an error will be returned during link attachment.
//
// Default: Accept the settlement mode set by the server, commonly ModeFirst.
func LinkReceiverSettle(mode ReceiverSettleMode) LinkOption {
	return func(l *link) error {
		if mode > ModeSecond {
			return errorErrorf("invalid ReceiverSettlementMode %d", mode)
		}
		l.receiverSettleMode = &mode
		return nil
	}
}

// LinkSelectorFilter sets a selector filter (apache.org:selector-filter:string) on the link source.
func LinkSelectorFilter(filter string) LinkOption {
	// <descriptor name="apache.org:selector-filter:string" code="0x0000468C:0x00000004"/>
	return LinkSourceFilter("apache.org:selector-filter:string", 0x0000468C00000004, filter)
}

// LinkSourceFilter is an advanced API for setting non-standard source filters.
// Please file an issue or open a PR if a standard filter is missing from this
// library.
//
// The name is the key for the filter map. It will be encoded as an AMQP symbol type.
//
// The code is the descriptor of the described type value. The domain-id and descriptor-id
// should be concatenated together. If 0 is passed as the code, the name will be used as
// the descriptor.
//
// The value is the value of the descriped types. Acceptable types for value are specific
// to the filter.
//
// Example:
//
// The standard selector-filter is defined as:
//  <descriptor name="apache.org:selector-filter:string" code="0x0000468C:0x00000004"/>
// In this case the name is "apache.org:selector-filter:string" and the code is
// 0x0000468C00000004.
//  LinkSourceFilter("apache.org:selector-filter:string", 0x0000468C00000004, exampleValue)
//
// References:
//  http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-filter-set
//  http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html#section-descriptor-values
func LinkSourceFilter(name string, code uint64, value interface{}) LinkOption {
	return func(l *link) error {
		if l.source == nil {
			l.source = new(source)
		}
		if l.source.Filter == nil {
			l.source.Filter = make(map[symbol]*describedType)
		}

		var descriptor interface{}
		if code != 0 {
			descriptor = code
		} else {
			descriptor = symbol(name)
		}

		l.source.Filter[symbol(name)] = &describedType{
			descriptor: descriptor,
			value:      value,
		}
		return nil
	}
}

// LinkMaxMessageSize sets the maximum message size that can
// be sent or received on the link.
//
// A size of zero indicates no limit.
//
// Default: 0.
func LinkMaxMessageSize(size uint64) LinkOption {
	return func(l *link) error {
		l.maxMessageSize = size
		return nil
	}
}

// LinkTargetDurability sets the target durability policy.
//
// Default: DurabilityNone.
func LinkTargetDurability(d Durability) LinkOption {
	return func(l *link) error {
		if d > DurabilityUnsettledState {
			return errorErrorf("invalid Durability %d", d)
		}

		if l.target == nil {
			l.target = new(target)
		}
		l.target.Durable = d

		return nil
	}
}

// LinkTargetExpiryPolicy sets the link expiration policy.
//
// Default: ExpirySessionEnd.
func LinkTargetExpiryPolicy(p ExpiryPolicy) LinkOption {
	return func(l *link) error {
		err := p.validate()
		if err != nil {
			return err
		}

		if l.target == nil {
			l.target = new(target)
		}
		l.target.ExpiryPolicy = p

		return nil
	}
}

// LinkTargetTimeout sets the duration that an expiring target will be retained.
//
// Default: 0.
func LinkTargetTimeout(timeout uint32) LinkOption {
	return func(l *link) error {
		if l.target == nil {
			l.target = new(target)
		}
		l.target.Timeout = timeout

		return nil
	}
}

// LinkSourceDurability sets the source durability policy.
//
// Default: DurabilityNone.
func LinkSourceDurability(d Durability) LinkOption {
	return func(l *link) error {
		if d > DurabilityUnsettledState {
			return errorErrorf("invalid Durability %d", d)
		}

		if l.source == nil {
			l.source = new(source)
		}
		l.source.Durable = d

		return nil
	}
}

// LinkSourceExpiryPolicy sets the link expiration policy.
//
// Default: ExpirySessionEnd.
func LinkSourceExpiryPolicy(p ExpiryPolicy) LinkOption {
	return func(l *link) error {
		err := p.validate()
		if err != nil {
			return err
		}

		if l.source == nil {
			l.source = new(source)
		}
		l.source.ExpiryPolicy = p

		return nil
	}
}

// LinkSourceTimeout sets the duration that an expiring source will be retained.
//
// Default: 0.
func LinkSourceTimeout(timeout uint32) LinkOption {
	return func(l *link) error {
		if l.source == nil {
			l.source = new(source)
		}
		l.source.Timeout = timeout

		return nil
	}
}

// LinkDetachOnDispositionError controls whether you detach on disposition
// errors (subject to some simple logic) or do NOT detach at all on disposition
// errors.
// Defaults to true.
func LinkDetachOnDispositionError(detachOnDispositionError bool) LinkOption {
	return func(l *link) error {
		l.detachOnDispositionError = detachOnDispositionError
		return nil
	}
}

const maxTransferFrameHeader = 66 // determined by calcMaxTransferFrameHeader

func calcMaxTransferFrameHeader() int {
	var buf buffer

	maxUint32 := uint32(math.MaxUint32)
	receiverSettleMode := ReceiverSettleMode(0)
	err := writeFrame(&buf, frame{
		type_:   frameTypeAMQP,
		channel: math.MaxUint16,
		body: &performTransfer{
			Handle:             maxUint32,
			DeliveryID:         &maxUint32,
			DeliveryTag:        bytes.Repeat([]byte{'a'}, 32),
			MessageFormat:      &maxUint32,
			Settled:            true,
			More:               true,
			ReceiverSettleMode: &receiverSettleMode,
			State:              nil, // TODO: determine whether state should be included in size
			Resume:             true,
			Aborted:            true,
			Batchable:          true,
			// Payload omitted as it is appended directly without any header
		},
	})
	if err != nil {
		panic(err)
	}

	return buf.len()
}
