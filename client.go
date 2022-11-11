package amqp

import (
	"context"
	"net"

	"github.com/Azure/go-amqp/internal/encoding"
)

// Client is an AMQP client connection.
type Client struct {
	conn *conn
}

// Dial connects to an AMQP server.
//
// If the addr includes a scheme, it must be "amqp", "amqps", or "amqp+ssl".
// If no port is provided, 5672 will be used for "amqp" and 5671 for "amqps" or "amqp+ssl".
//
// If username and password information is not empty it's used as SASL PLAIN
// credentials, equal to passing ConnSASLPlain option.
//
// opts: pass nil to accept the default values.
func Dial(addr string, opts *ConnOptions) (*Client, error) {
	c, err := dialConn(addr, opts)
	if err != nil {
		return nil, err
	}
	err = c.Start()
	if err != nil {
		return nil, err
	}
	return &Client{conn: c}, nil
}

// New establishes an AMQP client connection over conn.
// opts: pass nil to accept the default values.
func New(conn net.Conn, opts *ConnOptions) (*Client, error) {
	c, err := newConn(conn, opts)
	if err != nil {
		return nil, err
	}
	err = c.Start()
	if err != nil {
		return nil, err
	}
	return &Client{conn: c}, nil
}

// Close disconnects the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// NewSession opens a new AMQP session to the server.
// Returns ErrConnClosed if the underlying connection has been closed.
// opts: pass nil to accept the default values.
func (c *Client) NewSession(ctx context.Context, opts *SessionOptions) (*Session, error) {
	s, err := c.conn.NewSession(opts)
	if err != nil {
		return nil, err
	}

	if err = s.begin(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// SessionOption contains the optional settings for configuring an AMQP session.
type SessionOptions struct {
	// IncomingWindow sets the maximum number of unacknowledged
	// transfer frames the server can send.
	IncomingWindow uint32

	// OutgoingWindow sets the maximum number of unacknowledged
	// transfer frames the client can send.
	OutgoingWindow uint32

	// MaxLinks sets the maximum number of links (Senders/Receivers)
	// allowed on the session.
	//
	// Minimum: 1.
	// Default: 4294967295.
	MaxLinks uint32
}

// linkKey uniquely identifies a link on a connection by name and direction.
//
// A link can be identified uniquely by the ordered tuple
//
//	(source-container-id, target-container-id, name)
//
// On a single connection the container ID pairs can be abbreviated
// to a boolean flag indicating the direction of the link.
type linkKey struct {
	name string
	role encoding.Role // Local role: sender/receiver
}

const maxTransferFrameHeader = 66 // determined by calcMaxTransferFrameHeader
