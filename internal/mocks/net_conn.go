package mocks

import (
	"errors"
	"math"
	"net"
	"time"

	"github.com/Azure/go-amqp/internal/buffer"
	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/frames"
)

// NewNetConn creates a new instance of NetConn.
// Responder is invoked by Write when a frame is received.
// Return a nil slice/nil error to swallow the frame.
// Return a non-nil error to simulate a write error.
func NewNetConn(resp func(frames.FrameBody) ([]byte, error)) *NetConn {
	return &NetConn{
		resp: resp,
		// during shutdown, connReader can close before connWriter as they both
		// both return on c.Done being closed, so there is some non-determinism
		// here.  this means that sometimes writes can still happen but there's
		// no reader to consume them.  we used a buffered channel to prevent these
		// writes from blocking shutdown. the size was arbitrarily picked.
		readData:  make(chan []byte, 10),
		readClose: make(chan struct{}),
	}
}

// NetConn is a mock network connection that satisfies the net.Conn interface.
type NetConn struct {
	// OnClose is called from Close() before it returns.
	// The value returned from OnClose is returned from Close().
	OnClose func() error

	resp      func(frames.FrameBody) ([]byte, error)
	readDL    *time.Timer
	readData  chan []byte
	readClose chan struct{}
	closed    bool
}

// SendFrame sends the encoded frame to the client.
// Use this to send a frame at an arbitrary time.
func (n *NetConn) SendFrame(f []byte) {
	n.readData <- f
}

// SendKeepAlive sends a keep-alive frame to the client.
func (n *NetConn) SendKeepAlive() {
	// empty frame
	n.readData <- []uint8{0, 0, 0, 8, 2, 0, 0, 0}
}

///////////////////////////////////////////////////////
// following methods are for the net.Conn interface
///////////////////////////////////////////////////////

// NOTE: Read, Write, and Close are all called by separate goroutines!

// Read is invoked by conn.connReader to recieve frame data.
// It blocks until Write or Close are called, or the read
// deadline expires which will return an error.
func (n *NetConn) Read(b []byte) (int, error) {
	select {
	case <-n.readClose:
		return 0, errors.New("mock connection was closed")
	default:
		// not closed yet
	}

	select {
	case <-n.readClose:
		return 0, errors.New("mock connection was closed")
	case <-n.readDL.C:
		return 0, errors.New("mock connection read deadline exceeded")
	case rd := <-n.readData:
		return copy(b, rd), nil
	}
}

// Write is invoked by conn.connWriter when we're being sent frame
// data.  Every call to Write will invoke the responder callback that
// must reply with one of three possibilities.
//  1. an encoded frame and nil error
//  2. a non-nil error to similate a write failure
//  3. a nil slice and nil error indicating the frame should be ignored
func (n *NetConn) Write(b []byte) (int, error) {
	select {
	case <-n.readClose:
		return 0, errors.New("mock connection was closed")
	default:
		// not closed yet
	}

	frame, err := decodeFrame(b)
	if err != nil {
		return 0, err
	}
	resp, err := n.resp(frame)
	if err != nil {
		return 0, err
	}
	if resp != nil {
		n.readData <- resp
	}
	return len(b), nil
}

// Close is called by conn.close when conn.mux unwinds.
func (n *NetConn) Close() error {
	if n.closed {
		return errors.New("double close")
	}
	n.closed = true
	close(n.readClose)
	if n.OnClose != nil {
		return n.OnClose()
	}
	return nil
}

func (n *NetConn) LocalAddr() net.Addr {
	return &net.IPAddr{
		IP: net.IPv4(127, 0, 0, 2),
	}
}

func (n *NetConn) RemoteAddr() net.Addr {
	return &net.IPAddr{
		IP: net.IPv4(127, 0, 0, 2),
	}
}

func (n *NetConn) SetDeadline(t time.Time) error {
	return errors.New("not used")
}

func (n *NetConn) SetReadDeadline(t time.Time) error {
	// called by conn.connReader before calling Read
	// stop the last timer if available
	if n.readDL != nil && !n.readDL.Stop() {
		<-n.readDL.C
	}
	n.readDL = time.NewTimer(time.Until(t))
	return nil
}

func (n *NetConn) SetWriteDeadline(t time.Time) error {
	// called by conn.connWriter before calling Write
	return nil
}

///////////////////////////////////////////////////////
///////////////////////////////////////////////////////

// ProtoID indicates the type of protocol (copied from conn.go)
type ProtoID uint8

const (
	ProtoAMQP ProtoID = 0x0
	ProtoTLS  ProtoID = 0x2
	ProtoSASL ProtoID = 0x3
)

// ProtoHeader adds the initial handshake frame to the list of responses.
// This frame, and PerformOpen, are needed when calling amqp.New() to create a client.
func ProtoHeader(id ProtoID) ([]byte, error) {
	return []byte{'A', 'M', 'Q', 'P', byte(id), 1, 0, 0}, nil
}

// PerformOpen appends a PerformOpen frame with the specified container ID.
// This frame, and ProtoHeader, are needed when calling amqp.New() to create a client.
func PerformOpen(containerID string) ([]byte, error) {
	// send the default values for max channels and frame size
	return EncodeFrame(FrameAMQP, 0, &frames.PerformOpen{
		ChannelMax:   65535,
		ContainerID:  containerID,
		IdleTimeout:  time.Minute,
		MaxFrameSize: 4294967295,
	})
}

// PerformBegin appends a PerformBegin frame with the specified remote channel ID.
// This frame is needed when making a call to Client.NewSession().
func PerformBegin(remoteChannel uint16) ([]byte, error) {
	return EncodeFrame(FrameAMQP, remoteChannel, &frames.PerformBegin{
		RemoteChannel:  &remoteChannel,
		NextOutgoingID: 1,
		IncomingWindow: 5000,
		OutgoingWindow: 1000,
		HandleMax:      math.MaxInt16,
	})
}

// ReceiverAttach appends a PerformAttach frame with the specified values.
// This frame is needed when making a call to Session.NewReceiver().
func ReceiverAttach(remoteChannel uint16, linkName string, linkHandle uint32, mode encoding.ReceiverSettleMode) ([]byte, error) {
	return EncodeFrame(FrameAMQP, remoteChannel, &frames.PerformAttach{
		Name:   linkName,
		Handle: linkHandle,
		Role:   encoding.RoleSender,
		Source: &frames.Source{
			Address:      "test",
			Durable:      encoding.DurabilityNone,
			ExpiryPolicy: encoding.ExpirySessionEnd,
		},
		ReceiverSettleMode: &mode,
		MaxMessageSize:     math.MaxUint32,
	})
}

// PerformTransfer appends a PerformTransfer frame with the specified values.
// The linkHandle MUST match the linkHandle value specified in ReceiverAttach.
func PerformTransfer(remoteChannel uint16, linkHandle, deliveryID uint32, payload []byte) ([]byte, error) {
	format := uint32(0)
	payloadBuf := &buffer.Buffer{}
	encoding.WriteDescriptor(payloadBuf, encoding.TypeCodeApplicationData)
	err := encoding.WriteBinary(payloadBuf, payload)
	if err != nil {
		return nil, err
	}
	return EncodeFrame(FrameAMQP, remoteChannel, &frames.PerformTransfer{
		Handle:        linkHandle,
		DeliveryID:    &deliveryID,
		DeliveryTag:   []byte("tag"),
		MessageFormat: &format,
		Payload:       payloadBuf.Detach(),
	})
}

// PerformDisposition appends a PerformDisposition frame with the specified values.
// The deliveryID MUST match the deliveryID value specified in PerformTransfer.
func PerformDisposition(remoteChannel uint16, deliveryID uint32, state encoding.DeliveryState) ([]byte, error) {
	return EncodeFrame(FrameAMQP, remoteChannel, &frames.PerformDisposition{
		Role:    encoding.RoleSender,
		First:   deliveryID,
		Settled: true,
		State:   state,
	})
}

// PerformEnd encodes a PerformEnd frame with an optional error.
func PerformEnd(remoteChannel uint16, e *encoding.Error) ([]byte, error) {
	return EncodeFrame(FrameAMQP, remoteChannel, &frames.PerformEnd{Error: e})
}

// PerformClose encodes a PerformClose frame with an optional error.
func PerformClose(e *encoding.Error) ([]byte, error) {
	return EncodeFrame(FrameAMQP, 0, &frames.PerformClose{Error: e})
}

// AMQPProto is the frame type passed to FrameCallback() for the initial protocal handshake.
type AMQPProto struct {
	frames.FrameBody
}

// KeepAlive is the frame type passed to FrameCallback() for keep-alive frames.
type KeepAlive struct {
	frames.FrameBody
}

type frameHeader frames.Header

func (f frameHeader) Marshal(wr *buffer.Buffer) error {
	wr.AppendUint32(f.Size)
	wr.AppendByte(f.DataOffset)
	wr.AppendByte(f.FrameType)
	wr.AppendUint16(f.Channel)
	return nil
}

// FrameType indicates the type of frame (copied from sasl.go)
type FrameType uint8

const (
	FrameAMQP FrameType = 0x0
)

// EncodeFrame encodes the specified frame to be sent over the wire.
func EncodeFrame(t FrameType, remoteChannel uint16, f frames.FrameBody) ([]byte, error) {
	bodyBuf := buffer.New([]byte{})
	if err := encoding.Marshal(bodyBuf, f); err != nil {
		return nil, err
	}
	// create the frame header, needs size of the body plus itself
	header := frameHeader{
		Size:       uint32(bodyBuf.Len()) + 8,
		DataOffset: 2,
		FrameType:  uint8(t),
		Channel:    remoteChannel,
	}
	headerBuf := buffer.New([]byte{})
	if err := encoding.Marshal(headerBuf, header); err != nil {
		return nil, err
	}
	// concatenate header + body
	raw := headerBuf.Detach()
	raw = append(raw, bodyBuf.Detach()...)
	return raw, nil
}

func decodeFrame(b []byte) (frames.FrameBody, error) {
	if len(b) > 3 && b[0] == 'A' && b[1] == 'M' && b[2] == 'Q' && b[3] == 'P' {
		return &AMQPProto{}, nil
	}
	buf := buffer.New(b)
	header, err := frames.ParseHeader(buf)
	if err != nil {
		return nil, err
	}
	bodySize := int64(header.Size - frames.HeaderSize)
	if bodySize == 0 {
		// keep alive frame
		return &KeepAlive{}, nil
	}
	// parse the frame
	b, ok := buf.Next(bodySize)
	if !ok {
		return nil, err
	}
	return frames.ParseBody(buffer.New(b))
}
