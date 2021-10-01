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

// NewConnection creates a new instance of MockConnection.
// Responder is invoked by Write when a frame is received.
// Return a nil slice/nil error to swallow the frame.
// Return a non-nil error to simulate a write error.
func NewConnection(resp func(frames.FrameBody) ([]byte, error)) *MockConnection {
	return &MockConnection{
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

// MockConnection is a mock connection that satisfies the net.Conn interface.
type MockConnection struct {
	resp      func(frames.FrameBody) ([]byte, error)
	readDL    *time.Timer
	readData  chan []byte
	readClose chan struct{}
	closed    bool
}

///////////////////////////////////////////////////////
// following methods are for the net.Conn interface
///////////////////////////////////////////////////////

// NOTE: Read, Write, and Close are all called by separate goroutines!

// Read is invoked by conn.connReader to recieve frame data.
// It blocks until Write or Close are called, or the read
// deadline expires which will return an error.
func (m *MockConnection) Read(b []byte) (n int, err error) {
	select {
	case <-m.readClose:
		return 0, errors.New("mock connection was closed")
	default:
		// not closed yet
	}

	select {
	case <-m.readClose:
		return 0, errors.New("mock connection was closed")
	case <-m.readDL.C:
		return 0, errors.New("mock connection read deadline exceeded")
	case rd := <-m.readData:
		return copy(b, rd), nil
	}
}

// Write is invoked by conn.connWriter when we're being sent frame
// data.  Every call to Write will invoke the responder callback that
// must reply with one of three possibilities.
//  1. an encoded frame and nil error
//  2. a non-nil error to similate a write failure
//  3. a nil slice and nil error indicating the frame should be ignored
func (m *MockConnection) Write(b []byte) (n int, err error) {
	select {
	case <-m.readClose:
		return 0, errors.New("mock connection was closed")
	default:
		// not closed yet
	}

	frame, err := decodeFrame(b)
	if err != nil {
		return 0, err
	}
	resp, err := m.resp(frame)
	if err != nil {
		return 0, err
	}
	if resp != nil {
		m.readData <- resp
	}
	return len(b), nil
}

// Close is called by conn.close when conn.mux unwinds.
func (m *MockConnection) Close() error {
	if m.closed {
		return errors.New("double close")
	}
	m.closed = true
	close(m.readClose)
	return nil
}

func (m *MockConnection) LocalAddr() net.Addr {
	return &net.IPAddr{
		IP: net.IPv4(127, 0, 0, 2),
	}
}

func (m *MockConnection) RemoteAddr() net.Addr {
	return &net.IPAddr{
		IP: net.IPv4(127, 0, 0, 2),
	}
}

func (m *MockConnection) SetDeadline(t time.Time) error {
	return errors.New("not used")
}

func (m *MockConnection) SetReadDeadline(t time.Time) error {
	// called by conn.connReader before calling Read
	// stop the last timer if available
	if m.readDL != nil && !m.readDL.Stop() {
		<-m.readDL.C
	}
	m.readDL = time.NewTimer(time.Until(t))
	return nil
}

func (m *MockConnection) SetWriteDeadline(t time.Time) error {
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
	return encodeFrame(frameAMQP, &frames.PerformOpen{ContainerID: "test"})
}

// PerformBegin appends a PerformBegin frame with the specified remote channel ID.
// This frame is needed when making a call to Client.NewSession().
func PerformBegin(remoteChannel uint16) ([]byte, error) {
	return encodeFrame(frameAMQP, &frames.PerformBegin{
		RemoteChannel:  &remoteChannel,
		NextOutgoingID: 1,
		IncomingWindow: 5000,
		OutgoingWindow: 1000,
		HandleMax:      math.MaxInt16,
	})
}

// ReceiverAttach appends a PerformAttach frame with the specified values.
// This frame is needed when making a call to Session.NewReceiver().
func ReceiverAttach(linkName string, linkHandle uint32, mode encoding.ReceiverSettleMode) ([]byte, error) {
	return encodeFrame(frameAMQP, &frames.PerformAttach{
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
func PerformTransfer(linkHandle, deliveryID uint32, payload []byte) ([]byte, error) {
	format := uint32(0)
	payloadBuf := &buffer.Buffer{}
	encoding.WriteDescriptor(payloadBuf, encoding.TypeCodeApplicationData)
	err := encoding.WriteBinary(payloadBuf, payload)
	if err != nil {
		return nil, err
	}
	return encodeFrame(frameAMQP, &frames.PerformTransfer{
		Handle:        linkHandle,
		DeliveryID:    &deliveryID,
		DeliveryTag:   []byte("tag"),
		MessageFormat: &format,
		Payload:       payloadBuf.Detach(),
	})
}

// PerformDisposition appends a PerformDisposition frame with the specified values.
// The deliveryID MUST match the deliveryID value specified in PerformTransfer.
func PerformDisposition(deliveryID uint32, state encoding.DeliveryState) ([]byte, error) {
	return encodeFrame(frameAMQP, &frames.PerformDisposition{
		Role:    encoding.RoleSender,
		First:   deliveryID,
		Settled: true,
		State:   state,
	})
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
type frameType uint8

const (
	frameAMQP frameType = 0x0
)

func encodeFrame(t frameType, f frames.FrameBody) ([]byte, error) {
	bodyBuf := buffer.New([]byte{})
	if err := encoding.Marshal(bodyBuf, f); err != nil {
		return nil, err
	}
	// create the frame header, needs size of the body plus itself
	header := frameHeader{
		Size:       uint32(bodyBuf.Len()) + 8,
		DataOffset: 2,
		FrameType:  uint8(t),
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
