package amqp

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/buffer"
	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/fake"
	"github.com/Azure/go-amqp/internal/frames"
)

// nestedValue marshals to a self-nesting described-type byte sequence, letting
// a test embed a deeply nested value inside an otherwise valid performative.
type nestedValue struct{ pad int }

func (n nestedValue) Marshal(wr *buffer.Buffer) error {
	wr.Append([]byte{0x00, 0x53, 0x00})
	wr.Append(bytes.Repeat([]byte{0x00}, n.pad))
	return nil
}

// openResponder returns a fake server that completes the proto handshake and
// answers the client's Open with the supplied raw Open frame.
func openResponder(openFrame []byte) func(uint16, frames.FrameBody) (fake.Response, error) {
	return func(_ uint16, fr frames.FrameBody) (fake.Response, error) {
		switch fr.(type) {
		case *fake.AMQPProto:
			ph, _ := fake.ProtoHeader(fake.ProtoAMQP)
			return fake.Response{Payload: ph}, nil
		case *frames.PerformOpen:
			return fake.Response{Payload: openFrame}, nil
		default:
			return fake.Response{}, nil
		}
	}
}

func maliciousOpen(t *testing.T, pad int) []byte {
	t.Helper()
	raw, err := fake.EncodeFrame(frames.TypeAMQP, 0, &frames.PerformOpen{
		ContainerID: "test-broker",
		Properties:  map[encoding.Symbol]any{"p": nestedValue{pad: pad}},
	})
	if err != nil {
		t.Fatalf("encode open: %v", err)
	}
	return raw
}

// A crafted Open that is small enough to pass the frame-size check but nested
// beyond maxNestingDepth must be rejected by the decoder's depth guard during
// connection open.
func TestNewConnRejectsDeeplyNestedOpenFrame(t *testing.T) {
	conn := fake.NewNetConn(openResponder(maliciousOpen(t, 512)), fake.NetConnOptions{ChunkSize: 512})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := NewConn(ctx, conn, nil)
	if client != nil {
		_ = client.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "maximum depth") {
		t.Fatalf("expected a nesting-depth error during open, got: %v", err)
	}
}

// A frame larger than the negotiated max frame size must be rejected by
// readFrame before it reaches the type decoder.
func TestNewConnRejectsOversizedFrame(t *testing.T) {
	conn := fake.NewNetConn(openResponder(maliciousOpen(t, 1<<20)), fake.NetConnOptions{ChunkSize: 512})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := NewConn(ctx, conn, nil)
	if client != nil {
		_ = client.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "maximum frame size") {
		t.Fatalf("expected a max-frame-size error, got: %v", err)
	}
}

// Sanity: a normal Open still completes connection open cleanly.
func TestNewConnAcceptsBenignOpen(t *testing.T) {
	benign, _ := fake.PerformOpen("benign-broker")
	conn := fake.NewNetConn(openResponder(benign), fake.NetConnOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := NewConn(ctx, conn, nil)
	if err != nil {
		t.Fatalf("benign open should succeed, got: %v", err)
	}
	_ = client.Close()
}
