package amqp

import (
	"context"
	"fmt"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/fake"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/stretchr/testify/require"
)

func sendInitialFlowFrame(t require.TestingT, netConn *fake.NetConn, handle uint32, credit uint32) {
	nextIncoming := uint32(0)
	count := uint32(0)
	available := uint32(0)
	b, err := fake.EncodeFrame(frames.TypeAMQP, 0, &frames.PerformFlow{
		NextIncomingID: &nextIncoming,
		IncomingWindow: 1000000,
		OutgoingWindow: 1000000,
		NextOutgoingID: nextIncoming + 1,
		Handle:         &handle,
		DeliveryCount:  &count,
		LinkCredit:     &credit,
		Available:      &available,
	})
	require.NoError(t, err)
	netConn.SendFrame(b)
}

// standard frame handler for connecting/disconnecting etc.
// returns nil, nil for unhandled frames.
func senderFrameHandler(ssm encoding.SenderSettleMode) func(frames.FrameBody) ([]byte, error) {
	return func(req frames.FrameBody) ([]byte, error) {
		switch tt := req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		case *frames.PerformBegin:
			return fake.PerformBegin(0)
		case *frames.PerformEnd:
			return fake.PerformEnd(0, nil)
		case *frames.PerformAttach:
			return fake.SenderAttach(0, tt.Name, 0, ssm)
		case *frames.PerformDetach:
			return fake.PerformDetach(0, 0, nil)
		default:
			return nil, nil
		}
	}
}

// similar to senderFrameHandler but returns an error on unhandled frames
func senderFrameHandlerNoUnhandled(ssm encoding.SenderSettleMode) func(frames.FrameBody) ([]byte, error) {
	return func(req frames.FrameBody) ([]byte, error) {
		b, err := senderFrameHandler(ssm)(req)
		if b == nil && err == nil {
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
		return b, err
	}
}

// standard frame handler for connecting/disconnecting etc.
// returns nil, nil for unhandled frames.
func receiverFrameHandler(rsm encoding.ReceiverSettleMode) func(frames.FrameBody) ([]byte, error) {
	return func(req frames.FrameBody) ([]byte, error) {
		switch tt := req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		case *frames.PerformBegin:
			return fake.PerformBegin(0)
		case *frames.PerformEnd:
			return fake.PerformEnd(0, nil)
		case *frames.PerformAttach:
			return fake.ReceiverAttach(0, tt.Name, 0, rsm, tt.Source.Filter)
		case *frames.PerformDetach:
			return fake.PerformDetach(0, 0, nil)
		default:
			return nil, nil
		}
	}
}

// similar to receiverFrameHandler but returns an error on unhandled frames
// NOTE: consumes flow frames
func receiverFrameHandlerNoUnhandled(rsm encoding.ReceiverSettleMode) func(frames.FrameBody) ([]byte, error) {
	return func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(rsm)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch req.(type) {
		case *frames.PerformFlow, *fake.KeepAlive:
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
}

// this is the same API as Session.NewReceiver() but with support for adding test hooks
func newReceiverWithHooks(ctx context.Context, s *Session, source string, opts *ReceiverOptions, hooks receiverTestHooks) (*Receiver, error) {
	r, err := newReceiver(source, s, opts)
	if err != nil {
		return nil, err
	}
	if err = r.attach(ctx); err != nil {
		return nil, err
	}

	go r.mux(hooks)

	return r, nil
}
