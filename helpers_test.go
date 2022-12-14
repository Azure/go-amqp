package amqp

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/Azure/go-amqp/internal/mocks"
	"github.com/stretchr/testify/require"
)

func sendInitialFlowFrame(t *testing.T, netConn *mocks.NetConn, handle uint32, credit uint32) {
	nextIncoming := uint32(0)
	count := uint32(0)
	available := uint32(0)
	b, err := mocks.EncodeFrame(mocks.FrameAMQP, 0, &frames.PerformFlow{
		NextIncomingID: &nextIncoming,
		IncomingWindow: 1000,
		OutgoingWindow: 1000,
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
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformClose:
			return mocks.PerformClose(nil)
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformEnd:
			return mocks.PerformEnd(0, nil)
		case *frames.PerformAttach:
			return mocks.SenderAttach(0, tt.Name, 0, ssm)
		case *frames.PerformDetach:
			return mocks.PerformDetach(0, 0, nil)
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
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformClose:
			return mocks.PerformClose(nil)
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformEnd:
			return mocks.PerformEnd(0, nil)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, tt.Name, 0, rsm, tt.Source.Filter)
		case *frames.PerformDetach:
			return mocks.PerformDetach(0, 0, nil)
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
		case *frames.PerformFlow, *mocks.KeepAlive:
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
}

// helper to wait for a link to pause/resume
// returns an error if it times out waiting
func waitForReceiver(r *Receiver, paused bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		credit := atomic.LoadUint32(&r.l.availableCredit)
		// waiting for the link to pause means its credit has been consumed
		if (paused && credit == 0) || (!paused && credit > 0) {
			return nil
		} else if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-r.l.detached:
			return fmt.Errorf("link detached: detachErr %v, error %v", r.l.detachError, r.l.err)
		case <-time.After(50 * time.Millisecond):
			// try again
		}
	}
}
