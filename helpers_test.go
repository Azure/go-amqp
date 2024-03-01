package amqp

import (
	"fmt"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/fake"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/stretchr/testify/require"
)

func newResponse(b []byte, err error) (fake.Response, error) {
	if err != nil {
		return fake.Response{}, err
	}
	return fake.Response{Payload: b}, nil
}

func sendInitialFlowFrame(t require.TestingT, channel uint16, netConn *fake.NetConn, handle uint32, credit uint32) {
	nextIncoming := uint32(0)
	count := uint32(0)
	available := uint32(0)
	b, err := fake.EncodeFrame(frames.TypeAMQP, channel, &frames.PerformFlow{
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
// returns zero-value, nil for unhandled frames.
func senderFrameHandler(channel uint16, ssm encoding.SenderSettleMode) func(uint16, frames.FrameBody) (fake.Response, error) {
	return func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		switch tt := req.(type) {
		case *fake.AMQPProto:
			return newResponse(fake.ProtoHeader(fake.ProtoAMQP))
		case *frames.PerformOpen:
			return newResponse(fake.PerformOpen("container"))
		case *frames.PerformClose:
			return newResponse(fake.PerformClose(nil))
		case *frames.PerformBegin:
			return newResponse(fake.PerformBegin(channel, remoteChannel))
		case *frames.PerformEnd:
			return newResponse(fake.PerformEnd(channel, nil))
		case *frames.PerformAttach:
			return newResponse(fake.SenderAttach(channel, tt.Name, 0, ssm))
		case *frames.PerformDetach:
			return newResponse(fake.PerformDetach(channel, 0, nil))
		default:
			return fake.Response{}, nil
		}
	}
}

// similar to senderFrameHandler but returns an error on unhandled frames
func senderFrameHandlerNoUnhandled(channel uint16, ssm encoding.SenderSettleMode) func(uint16, frames.FrameBody) (fake.Response, error) {
	return func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		resp, err := senderFrameHandler(channel, ssm)(remoteChannel, req)
		if resp.Payload == nil && err == nil {
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
		return resp, err
	}
}

// standard frame handler for connecting/disconnecting etc.
// returns zero-value, nil for unhandled frames.
func receiverFrameHandler(channel uint16, rsm encoding.ReceiverSettleMode) func(uint16, frames.FrameBody) (fake.Response, error) {
	return func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		switch tt := req.(type) {
		case *fake.AMQPProto:
			return newResponse(fake.ProtoHeader(fake.ProtoAMQP))
		case *frames.PerformOpen:
			return newResponse(fake.PerformOpen("container"))
		case *frames.PerformClose:
			return newResponse(fake.PerformClose(nil))
		case *frames.PerformBegin:
			return newResponse(fake.PerformBegin(channel, remoteChannel))
		case *frames.PerformEnd:
			return newResponse(fake.PerformEnd(channel, nil))
		case *frames.PerformAttach:
			return newResponse(fake.ReceiverAttach(channel, tt.Name, 0, rsm, tt.Source.Filter))
		case *frames.PerformDetach:
			return newResponse(fake.PerformDetach(channel, 0, nil))
		default:
			return fake.Response{}, nil
		}
	}
}

// similar to receiverFrameHandler but returns an error on unhandled frames
// NOTE: consumes flow frames
func receiverFrameHandlerNoUnhandled(channel uint16, rsm encoding.ReceiverSettleMode) func(uint16, frames.FrameBody) (fake.Response, error) {
	return func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		resp, err := receiverFrameHandler(channel, rsm)(remoteChannel, req)
		if resp.Payload != nil || err != nil {
			return resp, err
		}
		switch req.(type) {
		case *frames.PerformFlow, *fake.KeepAlive:
			return fake.Response{}, nil
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
}
