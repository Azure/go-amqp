package amqp

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/fake"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/stretchr/testify/require"
)

func TestSessionClose(t *testing.T) {
	channelNum := uint16(0)
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return newResponse(fake.ProtoHeader(fake.ProtoAMQP))
		case *frames.PerformOpen:
			return newResponse(fake.PerformOpen("container"))
		case *frames.PerformBegin:
			b, err := fake.PerformBegin(uint16(channelNum), remoteChannel)
			if err != nil {
				return fake.Response{}, err
			}
			channelNum++
			return fake.Response{Payload: b}, nil
		case *frames.PerformEnd:
			// channelNum was incremented
			b, err := fake.PerformEnd(channelNum-1, nil)
			if err != nil {
				return fake.Response{}, err
			}
			channelNum--
			return fake.Response{Payload: b}, nil
		case *frames.PerformClose:
			return newResponse(fake.PerformClose(nil))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder, fake.NetConnOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		session, err := client.NewSession(ctx, nil)
		cancel()
		require.NoErrorf(t, err, "iteration %d", i)
		require.Equalf(t, uint16(0), session.channel, "iteration %d", i)
		require.Equalf(t, channelNum-1, session.remoteChannel, "iteration %d", i)
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		err = session.Close(ctx)
		cancel()
		require.NoErrorf(t, err, "iteration %d", i)
	}
	require.NoError(t, client.Close())
}

func TestSessionServerClose(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return newResponse(fake.ProtoHeader(fake.ProtoAMQP))
		case *frames.PerformOpen:
			return newResponse(fake.PerformOpen("container"))
		case *frames.PerformBegin:
			return newResponse(fake.PerformBegin(0, remoteChannel))
		case *frames.PerformEnd:
			return fake.Response{}, nil // swallow
		case *frames.PerformClose:
			return fake.Response{}, nil // swallow
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder, fake.NetConnOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	// initiate server-side closing of session
	fr, err := fake.PerformEnd(0, &encoding.Error{Condition: "closing", Description: "server side close"})
	require.NoError(t, err)
	netConn.SendFrame(fr)
	// wait a bit for connReader to read from the mock
	time.Sleep(100 * time.Millisecond)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = session.Close(ctx)
	cancel()
	require.Error(t, err)
	var sessionErr *SessionError
	require.ErrorAs(t, err, &sessionErr)
	require.NotNil(t, sessionErr.RemoteErr)
	require.Equal(t, ErrCond("closing"), sessionErr.RemoteErr.Condition)
	require.Equal(t, "server side close", sessionErr.RemoteErr.Description)
	require.NoError(t, client.Close())
}

func TestSessionCloseTimeout(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return newResponse(fake.ProtoHeader(fake.ProtoAMQP))
		case *frames.PerformOpen:
			return newResponse(fake.PerformOpen("container"))
		case *frames.PerformBegin:
			return newResponse(fake.PerformBegin(0, remoteChannel))
		case *frames.PerformEnd:
			b, err := fake.PerformEnd(0, nil)
			if err != nil {
				return fake.Response{}, err
			}
			// introduce a delay to trigger session close timeout
			return fake.Response{Payload: b, WriteDelay: 1 * time.Second}, nil
		case *frames.PerformClose:
			return newResponse(fake.PerformClose(nil))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder, fake.NetConnOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = session.Close(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = session.Close(ctx)
	cancel()
	var sessionErr *SessionError
	require.ErrorAs(t, err, &sessionErr)
	require.Contains(t, sessionErr.Error(), context.DeadlineExceeded.Error())

	require.NoError(t, client.Close())
}

func TestConnCloseSessionClose(t *testing.T) {
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled), fake.NetConnOptions{
		ChunkSize: 8,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	require.NoError(t, client.Close())
	// closing the connection should close all sessions
	select {
	case <-session.done:
		// session was closed
	case <-time.After(500 * time.Millisecond):
		t.Fatal("session wasn't closed")
	}

	rcv, err := session.NewReceiver(context.Background(), "blah", nil)
	require.Nil(t, rcv)
	var connErr *ConnError
	require.ErrorAs(t, err, &connErr)
}

func TestSessionNewReceiverBadOptionFails(t *testing.T) {
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled), fake.NetConnOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	recv, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		Properties: map[string]any{
			"": "bad_key",
		},
	})
	cancel()
	require.Error(t, err)
	require.Nil(t, recv)
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = session.Close(ctx)
	cancel()
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

func TestSessionNewReceiverMismatchedLinkName(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return newResponse(fake.ProtoHeader(fake.ProtoAMQP))
		case *frames.PerformOpen:
			return newResponse(fake.PerformOpen("container"))
		case *frames.PerformBegin:
			return newResponse(fake.PerformBegin(0, remoteChannel))
		case *frames.PerformEnd:
			return newResponse(fake.PerformEnd(0, nil))
		case *frames.PerformAttach:
			return newResponse(fake.ReceiverAttach(0, "wrong_name", 0, ReceiverSettleModeFirst, nil))
		case *frames.PerformClose:
			return newResponse(fake.PerformClose(nil))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder, fake.NetConnOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	recv, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		Credit: 10,
	})
	cancel()
	require.Error(t, err)
	require.Nil(t, recv)
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = session.Close(ctx)
	cancel()
	require.Error(t, err)
	require.NoError(t, client.Close())
}

func TestSessionNewSenderBadOptionFails(t *testing.T) {
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled), fake.NetConnOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	snd, err := session.NewSender(ctx, "target", &SenderOptions{
		Properties: map[string]any{
			"": "bad_key",
		},
	})
	cancel()
	require.Error(t, err)
	require.Nil(t, snd)
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = session.Close(ctx)
	cancel()
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

func TestSessionNewSenderMismatchedLinkName(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return newResponse(fake.ProtoHeader(fake.ProtoAMQP))
		case *frames.PerformOpen:
			return newResponse(fake.PerformOpen("container"))
		case *frames.PerformBegin:
			return newResponse(fake.PerformBegin(0, remoteChannel))
		case *frames.PerformEnd:
			return newResponse(fake.PerformEnd(0, nil))
		case *frames.PerformAttach:
			return newResponse(fake.SenderAttach(0, "wrong_name", 0, SenderSettleModeUnsettled))
		case *frames.PerformClose:
			return newResponse(fake.PerformClose(nil))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder, fake.NetConnOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	snd, err := session.NewSender(ctx, "target", nil)
	cancel()
	require.Error(t, err)
	require.Nil(t, snd)
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = session.Close(ctx)
	cancel()
	require.Error(t, err)
	require.NoError(t, client.Close())
}

func TestSessionNewSenderDuplicateLinks(t *testing.T) {
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled), fake.NetConnOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	snd, err := session.NewSender(ctx, "target", &SenderOptions{
		Name: "test",
	})
	cancel()
	require.NoError(t, err)
	require.NotNil(t, snd)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	snd, err = session.NewSender(ctx, "target", &SenderOptions{
		Name: "test",
	})
	cancel()
	require.Error(t, err)
	require.Nil(t, snd)
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = session.Close(ctx)
	cancel()
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

func TestSessionNewSenderMaxHandles(t *testing.T) {
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled), fake.NetConnOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, &SessionOptions{MaxLinks: 1})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	snd, err := session.NewSender(ctx, "target", &SenderOptions{
		Name: "test1",
	})
	cancel()
	require.NoError(t, err)
	require.NotNil(t, snd)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	snd, err = session.NewSender(ctx, "target", &SenderOptions{
		Name: "test2",
	})
	cancel()
	require.Error(t, err)
	require.Nil(t, snd)
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = session.Close(ctx)
	cancel()
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

func TestSessionUnexpectedFrame(t *testing.T) {
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled), fake.NetConnOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	// this frame causes the session to terminate
	b, err := fake.EncodeFrame(frames.TypeSASL, 0, &frames.SASLMechanisms{})
	require.NoError(t, err)
	netConn.SendFrame(b)

	// sleep for a bit so that the session mux has time to process the invalid frame before we close
	time.Sleep(50 * time.Millisecond)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	err = session.Close(ctx)
	cancel()
	require.Error(t, err)
	var sessionErr *SessionError
	require.ErrorAs(t, err, &sessionErr)
	require.NotNil(t, sessionErr.inner)
	require.ErrorContains(t, err, "unexpected frame *frames.SASLMechanisms")
	require.NoError(t, client.Close())
}

func TestSessionInvalidFlowFrame(t *testing.T) {
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled), fake.NetConnOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	// NextIncomingID cannot be nil once the session has been established
	b, err := fake.EncodeFrame(frames.TypeAMQP, 0, frames.NewPerformFlow())
	require.NoError(t, err)
	netConn.SendFrame(b)

	// wait a bit for connReader to read from the mock
	time.Sleep(100 * time.Millisecond)
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = session.Close(ctx)
	cancel()
	require.Error(t, err)
	require.NoError(t, client.Close())
}

func TestSessionFlowFrameWithEcho(t *testing.T) {
	nextIncomingID := uint32(1)
	const nextOutgoingID = 2
	echo := make(chan struct{})
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		switch tt := req.(type) {
		case *fake.AMQPProto:
			return newResponse(fake.ProtoHeader(fake.ProtoAMQP))
		case *frames.PerformOpen:
			return newResponse(fake.PerformOpen("container"))
		case *frames.PerformBegin:
			return newResponse(fake.PerformBegin(0, remoteChannel))
		case *frames.PerformFlow:
			defer func() { close(echo) }()
			// here we receive the echo.  verify state
			if id := *tt.NextIncomingID; id != nextOutgoingID {
				return fake.Response{}, fmt.Errorf("unexpected NextIncomingID %d", id)
			}
			if id := tt.NextOutgoingID; id != 0 {
				return fake.Response{}, fmt.Errorf("unexpected NextOutgoingID %d", id)
			}
			if w := tt.IncomingWindow; w != defaultWindow {
				return fake.Response{}, fmt.Errorf("unexpected IncomingWindow %d", w)
			}
			if w := tt.OutgoingWindow; w != defaultWindow {
				return fake.Response{}, fmt.Errorf("unexpected OutgoingWindow %d", w)
			}
			return fake.Response{}, nil
		case *frames.PerformEnd:
			return newResponse(fake.PerformEnd(0, nil))
		case *frames.PerformClose:
			return newResponse(fake.PerformClose(nil))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder, fake.NetConnOptions{
		ChunkSize: 10,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	f := frames.NewPerformFlow()
	f.NextIncomingID = &nextIncomingID
	f.IncomingWindow = 100
	f.OutgoingWindow = 100
	f.NextOutgoingID = nextOutgoingID
	f.Echo = true

	b, err := fake.EncodeFrame(frames.TypeAMQP, 0, f)
	require.NoError(t, err)
	netConn.SendFrame(b)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = session.Close(ctx)
	cancel()
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

func TestSessionInvalidAttachDeadlock(t *testing.T) {
	var enqueueFrames func()
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return newResponse(fake.ProtoHeader(fake.ProtoAMQP))
		case *frames.PerformOpen:
			return newResponse(fake.PerformOpen("container"))
		case *frames.PerformBegin:
			return newResponse(fake.PerformBegin(0, remoteChannel))
		case *frames.PerformEnd:
			return newResponse(fake.PerformEnd(0, nil))
		case *frames.PerformAttach:
			enqueueFrames()
			return fake.Response{}, nil
		case *frames.PerformClose:
			return newResponse(fake.PerformClose(nil))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder, fake.NetConnOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	enqueueFrames = func() {
		// send an invalid attach response
		b, err := fake.EncodeFrame(frames.TypeAMQP, 0, &frames.PerformAttach{
			Name: "mismatched",
			Role: encoding.RoleReceiver,
		})
		require.NoError(t, err)
		netConn.SendFrame(b)
		// now follow up with a detach frame
		b, err = fake.EncodeFrame(frames.TypeAMQP, 0, &frames.PerformDetach{
			Error: &encoding.Error{
				Condition:   "boom",
				Description: "failed",
			},
		})
		require.NoError(t, err)
		netConn.SendFrame(b)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	snd, err := session.NewSender(ctx, "target", nil)
	cancel()
	require.Error(t, err)
	require.Nil(t, snd)
	require.NoError(t, client.Close())
}

func TestNewSessionContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return newResponse(fake.ProtoHeader(fake.ProtoAMQP))
		case *frames.PerformOpen:
			return newResponse(fake.PerformOpen("container"))
		case *frames.PerformClose:
			return newResponse(fake.PerformClose(nil))
		case *frames.PerformBegin:
			cancel()
			// swallow frame to prevent non-determinism of cancellation
			return fake.Response{}, nil
		case *fake.KeepAlive:
			return fake.Response{}, nil
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder, fake.NetConnOptions{})

	newCtx, newCancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(newCtx, netConn, nil)
	newCancel()
	require.NoError(t, err)

	session, err := client.NewSession(ctx, nil)

	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, session)
}

func TestSessionReceiveTransferNoHandle(t *testing.T) {
	conn := fake.NewNetConn(receiverFrameHandlerNoUnhandled(0, ReceiverSettleModeFirst), fake.NetConnOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	// send transfer when there's no link
	b, err := fake.PerformTransfer(0, 0, 1, []byte("message 1"))
	require.NoError(t, err)
	conn.SendFrame(b)

	// wait for the messages to "arrive"
	time.Sleep(time.Second)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	var sessionErr *SessionError
	require.ErrorAs(t, session.Close(ctx), &sessionErr)
	require.Contains(t, sessionErr.Error(), "transfer frame with unknown link handle")
	cancel()
}

func TestSessionReceiveDetachrNoHandle(t *testing.T) {
	conn := fake.NewNetConn(receiverFrameHandlerNoUnhandled(0, ReceiverSettleModeFirst), fake.NetConnOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	// send transfer when there's no link
	b, err := fake.PerformDetach(0, 0, nil)
	require.NoError(t, err)
	conn.SendFrame(b)

	// wait for the messages to "arrive"
	time.Sleep(time.Second)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	var sessionErr *SessionError
	require.ErrorAs(t, session.Close(ctx), &sessionErr)
	require.Contains(t, sessionErr.Error(), "detach frame with unknown link handle")
	cancel()
}
