package amqp

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/Azure/go-amqp/internal/mocks"
	"github.com/stretchr/testify/require"
)

func TestSenderInvalidOptions(t *testing.T) {
	netConn := mocks.NewNetConn(senderFrameHandlerNoUnhandled(SenderSettleModeUnsettled))

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
		SettlementMode: SenderSettleMode(3).Ptr(),
	})
	cancel()
	require.Error(t, err)
	require.Nil(t, snd)
}

func TestSenderMethodsNoSend(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch tt := req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformEnd:
			return mocks.PerformEnd(0, nil)
		case *frames.PerformAttach:
			require.Equal(t, DurabilityUnsettledState, tt.Source.Durable)
			require.Equal(t, ExpiryPolicyNever, tt.Source.ExpiryPolicy)
			require.Equal(t, uint32(300), tt.Source.Timeout)
			return mocks.SenderAttach(0, tt.Name, 0, SenderSettleModeUnsettled)
		case *frames.PerformDetach:
			return mocks.PerformDetach(0, 0, nil)
		case *frames.PerformClose:
			return mocks.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	const (
		linkAddr = "addr1"
		linkName = "test1"
	)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	snd, err := session.NewSender(ctx, linkAddr, &SenderOptions{
		Name:          linkName,
		Durability:    DurabilityUnsettledState,
		ExpiryPolicy:  ExpiryPolicyNever,
		ExpiryTimeout: 300,
	})
	cancel()
	require.NoError(t, err)
	require.NotNil(t, snd)
	require.Equal(t, linkAddr, snd.Address())
	require.Equal(t, linkName, snd.LinkName())
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	require.NoError(t, snd.Close(ctx))
	cancel()
	require.NoError(t, client.Close())
}

func TestSenderSendOnClosed(t *testing.T) {
	netConn := mocks.NewNetConn(senderFrameHandlerNoUnhandled(SenderSettleModeUnsettled))

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
	require.NoError(t, err)
	require.NotNil(t, snd)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	require.NoError(t, snd.Close(ctx))
	cancel()
	// sending on a closed sender returns ErrLinkClosed
	var detachErr *DetachError
	require.ErrorAs(t, snd.Send(context.Background(), NewMessage([]byte("failed")), nil), &detachErr)
	require.Equal(t, "amqp: link closed", detachErr.Error())
	require.NoError(t, client.Close())
}

func TestSenderSendOnSessionClosed(t *testing.T) {
	netConn := mocks.NewNetConn(senderFrameHandlerNoUnhandled(SenderSettleModeUnsettled))

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
	require.NoError(t, err)
	require.NotNil(t, snd)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	require.NoError(t, session.Close(ctx))
	cancel()
	// sending on a closed sender returns SessionError
	var sessionErr *SessionError
	err = snd.Send(context.Background(), NewMessage([]byte("failed")), nil)
	require.ErrorAs(t, err, &sessionErr)
	var amqpErr *Error
	// there should be no inner error when closed on our side
	require.False(t, errors.As(err, &amqpErr))
	require.NoError(t, client.Close())
}

func TestSenderSendOnConnClosed(t *testing.T) {
	netConn := mocks.NewNetConn(senderFrameHandlerNoUnhandled(SenderSettleModeUnsettled))

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
	require.NoError(t, err)
	require.NotNil(t, snd)

	require.NoError(t, client.Close())
	// sending on a closed sender returns a ConnectionError
	err = snd.Send(context.Background(), NewMessage([]byte("failed")), nil)
	var connErr *ConnError
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	require.NoError(t, client.Close())
}

func TestSenderSendOnDetached(t *testing.T) {
	netConn := mocks.NewNetConn(senderFrameHandlerNoUnhandled(SenderSettleModeUnsettled))

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
	require.NoError(t, err)
	require.NotNil(t, snd)
	// initiate a server-side detach
	const (
		errcon  = "detaching"
		errdesc = "server side detach"
	)
	b, err := mocks.PerformDetach(0, 0, &Error{Condition: errcon, Description: errdesc})
	require.NoError(t, err)
	netConn.SendFrame(b)
	// sending on a detached link returns a DetachError
	err = snd.Send(context.Background(), NewMessage([]byte("failed")), nil)
	var de *DetachError
	require.ErrorAs(t, err, &de)
	var detachErr *DetachError
	require.ErrorAs(t, de, &detachErr)
	require.Equal(t, ErrCond(errcon), detachErr.RemoteErr.Condition)
	require.Equal(t, errdesc, detachErr.RemoteErr.Description)
	require.NoError(t, client.Close())
}

func TestSenderAttachError(t *testing.T) {
	detachAck := make(chan bool, 1)
	var enqueueFrames func(string)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch tt := req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformEnd:
			return mocks.PerformEnd(0, nil)
		case *frames.PerformAttach:
			enqueueFrames(tt.Name)
			return nil, nil
		case *frames.PerformDetach:
			// we don't need to respond to the ack
			detachAck <- true
			return nil, nil
		case *frames.PerformClose:
			return mocks.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	const (
		errcon  = "cantattach"
		errdesc = "server side error"
	)

	enqueueFrames = func(n string) {
		// send an invalid attach response
		b, err := mocks.EncodeFrame(mocks.FrameAMQP, 0, &frames.PerformAttach{
			Name: n,
			Role: encoding.RoleReceiver,
		})
		require.NoError(t, err)
		netConn.SendFrame(b)
		// now follow up with a detach frame
		b, err = mocks.EncodeFrame(mocks.FrameAMQP, 0, &frames.PerformDetach{
			Error: &encoding.Error{
				Condition:   errcon,
				Description: errdesc,
			},
		})
		require.NoError(t, err)
		netConn.SendFrame(b)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	snd, err := session.NewSender(ctx, "target", nil)
	cancel()
	var de *Error
	if !errors.As(err, &de) {
		t.Fatalf("unexpected error type %T", err)
	}
	require.Equal(t, ErrCond(errcon), de.Condition)
	require.Equal(t, errdesc, de.Description)
	require.Nil(t, snd)
	require.Equal(t, true, <-detachAck)
	require.NoError(t, client.Close())
}

func TestSenderSendMismatchedModes(t *testing.T) {
	netConn := mocks.NewNetConn(senderFrameHandlerNoUnhandled(SenderSettleModeUnsettled))

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
		SettlementMode: SenderSettleModeSettled.Ptr(),
	})
	cancel()
	require.Error(t, err)
	require.Equal(t, "amqp: sender settlement mode \"settled\" requested, received \"unsettled\" from server", err.Error())
	require.Nil(t, snd)
	require.NoError(t, client.Close())
}

func TestSenderSendSuccess(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := senderFrameHandler(SenderSettleModeUnsettled)(req)
		if err != nil || b != nil {
			return b, err
		}
		switch tt := req.(type) {
		case *frames.PerformTransfer:
			if tt.More {
				return nil, errors.New("didn't expect more to be true")
			}
			if tt.Settled {
				return nil, errors.New("didn't expect message to be settled")
			}
			if tt.MessageFormat == nil {
				return nil, errors.New("unexpected nil MessageFormat")
			}
			if !reflect.DeepEqual([]byte{0, 83, 117, 160, 4, 116, 101, 115, 116}, tt.Payload) {
				return nil, fmt.Errorf("unexpected payload %v", tt.Payload)
			}
			return mocks.PerformDisposition(encoding.RoleReceiver, 0, *tt.DeliveryID, nil, &encoding.StateAccepted{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

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
	require.NoError(t, err)

	sendInitialFlowFrame(t, netConn, 0, 100)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	require.NoError(t, snd.Send(ctx, NewMessage([]byte("test")), nil))
	cancel()

	require.NoError(t, client.Close())
}

func TestSenderSendSettled(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := senderFrameHandler(SenderSettleModeSettled)(req)
		if err != nil || b != nil {
			return b, err
		}
		switch tt := req.(type) {
		case *frames.PerformTransfer:
			if tt.More {
				return nil, errors.New("didn't expect more to be true")
			}
			if !tt.Settled {
				return nil, errors.New("expected message to be settled")
			}
			if !reflect.DeepEqual([]byte{0, 83, 117, 160, 4, 116, 101, 115, 116}, tt.Payload) {
				return nil, fmt.Errorf("unexpected payload %v", tt.Payload)
			}
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

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
		SettlementMode: SenderSettleModeSettled.Ptr(),
	})
	cancel()
	require.NoError(t, err)

	sendInitialFlowFrame(t, netConn, 0, 100)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	require.NoError(t, snd.Send(ctx, NewMessage([]byte("test")), nil))
	cancel()

	require.NoError(t, client.Close())
}

func TestSenderSendRejected(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := senderFrameHandler(SenderSettleModeUnsettled)(req)
		if err != nil || b != nil {
			return b, err
		}
		switch tt := req.(type) {
		case *frames.PerformTransfer:
			return mocks.PerformDisposition(encoding.RoleReceiver, 0, *tt.DeliveryID, nil, &encoding.StateRejected{
				Error: &Error{
					Condition:   "rejected",
					Description: "didn't like it",
				},
			})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

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
	require.NoError(t, err)

	sendInitialFlowFrame(t, netConn, 0, 100)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = snd.Send(ctx, NewMessage([]byte("test")), nil)
	cancel()
	var deErr *DetachError
	require.ErrorAs(t, err, &deErr)
	require.NotNil(t, deErr.RemoteErr)
	require.Equal(t, ErrCond("rejected"), deErr.RemoteErr.Condition)

	// link should now be detached
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = snd.Send(ctx, NewMessage([]byte("test")), nil)
	cancel()
	if !errors.As(err, &deErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	require.NoError(t, client.Close())
}

func TestSenderSendRejectedNoDetach(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch tt := req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformEnd:
			return mocks.PerformEnd(0, nil)
		case *frames.PerformAttach:
			return mocks.SenderAttach(0, tt.Name, 0, SenderSettleModeUnsettled)
		case *frames.PerformTransfer:
			// reject first delivery
			if *tt.DeliveryID == 0 {
				return mocks.PerformDisposition(encoding.RoleReceiver, 0, *tt.DeliveryID, nil, &encoding.StateRejected{
					Error: &Error{
						Condition:   "rejected",
						Description: "didn't like it",
					},
				})
			}
			return mocks.PerformDisposition(encoding.RoleReceiver, 0, *tt.DeliveryID, nil, &encoding.StateAccepted{})
		case *frames.PerformDetach:
			return mocks.PerformDetach(0, 0, nil)
		case *frames.PerformClose:
			return mocks.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

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
		IgnoreDispositionErrors: true,
	})
	cancel()
	require.NoError(t, err)

	sendInitialFlowFrame(t, netConn, 0, 100)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = snd.Send(ctx, NewMessage([]byte("test")), nil)
	cancel()
	var asErr *Error
	if !errors.As(err, &asErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	require.Equal(t, ErrCond("rejected"), asErr.Condition)

	// link should *not* be detached
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = snd.Send(ctx, NewMessage([]byte("test")), nil)
	cancel()
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

func TestSenderSendDetached(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := senderFrameHandler(SenderSettleModeUnsettled)(req)
		if err != nil || b != nil {
			return b, err
		}
		switch req.(type) {
		case *frames.PerformTransfer:
			return mocks.PerformDetach(0, 0, &Error{
				Condition:   "detached",
				Description: "server exploded",
			})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

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
	require.NoError(t, err)

	sendInitialFlowFrame(t, netConn, 0, 100)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = snd.Send(ctx, NewMessage([]byte("test")), nil)
	cancel()
	var deErr *DetachError
	require.ErrorAs(t, err, &deErr)
	var detachErr *DetachError
	require.ErrorAs(t, deErr, &detachErr)
	require.NotNil(t, deErr.RemoteErr)
	require.Equal(t, ErrCond("detached"), deErr.RemoteErr.Condition)

	require.NoError(t, client.Close())
}

func TestSenderSendTimeout(t *testing.T) {
	netConn := mocks.NewNetConn(senderFrameHandlerNoUnhandled(SenderSettleModeUnsettled))

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
	require.NoError(t, err)

	// no credits have been issued so the send will time out
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	require.Error(t, snd.Send(ctx, NewMessage([]byte("test")), nil))
	cancel()

	require.NoError(t, client.Close())
}

func TestSenderSendMsgTooBig(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch tt := req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformEnd:
			return mocks.PerformEnd(0, nil)
		case *frames.PerformAttach:
			mode := SenderSettleModeUnsettled
			return mocks.EncodeFrame(mocks.FrameAMQP, 0, &frames.PerformAttach{
				Name:   tt.Name,
				Handle: 0,
				Role:   encoding.RoleReceiver,
				Target: &frames.Target{
					Address:      "test",
					Durable:      encoding.DurabilityNone,
					ExpiryPolicy: encoding.ExpirySessionEnd,
				},
				SenderSettleMode: &mode,
				MaxMessageSize:   16, // really small messages only
			})
		case *frames.PerformTransfer:
			return mocks.PerformDisposition(encoding.RoleReceiver, 0, *tt.DeliveryID, nil, &encoding.StateAccepted{})
		case *frames.PerformDetach:
			return mocks.PerformDetach(0, 0, nil)
		case *frames.PerformClose:
			return mocks.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

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
	require.NoError(t, err)

	sendInitialFlowFrame(t, netConn, 0, 100)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	require.Error(t, snd.Send(ctx, NewMessage([]byte("test message that's too big")), nil))
	cancel()

	require.NoError(t, client.Close())
}

func TestSenderSendTagTooBig(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := senderFrameHandler(SenderSettleModeUnsettled)(req)
		if err != nil || b != nil {
			return b, err
		}
		switch tt := req.(type) {
		case *frames.PerformTransfer:
			return mocks.PerformDisposition(encoding.RoleReceiver, 0, *tt.DeliveryID, nil, &encoding.StateAccepted{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

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
	require.NoError(t, err)

	sendInitialFlowFrame(t, netConn, 0, 100)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	msg := NewMessage([]byte("test"))
	// make the tag larger than max allowed of 32
	msg.DeliveryTag = make([]byte, 33)
	require.Error(t, snd.Send(ctx, msg, nil))
	cancel()

	require.NoError(t, client.Close())
}

func TestSenderSendMultiTransfer(t *testing.T) {
	var deliveryID uint32
	transferCount := 0
	const maxReceiverFrameSize = 128
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch tt := req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.EncodeFrame(mocks.FrameAMQP, 0, &frames.PerformOpen{
				ChannelMax:   65535,
				ContainerID:  "container",
				IdleTimeout:  time.Minute,
				MaxFrameSize: maxReceiverFrameSize, // really small max frame size
			})
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformEnd:
			return mocks.PerformEnd(0, nil)
		case *frames.PerformAttach:
			return mocks.SenderAttach(0, tt.Name, 0, SenderSettleModeUnsettled)
		case *frames.PerformTransfer:
			if tt.DeliveryID != nil {
				// deliveryID is only sent on the first transfer frame for multi-frame transfers
				if transferCount != 0 {
					return nil, fmt.Errorf("unexpected DeliveryID for frame number %d", transferCount)
				}
				deliveryID = *tt.DeliveryID
			}
			if tt.MessageFormat != nil && transferCount != 0 {
				// MessageFormat is only sent on the first transfer frame for multi-frame transfers
				return nil, fmt.Errorf("unexpected MessageFormat for frame number %d", transferCount)
			} else if tt.MessageFormat == nil && transferCount == 0 {
				return nil, errors.New("unexpected nil MessageFormat")
			}
			if tt.More {
				transferCount++
				return nil, nil
			}
			return mocks.PerformDisposition(encoding.RoleReceiver, 0, deliveryID, nil, &encoding.StateAccepted{})
		case *frames.PerformDetach:
			return mocks.PerformDetach(0, 0, nil)
		case *frames.PerformClose:
			return mocks.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

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
	require.NoError(t, err)

	sendInitialFlowFrame(t, netConn, 0, 100)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	payload := make([]byte, maxReceiverFrameSize*4)
	for i := 0; i < maxReceiverFrameSize*4; i++ {
		payload[i] = byte(i % 256)
	}
	require.NoError(t, snd.Send(ctx, NewMessage(payload), nil))
	cancel()

	// split up into 8 transfers due to transfer frame header size
	require.Equal(t, 8, transferCount)

	require.NoError(t, client.Close())
}

func TestSenderConnReaderError(t *testing.T) {
	netConn := mocks.NewNetConn(senderFrameHandlerNoUnhandled(SenderSettleModeUnsettled))

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
	require.NoError(t, err)
	require.NotNil(t, snd)

	go func() {
		// trigger some kind of error
		netConn.ReadErr <- errors.New("failed")
	}()

	err = snd.Send(context.Background(), NewMessage([]byte("failed")), nil)
	var connErr *ConnError
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}

	err = client.Close()
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
}

func TestSenderConnWriterError(t *testing.T) {
	netConn := mocks.NewNetConn(senderFrameHandlerNoUnhandled(SenderSettleModeUnsettled))

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
	require.NoError(t, err)
	require.NotNil(t, snd)

	sendInitialFlowFrame(t, netConn, 0, 100)

	// simulate some connWriter error
	netConn.WriteErr <- errors.New("failed")

	err = snd.Send(context.Background(), NewMessage([]byte("failed")), nil)
	var connErr *ConnError
	require.ErrorAs(t, err, &connErr)
	require.Equal(t, "failed", connErr.Error())

	err = client.Close()
	require.ErrorAs(t, err, &connErr)
	require.Equal(t, "failed", connErr.Error())
}

func TestSenderFlowFrameWithEcho(t *testing.T) {
	linkCredit := uint32(1)
	echo := make(chan struct{})
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := senderFrameHandler(SenderSettleModeUnsettled)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch tt := req.(type) {
		case *frames.PerformFlow:
			defer func() { close(echo) }()
			// here we receive the echo.  verify state
			if id := *tt.Handle; id != 0 {
				return nil, fmt.Errorf("unexpected Handle %d", id)
			}
			if dc := *tt.DeliveryCount; dc != 0 {
				return nil, fmt.Errorf("unexpected DeliveryCount %d", dc)
			}
			if lc := *tt.LinkCredit; lc != linkCredit {
				return nil, fmt.Errorf("unexpected LinkCredit %d", lc)
			}
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	sender, err := session.NewSender(ctx, "target", nil)
	cancel()
	require.NoError(t, err)

	nextIncomingID := uint32(1)
	b, err := mocks.EncodeFrame(mocks.FrameAMQP, 0, &frames.PerformFlow{
		Handle:         &sender.l.handle,
		NextIncomingID: &nextIncomingID,
		IncomingWindow: 100,
		OutgoingWindow: 100,
		NextOutgoingID: 1,
		LinkCredit:     &linkCredit,
		Echo:           true,
	})
	require.NoError(t, err)
	netConn.SendFrame(b)

	<-echo
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = sender.Close(ctx)
	cancel()
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

func TestNewSenderTimedOut(t *testing.T) {
	detachAck := make(chan struct{})
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			// swallow the frame so attach never gets an ack
			return nil, nil
		case *frames.PerformDetach:
			close(detachAck)
			return mocks.PerformEnd(0, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

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
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, snd)

	select {
	case <-time.After(time.Second):
		t.Fatal("didn't receive end ack")
	case <-detachAck:
		// expected
	}

	// cannot check handle count in this case as detach is asynchronous
}

func TestNewSenderWriteError(t *testing.T) {
	detachAck := make(chan struct{})
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return nil, errors.New("write error")
		case *frames.PerformDetach:
			close(detachAck)
			return mocks.PerformEnd(0, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

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
	var connErr *ConnError
	require.ErrorAs(t, err, &connErr)
	require.Equal(t, "write error", connErr.Error())
	require.Nil(t, snd)

	select {
	case <-time.After(time.Second):
		// expected
	case <-detachAck:
		t.Fatal("unexpected ack")
	}

	// cannot check handle count as this kills the connection
}

func TestNewSenderTimedOutAckTimedOut(t *testing.T) {
	detachAck := make(chan struct{})
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			// swallow the frame so attach never gets an ack
			return nil, nil
		case *frames.PerformDetach:
			close(detachAck)
			// swallow the frame so the closing goroutine never gets an ack
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)
	// fisrt session succeeds
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	snd, err := session.NewSender(ctx, "target", nil)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, snd)

	select {
	case <-time.After(time.Second):
		t.Fatal("didn't receive end ack")
	case <-detachAck:
		// expected
	}

	// cannot check handle count in this case as detach is asynchronous
}

func TestNewSenderContextCancelled(t *testing.T) {
	senderCtx, senderCancel := context.WithCancel(context.Background())

	responder := func(req frames.FrameBody) ([]byte, error) {
		switch tt := req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformEnd:
			return mocks.PerformEnd(0, nil)
		case *frames.PerformAttach:
			// cancel the context to trigger early exit and clean-up
			senderCancel()
			return mocks.SenderAttach(0, tt.Name, 0, SenderSettleModeUnsettled)
		case *frames.PerformDetach:
			return mocks.PerformDetach(0, 0, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	snd, err := session.NewSender(senderCtx, "target", nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, snd)

	// don't let the test exit before the attach frame has a chance to arrive
	time.Sleep(time.Second)
}
