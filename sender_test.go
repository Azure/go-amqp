package amqp

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/fake"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/stretchr/testify/require"
)

func TestSenderInvalidOptions(t *testing.T) {
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled))

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
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch tt := req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformBegin:
			return fake.PerformBegin(0, remoteChannel)
		case *frames.PerformEnd:
			return fake.PerformEnd(0, nil)
		case *frames.PerformAttach:
			require.Equal(t, DurabilityUnsettledState, tt.Source.Durable)
			require.Equal(t, ExpiryPolicyNever, tt.Source.ExpiryPolicy)
			require.Equal(t, uint32(300), tt.Source.Timeout)
			return fake.SenderAttach(0, tt.Name, 0, SenderSettleModeUnsettled)
		case *frames.PerformDetach:
			return fake.PerformDetach(0, 0, nil)
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

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
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled))

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
	var linkErr *LinkError
	require.ErrorAs(t, snd.Send(context.Background(), NewMessage([]byte("failed")), nil), &linkErr)
	require.Equal(t, "amqp: link closed", linkErr.Error())
	require.NoError(t, client.Close())
}

func TestSenderSendOnSessionClosed(t *testing.T) {
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled))

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
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled))

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
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled))

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
	b, err := fake.PerformDetach(0, 0, &Error{Condition: errcon, Description: errdesc})
	require.NoError(t, err)
	netConn.SendFrame(b)
	// sending on a detached link returns a LinkError
	err = snd.Send(context.Background(), NewMessage([]byte("failed")), nil)
	var linkErr *LinkError
	require.ErrorAs(t, err, &linkErr)
	require.Equal(t, ErrCond(errcon), linkErr.RemoteErr.Condition)
	require.Equal(t, errdesc, linkErr.RemoteErr.Description)
	require.NoError(t, client.Close())
}

func TestSenderCloseTimeout(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch tt := req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformBegin:
			return fake.PerformBegin(0, remoteChannel)
		case *frames.PerformEnd:
			return fake.PerformEnd(0, nil)
		case *frames.PerformAttach:
			return fake.SenderAttach(0, tt.Name, tt.Handle, SenderSettleModeUnsettled)
		case *frames.PerformDetach:
			// sleep to trigger sender close timeout
			time.Sleep(1 * time.Second)
			return fake.PerformDetach(0, tt.Handle, nil)
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)
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
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = snd.Close(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = snd.Close(ctx)
	cancel()
	var linkErr *LinkError
	require.ErrorAs(t, err, &linkErr)
	require.Contains(t, linkErr.Error(), context.DeadlineExceeded.Error())
	require.NoError(t, client.Close())
}

func TestSenderAttachError(t *testing.T) {
	detachAck := make(chan bool, 1)
	var enqueueFrames func(string)
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch tt := req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformBegin:
			return fake.PerformBegin(0, remoteChannel)
		case *frames.PerformEnd:
			return fake.PerformEnd(0, nil)
		case *frames.PerformAttach:
			enqueueFrames(tt.Name)
			return nil, nil
		case *frames.PerformDetach:
			// we don't need to respond to the ack
			detachAck <- true
			return nil, nil
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)
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
		b, err := fake.EncodeFrame(frames.TypeAMQP, 0, &frames.PerformAttach{
			Name: n,
			Role: encoding.RoleReceiver,
		})
		require.NoError(t, err)
		netConn.SendFrame(b)
		// now follow up with a detach frame
		b, err = fake.EncodeFrame(frames.TypeAMQP, 0, &frames.PerformDetach{
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
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled))

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
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		b, err := senderFrameHandler(0, SenderSettleModeUnsettled)(remoteChannel, req)
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
			return fake.PerformDisposition(encoding.RoleReceiver, 0, *tt.DeliveryID, nil, &encoding.StateAccepted{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

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

	sendInitialFlowFrame(t, 0, netConn, 0, 100)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	require.NoError(t, snd.Send(ctx, NewMessage([]byte("test")), nil))
	cancel()

	require.NoError(t, client.Close())
}

func TestSenderSendSettled(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		b, err := senderFrameHandler(0, SenderSettleModeSettled)(remoteChannel, req)
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
	netConn := fake.NewNetConn(responder)

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

	sendInitialFlowFrame(t, 0, netConn, 0, 100)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	require.NoError(t, snd.Send(ctx, NewMessage([]byte("test")), nil))
	cancel()

	require.NoError(t, client.Close())
}

func TestSenderSendSettledModeMixed(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		b, err := senderFrameHandler(0, SenderSettleModeSettled)(remoteChannel, req)
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
	netConn := fake.NewNetConn(responder)

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

	sendInitialFlowFrame(t, 0, netConn, 0, 100)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	require.NoError(t, snd.Send(ctx, NewMessage([]byte("test")), &SendOptions{Settled: true}))
	cancel()

	require.NoError(t, client.Close())
}

func TestSenderSendSettledError(t *testing.T) {
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled))

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
		SettlementMode: SenderSettleModeUnsettled.Ptr(),
	})
	cancel()
	require.NoError(t, err)

	sendInitialFlowFrame(t, 0, netConn, 0, 100)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	require.Error(t, snd.Send(ctx, NewMessage([]byte("test")), &SendOptions{Settled: true}))
	cancel()

	require.NoError(t, client.Close())
}

func TestSenderSendRejectedNoDetach(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch tt := req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformBegin:
			return fake.PerformBegin(0, remoteChannel)
		case *frames.PerformEnd:
			return fake.PerformEnd(0, nil)
		case *frames.PerformAttach:
			return fake.SenderAttach(0, tt.Name, 0, SenderSettleModeUnsettled)
		case *frames.PerformTransfer:
			// reject first delivery
			if *tt.DeliveryID == 0 {
				return fake.PerformDisposition(encoding.RoleReceiver, 0, *tt.DeliveryID, nil, &encoding.StateRejected{
					Error: &Error{
						Condition:   "rejected",
						Description: "didn't like it",
					},
				})
			}
			return fake.PerformDisposition(encoding.RoleReceiver, 0, *tt.DeliveryID, nil, &encoding.StateAccepted{})
		case *frames.PerformDetach:
			return fake.PerformDetach(0, 0, nil)
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

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

	sendInitialFlowFrame(t, 0, netConn, 0, 100)

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
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		b, err := senderFrameHandler(0, SenderSettleModeUnsettled)(remoteChannel, req)
		if err != nil || b != nil {
			return b, err
		}
		switch req.(type) {
		case *frames.PerformTransfer:
			return fake.PerformDetach(0, 0, &Error{
				Condition:   "detached",
				Description: "server exploded",
			})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

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

	sendInitialFlowFrame(t, 0, netConn, 0, 100)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = snd.Send(ctx, NewMessage([]byte("test")), nil)
	cancel()
	var linkErr *LinkError
	require.ErrorAs(t, err, &linkErr)
	require.NotNil(t, linkErr.RemoteErr)
	require.Equal(t, ErrCond("detached"), linkErr.RemoteErr.Condition)

	require.NoError(t, client.Close())
}

func TestSenderSendTimeout(t *testing.T) {
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled))

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
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch tt := req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformBegin:
			return fake.PerformBegin(0, remoteChannel)
		case *frames.PerformEnd:
			return fake.PerformEnd(0, nil)
		case *frames.PerformAttach:
			mode := SenderSettleModeUnsettled
			return fake.EncodeFrame(frames.TypeAMQP, 0, &frames.PerformAttach{
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
			return fake.PerformDisposition(encoding.RoleReceiver, 0, *tt.DeliveryID, nil, &encoding.StateAccepted{})
		case *frames.PerformDetach:
			return fake.PerformDetach(0, 0, nil)
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

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

	sendInitialFlowFrame(t, 0, netConn, 0, 100)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	require.Error(t, snd.Send(ctx, NewMessage([]byte("test message that's too big")), nil))
	cancel()

	require.NoError(t, client.Close())
}

func TestSenderSendTagTooBig(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		b, err := senderFrameHandler(0, SenderSettleModeUnsettled)(remoteChannel, req)
		if err != nil || b != nil {
			return b, err
		}
		switch tt := req.(type) {
		case *frames.PerformTransfer:
			return fake.PerformDisposition(encoding.RoleReceiver, 0, *tt.DeliveryID, nil, &encoding.StateAccepted{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

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

	sendInitialFlowFrame(t, 0, netConn, 0, 100)

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
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch tt := req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.EncodeFrame(frames.TypeAMQP, 0, &frames.PerformOpen{
				ChannelMax:   65535,
				ContainerID:  "container",
				IdleTimeout:  time.Minute,
				MaxFrameSize: maxReceiverFrameSize, // really small max frame size
			})
		case *frames.PerformBegin:
			return fake.PerformBegin(0, remoteChannel)
		case *frames.PerformEnd:
			return fake.PerformEnd(0, nil)
		case *frames.PerformAttach:
			return fake.SenderAttach(0, tt.Name, 0, SenderSettleModeUnsettled)
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
			return fake.PerformDisposition(encoding.RoleReceiver, 0, deliveryID, nil, &encoding.StateAccepted{})
		case *frames.PerformDetach:
			return fake.PerformDetach(0, 0, nil)
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

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

	sendInitialFlowFrame(t, 0, netConn, 0, 100)

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
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled))

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
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled))

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

	sendInitialFlowFrame(t, 0, netConn, 0, 100)

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
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		b, err := senderFrameHandler(0, SenderSettleModeUnsettled)(remoteChannel, req)
		if b != nil || err != nil {
			return b, err
		}
		switch tt := req.(type) {
		case *frames.PerformFlow:
			require.False(t, tt.Echo)
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
	netConn := fake.NewNetConn(responder)

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
	b, err := fake.EncodeFrame(frames.TypeAMQP, 0, &frames.PerformFlow{
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
	var senderCount uint32
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch fr := req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		case *frames.PerformBegin:
			return fake.PerformBegin(0, remoteChannel)
		case *frames.PerformAttach:
			if senderCount == 0 {
				senderCount++
				// wait a bit so NewSender times out
				time.Sleep(100 * time.Millisecond)
				return fake.SenderAttach(0, fr.Name, fr.Handle, SenderSettleModeMixed)
			}
			return fake.SenderAttach(0, fr.Name, fr.Handle, SenderSettleModeMixed)
		case *frames.PerformDetach:
			return fake.PerformDetach(0, fr.Handle, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	// first sender fails due to deadline exceeded
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	snd, err := session.NewSender(ctx, "target", nil)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, snd)

	// should have one sender to clean up
	require.Len(t, session.abandonedLinks, 1)
	require.Len(t, session.linksByKey, 1)

	// creating a new sender cleans up the old one
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	snd, err = session.NewSender(ctx, "target", nil)
	cancel()
	require.NoError(t, err)
	require.NotNil(t, snd)
	require.Empty(t, session.abandonedLinks)
	require.Len(t, session.linksByKey, 1)
}

func TestNewSenderWriteError(t *testing.T) {
	detachAck := make(chan struct{})
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformBegin:
			return fake.PerformBegin(0, remoteChannel)
		case *frames.PerformAttach:
			return nil, errors.New("write error")
		case *frames.PerformDetach:
			close(detachAck)
			return fake.PerformEnd(0, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

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

func TestNewSenderContextCancelled(t *testing.T) {
	senderCtx, senderCancel := context.WithCancel(context.Background())

	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		case *frames.PerformBegin:
			return fake.PerformBegin(0, remoteChannel)
		case *frames.PerformEnd:
			return fake.PerformEnd(0, nil)
		case *frames.PerformAttach:
			// cancel the context to trigger early exit and clean-up
			senderCancel()
			return nil, nil
		case *frames.PerformDetach:
			return fake.PerformDetach(0, 0, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

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
}

func TestSenderUnexpectedFrame(t *testing.T) {
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled))

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

	// senders don't receive transfer frames
	fr, err := fake.PerformTransfer(0, 0, 1, []byte("boom"))
	require.NoError(t, err)
	netConn.SendFrame(fr)

	// sender should now be dead
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	err = snd.Send(ctx, NewMessage([]byte("hello")), nil)
	cancel()

	var linkErr *LinkError
	require.ErrorAs(t, err, &linkErr)
	require.NotNil(t, linkErr.inner)
	require.ErrorContains(t, err, "unexpected frame *frames.PerformTransfer")
	require.NoError(t, client.Close())
}
