package amqp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/Azure/go-amqp/internal/mocks"
	"github.com/stretchr/testify/require"
)

func TestReceiverInvalidOptions(t *testing.T) {
	conn := mocks.NewNetConn(receiverFrameHandlerNoUnhandled(ModeFirst))
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleMode(3).Ptr(),
	})
	cancel()
	require.Error(t, err)
	require.Nil(t, r)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err = session.NewReceiver(ctx, "source", &ReceiverOptions{
		Durability: Durability(3),
	})
	cancel()
	require.Error(t, err)
	require.Nil(t, r)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err = session.NewReceiver(ctx, "source", &ReceiverOptions{
		ExpiryPolicy: ExpiryPolicy("not-a-real-policy"),
	})
	cancel()
	require.Error(t, err)
	require.Nil(t, r)
}

func TestReceiverMethodsNoReceive(t *testing.T) {
	const linkName = "test"
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			require.Equal(t, DurabilityUnsettledState, ff.Target.Durable)
			require.Equal(t, ExpiryNever, ff.Target.ExpiryPolicy)
			require.Equal(t, uint32(300), ff.Target.Timeout)
			return mocks.ReceiverAttach(0, linkName, 0, ModeFirst, nil)
		case *frames.PerformFlow, *mocks.KeepAlive:
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	const sourceAddr = "thesource"
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, sourceAddr, &ReceiverOptions{
		Name:          linkName,
		Durability:    DurabilityUnsettledState,
		ExpiryPolicy:  ExpiryNever,
		ExpiryTimeout: 300,
	})
	cancel()
	require.NoError(t, err)
	require.Equal(t, sourceAddr, r.Address())
	require.Equal(t, linkName, r.LinkName())
	require.Nil(t, r.LinkSourceFilterValue("nofilter"))
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, r.Close(ctx))
	cancel()
}

func TestReceiverLinkSourceFilter(t *testing.T) {
	conn := mocks.NewNetConn(receiverFrameHandlerNoUnhandled(ModeFirst))
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	const filterName = "myfilter"
	const filterExp = "filter_exp"
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "ignored", &ReceiverOptions{
		DynamicAddress: true,
		Filters: []LinkFilter{
			LinkFilterSource(filterName, 0, filterExp),
		},
	})
	cancel()
	require.NoError(t, err)
	require.Equal(t, "test", r.Address())
	require.NotEmpty(t, r.LinkName())
	require.Equal(t, filterExp, r.LinkSourceFilterValue(filterName))
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, r.Close(ctx))
	cancel()
}

func TestReceiverOnClosed(t *testing.T) {
	conn := mocks.NewNetConn(receiverFrameHandlerNoUnhandled(ModeFirst))
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	errChan := make(chan error)
	go func() {
		_, err := r.Receive(context.Background())
		errChan <- err
	}()

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, r.Close(ctx))
	cancel()
	if err = <-errChan; !errors.Is(err, ErrLinkClosed) {
		t.Fatalf("unexpected error %v", err)
	}
	_, err = r.Receive(context.Background())
	if !errors.Is(err, ErrLinkClosed) {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestReceiverOnSessionClosed(t *testing.T) {
	conn := mocks.NewNetConn(receiverFrameHandlerNoUnhandled(ModeFirst))
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	errChan := make(chan error)
	go func() {
		_, err := r.Receive(context.Background())
		errChan <- err
	}()

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, session.Close(ctx))
	cancel()
	if err = <-errChan; !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("unexpected error %v", err)
	}
	_, err = r.Receive(context.Background())
	if !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestReceiverOnConnClosed(t *testing.T) {
	conn := mocks.NewNetConn(receiverFrameHandlerNoUnhandled(ModeFirst))
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	errChan := make(chan error)
	go func() {
		_, err := r.Receive(context.Background())
		errChan <- err
	}()

	require.NoError(t, client.Close())
	err = <-errChan
	var connErr *ConnectionError
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	_, err = r.Receive(context.Background())
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
}

func TestReceiverOnDetached(t *testing.T) {
	conn := mocks.NewNetConn(receiverFrameHandlerNoUnhandled(ModeFirst))
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	errChan := make(chan error)
	go func() {
		_, err := r.Receive(context.Background())
		errChan <- err
	}()

	// initiate a server-side detach
	const (
		errcon  = "detaching"
		errdesc = "server side detach"
	)
	b, err := mocks.PerformDetach(0, 0, &Error{Condition: errcon, Description: errdesc})
	require.NoError(t, err)
	conn.SendFrame(b)

	var de *DetachError
	if !errors.As(<-errChan, &de) {
		t.Fatalf("unexpected error type %T", err)
	}
	require.Equal(t, encoding.ErrorCondition(errcon), de.RemoteError.Condition)
	require.Equal(t, errdesc, de.RemoteError.Description)
	require.NoError(t, client.Close())
	_, err = r.Receive(context.Background())
	if !errors.As(err, &de) {
		t.Fatalf("unexpected error type %T", err)
	}
}

func TestReceiveInvalidMessage(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ModeFirst)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch req.(type) {
		case *frames.PerformFlow, *mocks.KeepAlive:
			return nil, nil
		case *frames.PerformDisposition:
			return mocks.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateAccepted{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	msgChan := make(chan *Message)
	errChan := make(chan error)
	go func() {
		msg, err := r.Receive(context.Background())
		msgChan <- msg
		errChan <- err
	}()

	// missing DeliveryID
	fr, err := mocks.EncodeFrame(mocks.FrameAMQP, 0, &frames.PerformTransfer{
		Handle: linkHandle,
	})
	require.NoError(t, err)
	conn.SendFrame(fr)

	require.Nil(t, <-msgChan)
	var amqpErr *Error
	if err = <-errChan; !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}
	require.Equal(t, ErrorNotAllowed, amqpErr.Condition)

	_, err = r.Receive(context.Background())
	if !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}

	// missing MessageFormat
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err = session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)
	go func() {
		msg, err := r.Receive(context.Background())
		msgChan <- msg
		errChan <- err
	}()
	fr, err = mocks.EncodeFrame(mocks.FrameAMQP, 0, &frames.PerformTransfer{
		DeliveryID: &deliveryID,
		Handle:     linkHandle,
	})
	require.NoError(t, err)
	conn.SendFrame(fr)

	require.Nil(t, <-msgChan)
	if err = <-errChan; !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}
	require.Equal(t, ErrorNotAllowed, amqpErr.Condition)

	_, err = r.Receive(context.Background())
	if !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}

	// missing delivery tag
	format := uint32(0)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err = session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)
	go func() {
		msg, err := r.Receive(context.Background())
		msgChan <- msg
		errChan <- err
	}()
	fr, err = mocks.EncodeFrame(mocks.FrameAMQP, 0, &frames.PerformTransfer{
		DeliveryID:    &deliveryID,
		Handle:        linkHandle,
		MessageFormat: &format,
	})
	require.NoError(t, err)
	conn.SendFrame(fr)

	require.Nil(t, <-msgChan)
	if err = <-errChan; !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}
	require.Equal(t, ErrorNotAllowed, amqpErr.Condition)

	_, err = r.Receive(context.Background())
	if !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}

	require.NoError(t, client.Close())
}

func TestReceiveSuccessModeFirst(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ModeFirst)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return mocks.PerformTransfer(0, linkHandle, deliveryID, []byte("hello"))
			}
			// ignore future flow frames as we have no response
			return nil, nil
		case *frames.PerformDisposition:
			return mocks.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateAccepted{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ModeFirst.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	require.NoError(t, err)
	if c := r.link.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// wait for the link to unpause as credit should now be available
	require.NoError(t, waitForLink(r.link, false))
	// link credit should be 1
	if c := r.link.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	// subsequent dispositions should have no effect
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

func TestReceiveSuccessModeSecondAccept(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ModeSecond)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return mocks.PerformTransfer(0, linkHandle, deliveryID, []byte("hello"))
			}
			// ignore future flow frames as we have no response
			return nil, nil
		case *frames.PerformDisposition:
			if _, ok := ff.State.(*encoding.StateAccepted); !ok {
				return nil, fmt.Errorf("unexpected State %T", ff.State)
			}
			return mocks.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateAccepted{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	require.NoError(t, err)
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// wait for the link to pause as we've consumed all available credit
	require.NoError(t, waitForLink(r.link, true))
	// link credit must be zero since we only started with 1
	if c := r.link.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	require.NoError(t, err)
	if c := r.link.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	require.Equal(t, true, msg.settled)
	// perform a dummy receive with short timeout to trigger flow
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = r.Receive(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	// wait for the link to unpause as credit should now be available
	require.NoError(t, waitForLink(r.link, false))
	// link credit should be back to 1
	if c := r.link.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	// subsequent dispositions should have no effect
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

func TestReceiveSuccessModeSecondAcceptOnClosedLink(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ModeSecond)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return mocks.PerformTransfer(0, linkHandle, deliveryID, []byte("hello"))
			}
			// ignore future flow frames as we have no response
			return nil, nil
		case *frames.PerformDisposition:
			if _, ok := ff.State.(*encoding.StateAccepted); !ok {
				return nil, fmt.Errorf("unexpected State %T", ff.State)
			}
			return mocks.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateAccepted{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	require.NoError(t, err)
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// wait for the link to pause as we've consumed all available credit
	require.NoError(t, waitForLink(r.link, true))
	// link credit must be zero since we only started with 1
	if c := r.link.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}

	require.NoError(t, r.Close(context.Background()))

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	require.ErrorIs(t, err, ErrLinkClosed)
}

func TestReceiveSuccessModeSecondReject(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ModeSecond)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return mocks.PerformTransfer(0, linkHandle, deliveryID, []byte("hello"))
			}
			// ignore future flow frames as we have no response
			return nil, nil
		case *frames.PerformDisposition:
			if _, ok := ff.State.(*encoding.StateRejected); !ok {
				return nil, fmt.Errorf("unexpected State %T", ff.State)
			}
			return mocks.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateRejected{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	require.NoError(t, err)
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// wait for the link to pause as we've consumed all available credit
	require.NoError(t, waitForLink(r.link, true))
	// link credit must be zero since we only started with 1
	if c := r.link.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.RejectMessage(ctx, msg, nil)
	cancel()
	require.NoError(t, err)
	if c := r.link.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// perform a dummy receive with short timeout to trigger flow
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = r.Receive(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	// wait for the link to unpause as credit should now be available
	require.NoError(t, waitForLink(r.link, false))
	// link credit should be back to 1
	if c := r.link.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	require.NoError(t, client.Close())
}

func TestReceiveSuccessModeSecondRelease(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ModeSecond)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return mocks.PerformTransfer(0, linkHandle, deliveryID, []byte("hello"))
			}
			// ignore future flow frames as we have no response
			return nil, nil
		case *frames.PerformDisposition:
			if _, ok := ff.State.(*encoding.StateReleased); !ok {
				return nil, fmt.Errorf("unexpected State %T", ff.State)
			}
			return mocks.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateReleased{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	require.NoError(t, err)
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// wait for the link to pause as we've consumed all available credit
	require.NoError(t, waitForLink(r.link, true))
	// link credit must be zero since we only started with 1
	if c := r.link.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.ReleaseMessage(ctx, msg)
	cancel()
	require.NoError(t, err)
	if c := r.link.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// perform a dummy receive with short timeout to trigger flow
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = r.Receive(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	// wait for the link to unpause as credit should now be available
	require.NoError(t, waitForLink(r.link, false))
	// link credit should be back to 1
	if c := r.link.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	require.NoError(t, client.Close())
}

func TestReceiveSuccessModeSecondModify(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ModeSecond)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return mocks.PerformTransfer(0, linkHandle, deliveryID, []byte("hello"))
			}
			// ignore future flow frames as we have no response
			return nil, nil
		case *frames.PerformDisposition:
			var mod *encoding.StateModified
			var ok bool
			if mod, ok = ff.State.(*encoding.StateModified); !ok {
				return nil, fmt.Errorf("unexpected State %T", ff.State)
			}
			if v := mod.MessageAnnotations["some"]; v != "value" {
				return nil, fmt.Errorf("unexpected annotation value %v", v)
			}
			return mocks.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateModified{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	require.NoError(t, err)
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// wait for the link to pause as we've consumed all available credit
	require.NoError(t, waitForLink(r.link, true))
	// link credit must be zero since we only started with 1
	if c := r.link.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.ModifyMessage(ctx, msg, &ModifyMessageOptions{
		UndeliverableHere: true,
		Annotations: Annotations{
			"some": "value",
		},
	})
	cancel()
	require.NoError(t, err)
	if c := r.link.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// perform a dummy receive with short timeout to trigger flow
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = r.Receive(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	// wait for the link to unpause as credit should now be available
	require.NoError(t, waitForLink(r.link, false))
	// link credit should be back to 1
	if c := r.link.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	require.NoError(t, client.Close())
}

func TestReceiverPrefetch(t *testing.T) {
	messagesCh := make(chan Message, 1)

	receiver := &Receiver{
		link: &link{
			Messages:      messagesCh,
			ReceiverReady: make(chan struct{}),
		},
	}

	// if there are no cached messages we just return immediately - no error, no message.
	msg := receiver.Prefetched()
	require.Nil(t, msg)

	messagesCh <- Message{
		ApplicationProperties: map[string]interface{}{
			"prop": "hello",
		},
		settled: true,
	}

	require.NotEmpty(t, messagesCh)
	msg = receiver.Prefetched()

	require.EqualValues(t, "hello", msg.ApplicationProperties["prop"].(string))
	require.Empty(t, messagesCh)
}

func TestReceiveMultiFrameMessageSuccess(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ModeSecond)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow, *mocks.KeepAlive:
			return nil, nil
		case *frames.PerformDisposition:
			if _, ok := ff.State.(*encoding.StateAccepted); !ok {
				return nil, fmt.Errorf("unexpected State %T", ff.State)
			}
			return mocks.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateAccepted{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	msgChan := make(chan *Message)
	errChan := make(chan error)
	go func() {
		msg, err := r.Receive(context.Background())
		msgChan <- msg
		errChan <- err
	}()
	// send multi-frame message
	payload := []byte("this should be split into three frames for a multi-frame transfer message")
	require.NoError(t, conn.SendMultiFrameTransfer(0, linkHandle, deliveryID, payload, nil))
	msg := <-msgChan
	require.NoError(t, <-errChan)
	// validate message content
	result := []byte{}
	for i := range msg.Data {
		result = append(result, msg.Data[i]...)
	}
	require.Equal(t, payload, result)
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// wait for the link to pause as we've consumed all available credit
	require.NoError(t, waitForLink(r.link, true))
	// link credit must be zero since we only started with 1
	if c := r.link.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	require.NoError(t, err)
	if c := r.link.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	require.Equal(t, true, msg.settled)
	// perform a dummy receive with short timeout to trigger flow
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = r.Receive(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	// wait for the link to unpause as credit should now be available
	require.NoError(t, waitForLink(r.link, false))
	// link credit should be back to 1
	if c := r.link.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	require.NoError(t, client.Close())
}

func TestReceiveInvalidMultiFrameMessage(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ModeSecond)(req)
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
	conn := mocks.NewNetConn(responder)
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	msgChan := make(chan *Message)
	errChan := make(chan error)
	go func() {
		msg, err := r.Receive(context.Background())
		msgChan <- msg
		errChan <- err
	}()
	// send multi-frame message
	payload := []byte("this should be split into two frames for a multi-frame transfer")

	// mismatched DeliveryID
	require.NoError(t, conn.SendMultiFrameTransfer(0, linkHandle, deliveryID, payload, func(i int, fr *frames.PerformTransfer) {
		if i == 0 {
			return
		}
		// modify the second frame with mismatched data
		badID := uint32(123)
		fr.DeliveryID = &badID
	}))
	msg := <-msgChan
	require.Nil(t, msg)
	var amqpErr *Error
	if err = <-errChan; !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}
	require.Equal(t, ErrorNotAllowed, amqpErr.Condition)

	// mismatched MessageFormat
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err = session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	go func() {
		msg, err := r.Receive(context.Background())
		msgChan <- msg
		errChan <- err
	}()
	require.NoError(t, conn.SendMultiFrameTransfer(0, linkHandle, deliveryID, payload, func(i int, fr *frames.PerformTransfer) {
		if i == 0 {
			return
		}
		// modify the second frame with mismatched data
		badFormat := uint32(123)
		fr.MessageFormat = &badFormat
	}))
	msg = <-msgChan
	require.Nil(t, msg)
	if err = <-errChan; !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}
	require.Equal(t, ErrorNotAllowed, amqpErr.Condition)

	// mismatched DeliveryTag
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err = session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	go func() {
		msg, err := r.Receive(context.Background())
		msgChan <- msg
		errChan <- err
	}()
	require.NoError(t, conn.SendMultiFrameTransfer(0, linkHandle, deliveryID, payload, func(i int, fr *frames.PerformTransfer) {
		if i == 0 {
			return
		}
		// modify the second frame with mismatched data
		fr.DeliveryTag = []byte("bad_tag")
	}))
	msg = <-msgChan
	require.Nil(t, msg)
	if err = <-errChan; !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}
	require.Equal(t, ErrorNotAllowed, amqpErr.Condition)

	require.NoError(t, client.Close())
}

func TestReceiveMultiFrameMessageAborted(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ModeSecond)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow, *mocks.KeepAlive:
			return nil, nil
		case *frames.PerformDisposition:
			if _, ok := ff.State.(*encoding.StateAccepted); !ok {
				return nil, fmt.Errorf("unexpected State %T", ff.State)
			}
			return mocks.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateAccepted{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	msgChan := make(chan *Message)
	errChan := make(chan error)
	go func() {
		msg, err := r.Receive(context.Background())
		errChan <- err
		msgChan <- msg
	}()
	// send multi-frame message
	payload := []byte("this should be split into three frames for a multi-frame transfer message")
	require.NoError(t, conn.SendMultiFrameTransfer(0, linkHandle, deliveryID, payload, func(i int, fr *frames.PerformTransfer) {
		if i < 2 {
			return
		}
		// set abort flag on the last frame
		fr.Aborted = true
	}))
	// we shouldn't have received any message at this point, now send a single-frame message
	payload = []byte("single message")
	b, err := mocks.PerformTransfer(0, linkHandle, deliveryID+1, payload)
	require.NoError(t, err)
	conn.SendFrame(b)
	require.NoError(t, <-errChan)
	msg := <-msgChan
	require.Equal(t, payload, msg.GetData())
	require.NoError(t, client.Close())
}

func TestReceiveMessageTooBig(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ModeSecond)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				bigPayload := make([]byte, 256)
				return mocks.PerformTransfer(0, linkHandle, deliveryID, bigPayload)
			}
			// ignore future flow frames as we have no response
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ModeSecond.Ptr(),
		MaxMessageSize: 128,
	})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	require.Nil(t, msg)
	var amqpErr *Error
	if !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}
	require.Equal(t, ErrorMessageSizeExceeded, amqpErr.Condition)
	require.NoError(t, client.Close())
}

func TestReceiveSuccessAcceptFails(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ModeSecond)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return mocks.PerformTransfer(0, linkHandle, deliveryID, []byte("hello"))
			}
			// ignore future flow frames as we have no response
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	require.NoError(t, err)
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// wait for the link to pause as we've consumed all available credit
	require.NoError(t, waitForLink(r.link, true))
	// link credit must be zero since we only started with 1
	if c := r.link.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	// close client before accepting the message
	require.NoError(t, client.Close())
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	var connErr *ConnectionError
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
}

func TestReceiverDispositionBatcherTimer(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ModeSecond)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return mocks.PerformTransfer(0, linkHandle, deliveryID, []byte("hello"))
			}
			// ignore future flow frames as we have no response
			return nil, nil
		case *frames.PerformDisposition:
			if _, ok := ff.State.(*encoding.StateAccepted); !ok {
				return nil, fmt.Errorf("unexpected State %T", ff.State)
			}
			return mocks.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateAccepted{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		Batching:       true,
		BatchMaxAge:    time.Second,
		Credit:         2,
		SettlementMode: ModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	require.NoError(t, err)
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	require.NoError(t, err)
	if c := r.link.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	require.Equal(t, 0, r.inFlight.len())
	require.Equal(t, true, msg.settled)
	require.NoError(t, client.Close())
}

func TestReceiverDispositionBatcherFull(t *testing.T) {
	const credit = 3
	const linkHandle = 0
	deliveryID := uint32(1)
	acceptCount := 0
	allAccepted := make(chan struct{})
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ModeSecond)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow, *mocks.KeepAlive:
			return nil, nil
		case *frames.PerformDisposition:
			if _, ok := ff.State.(*encoding.StateAccepted); !ok {
				return nil, fmt.Errorf("unexpected State %T", ff.State)
			}
			if ff.Last == nil || *ff.Last == ff.First {
				acceptCount++
			} else {
				acceptCount += int(*ff.Last)
			}
			if acceptCount == credit {
				close(allAccepted)
			}
			return mocks.PerformDisposition(encoding.RoleSender, 0, ff.First, ff.Last, &encoding.StateAccepted{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		Batching:       true,
		BatchMaxAge:    time.Second,
		Credit:         credit,
		SettlementMode: ModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	wg := &sync.WaitGroup{}
	wg.Add(credit)
	for i := 0; i < credit; i++ {
		b, err := mocks.PerformTransfer(0, linkHandle, deliveryID, []byte("hello"))
		require.NoError(t, err)
		conn.SendFrame(b)
		deliveryID++
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		msg, err := r.Receive(ctx)
		cancel()
		require.NoError(t, err)
		go func() {
			require.NoError(t, r.AcceptMessage(context.Background(), msg))
			require.Equal(t, true, msg.settled)
			wg.Done()
		}()
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-allAccepted:
		// all messages were settled
	case <-ctx.Done():
		t.Fatalf("not all messages were settled within the allotted time: %d", acceptCount)
	}
	wg.Wait()
	require.Equal(t, 0, r.inFlight.len())
	require.NoError(t, client.Close())
}

func TestReceiverDispositionBatcherRelease(t *testing.T) {
	const credit = 3
	const linkHandle = 0
	deliveryID := uint32(1)
	acceptCount := 0
	allAccepted := make(chan struct{})
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ModeSecond)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow, *mocks.KeepAlive:
			return nil, nil
		case *frames.PerformDisposition:
			if ff.Last == nil || *ff.Last == ff.First {
				acceptCount++
			} else {
				acceptCount += int(*ff.Last)
			}
			if acceptCount == credit {
				close(allAccepted)
			}
			return mocks.PerformDisposition(encoding.RoleSender, 0, ff.First, ff.Last, &encoding.StateAccepted{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		Batching:       true,
		BatchMaxAge:    time.Second,
		Credit:         credit,
		SettlementMode: ModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	wg := &sync.WaitGroup{}
	wg.Add(credit)
	for i := 0; i < credit; i++ {
		b, err := mocks.PerformTransfer(0, linkHandle, deliveryID, []byte("hello"))
		require.NoError(t, err)
		conn.SendFrame(b)
		deliveryID++
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
		msg, err := r.Receive(ctx)
		cancel()
		require.NoError(t, err)
		go func(count int) {
			if count == credit-1 {
				require.NoError(t, r.AcceptMessage(context.Background(), msg))
			} else {
				require.NoError(t, r.ReleaseMessage(context.Background(), msg))
			}
			require.Equal(t, true, msg.settled)
			wg.Done()
		}(i)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-allAccepted:
		// all messages were settled
	case <-ctx.Done():
		t.Fatalf("not all messages were settled within the allotted time: %d", acceptCount)
	}
	wg.Wait()
	require.Equal(t, 0, r.inFlight.len())
	require.NoError(t, client.Close())
}

func TestReceiverCloseOnUnsettledWithPending(t *testing.T) {
	conn := mocks.NewNetConn(receiverFrameHandlerNoUnhandled(ModeFirst))
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	// first message exhausts the link credit
	b, err := mocks.PerformTransfer(0, 0, 1, []byte("message 1"))
	require.NoError(t, err)
	conn.SendFrame(b)

	// wait for the messages to "arrive"
	time.Sleep(time.Second)

	// now close the receiver without reading any of the messages
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, r.Close(ctx))
	cancel()
}

func TestReceiverConnReaderError(t *testing.T) {
	conn := mocks.NewNetConn(receiverFrameHandlerNoUnhandled(ModeFirst))
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	errChan := make(chan error)
	go func() {
		_, err := r.Receive(context.Background())
		errChan <- err
	}()

	// trigger some kind of error
	conn.ReadErr <- errors.New("failed")

	err = <-errChan
	var connErr *ConnectionError
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	_, err = r.Receive(context.Background())
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	require.Error(t, conn.Close())
}

func TestReceiverConnWriterError(t *testing.T) {
	conn := mocks.NewNetConn(receiverFrameHandlerNoUnhandled(ModeFirst))
	client, err := New(conn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	errChan := make(chan error)
	go func() {
		_, err := r.Receive(context.Background())
		errChan <- err
	}()

	conn.WriteErr <- errors.New("failed")
	// trigger the write error
	conn.SendKeepAlive()

	err = <-errChan
	var connErr *ConnectionError
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	_, err = r.Receive(context.Background())
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	require.Error(t, conn.Close())
}

// TODO: add unit tests for manual credit management
