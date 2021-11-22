package amqp

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/Azure/go-amqp/internal/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReceiverMethodsNoReceive(t *testing.T) {
	const linkName = "test"
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, linkName, 0, ModeFirst, nil)
		case *frames.PerformFlow:
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	const sourceAddr = "thesource"
	r, err := session.NewReceiver(LinkName(linkName), LinkSourceAddress(sourceAddr))
	assert.NoError(t, err)
	require.Equal(t, sourceAddr, r.Address())
	require.Equal(t, linkName, r.LinkName())
	require.Nil(t, r.LinkSourceFilterValue("nofilter"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, r.Close(ctx))
	cancel()
}

func TestReceiverLinkSourceFilter(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			// propagate filter specified by caller
			return mocks.ReceiverAttach(0, ff.Name, 0, ModeFirst, ff.Source.Filter)
		case *frames.PerformFlow:
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	const filterName = "myfilter"
	const filterExp = "filter_exp"
	r, err := session.NewReceiver(LinkAddressDynamic(), LinkSourceFilter(filterName, 0, filterExp))
	assert.NoError(t, err)
	require.Equal(t, "test", r.Address())
	require.NotEmpty(t, r.LinkName())
	require.Equal(t, filterExp, r.LinkSourceFilterValue(filterName))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, r.Close(ctx))
	cancel()
}

func TestReceiverOnClosed(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, 0, ModeFirst, ff.Source.Filter)
		case *frames.PerformFlow:
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver()
	assert.NoError(t, err)

	errChan := make(chan error)
	go func() {
		_, err := r.Receive(context.Background())
		errChan <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, 0, ModeFirst, ff.Source.Filter)
		case *frames.PerformFlow:
			return nil, nil
		case *frames.PerformEnd:
			return mocks.PerformEnd(0, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver()
	assert.NoError(t, err)

	errChan := make(chan error)
	go func() {
		_, err := r.Receive(context.Background())
		errChan <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, 0, ModeFirst, ff.Source.Filter)
		case *frames.PerformFlow:
			return nil, nil
		case *frames.PerformEnd:
			return mocks.PerformEnd(0, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver()
	assert.NoError(t, err)

	errChan := make(chan error)
	go func() {
		_, err := r.Receive(context.Background())
		errChan <- err
	}()

	require.NoError(t, client.Close())
	if err = <-errChan; !errors.Is(err, ErrConnClosed) {
		t.Fatalf("unexpected error %v", err)
	}
	_, err = r.Receive(context.Background())
	if !errors.Is(err, ErrConnClosed) {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestReceiverOnDetached(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, 0, ModeFirst, ff.Source.Filter)
		case *frames.PerformFlow:
			return nil, nil
		case *frames.PerformDetach:
			return mocks.PerformDetach(0, 0, nil)
		case *frames.PerformEnd:
			return mocks.PerformEnd(0, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver()
	assert.NoError(t, err)

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
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, linkHandle, ModeFirst, nil)
		case *frames.PerformFlow:
			return nil, nil
		case *frames.PerformDisposition:
			return mocks.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateAccepted{})
		case *frames.PerformDetach:
			return mocks.PerformDetach(0, 0, nil)
		case *frames.PerformEnd:
			return mocks.PerformEnd(0, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver()
	assert.NoError(t, err)

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
	assert.NoError(t, err)
	conn.SendFrame(fr)

	assert.Nil(t, <-msgChan)
	var amqpErr *Error
	if err = <-errChan; !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}
	assert.Equal(t, ErrorNotAllowed, amqpErr.Condition)

	_, err = r.Receive(context.Background())
	if !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}

	// missing MessageFormat
	r, err = session.NewReceiver()
	assert.NoError(t, err)
	go func() {
		msg, err := r.Receive(context.Background())
		msgChan <- msg
		errChan <- err
	}()
	fr, err = mocks.EncodeFrame(mocks.FrameAMQP, 0, &frames.PerformTransfer{
		DeliveryID: &deliveryID,
		Handle:     linkHandle,
	})
	assert.NoError(t, err)
	conn.SendFrame(fr)

	assert.Nil(t, <-msgChan)
	if err = <-errChan; !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}
	assert.Equal(t, ErrorNotAllowed, amqpErr.Condition)

	_, err = r.Receive(context.Background())
	if !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}

	// missing delivery tag
	format := uint32(0)
	r, err = session.NewReceiver()
	assert.NoError(t, err)
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
	assert.NoError(t, err)
	conn.SendFrame(fr)

	assert.Nil(t, <-msgChan)
	if err = <-errChan; !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}
	assert.Equal(t, ErrorNotAllowed, amqpErr.Condition)

	_, err = r.Receive(context.Background())
	if !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}

	assert.NoError(t, client.Close())
}

func TestReceiveSuccessModeFirst(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, linkHandle, ModeFirst, nil)
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
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver(LinkReceiverSettle(ModeFirst))
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	assert.NoError(t, err)
	if c := r.link.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// wait for the link to unpause as credit should now be available
	assert.NoError(t, waitForLink(r.link, false))
	// link credit should be 1
	if c := r.link.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	// subsequent dispositions should have no effect
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	assert.NoError(t, err)
	assert.NoError(t, client.Close())
}

func TestReceiveSuccessModeSecondAccept(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, linkHandle, ModeSecond, nil)
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
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver(LinkReceiverSettle(ModeSecond))
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	assert.NoError(t, err)
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// wait for the link to pause as we've consumed all available credit
	assert.NoError(t, waitForLink(r.link, true))
	// link credit must be zero since we only started with 1
	if c := r.link.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	assert.NoError(t, err)
	if c := r.link.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	require.Equal(t, true, msg.settled)
	// perform a dummy receive with short timeout to trigger flow
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, _ = r.Receive(ctx)
	cancel()
	// wait for the link to unpause as credit should now be available
	assert.NoError(t, waitForLink(r.link, false))
	// link credit should be back to 1
	if c := r.link.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	// subsequent dispositions should have no effect
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	assert.NoError(t, err)
	assert.NoError(t, client.Close())
}

func TestReceiveSuccessModeSecondReject(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, linkHandle, ModeSecond, nil)
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
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver(LinkReceiverSettle(ModeSecond))
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	assert.NoError(t, err)
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// wait for the link to pause as we've consumed all available credit
	assert.NoError(t, waitForLink(r.link, true))
	// link credit must be zero since we only started with 1
	if c := r.link.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.RejectMessage(ctx, msg, nil)
	cancel()
	assert.NoError(t, err)
	if c := r.link.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// perform a dummy receive with short timeout to trigger flow
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, _ = r.Receive(ctx)
	cancel()
	// wait for the link to unpause as credit should now be available
	assert.NoError(t, waitForLink(r.link, false))
	// link credit should be back to 1
	if c := r.link.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	assert.NoError(t, client.Close())
}

func TestReceiveSuccessModeSecondRelease(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, linkHandle, ModeSecond, nil)
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
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver(LinkReceiverSettle(ModeSecond))
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	assert.NoError(t, err)
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// wait for the link to pause as we've consumed all available credit
	assert.NoError(t, waitForLink(r.link, true))
	// link credit must be zero since we only started with 1
	if c := r.link.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.ReleaseMessage(ctx, msg)
	cancel()
	assert.NoError(t, err)
	if c := r.link.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// perform a dummy receive with short timeout to trigger flow
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, _ = r.Receive(ctx)
	cancel()
	// wait for the link to unpause as credit should now be available
	assert.NoError(t, waitForLink(r.link, false))
	// link credit should be back to 1
	if c := r.link.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	assert.NoError(t, client.Close())
}

func TestReceiveSuccessModeSecondModify(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, linkHandle, ModeSecond, nil)
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
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver(LinkReceiverSettle(ModeSecond))
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	assert.NoError(t, err)
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// wait for the link to pause as we've consumed all available credit
	assert.NoError(t, waitForLink(r.link, true))
	// link credit must be zero since we only started with 1
	if c := r.link.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.ModifyMessage(ctx, msg, false, true, Annotations{
		"some": "value",
	})
	cancel()
	assert.NoError(t, err)
	if c := r.link.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// perform a dummy receive with short timeout to trigger flow
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, _ = r.Receive(ctx)
	cancel()
	// wait for the link to unpause as credit should now be available
	assert.NoError(t, waitForLink(r.link, false))
	// link credit should be back to 1
	if c := r.link.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	assert.NoError(t, client.Close())
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
	msg, err := receiver.Prefetched(context.Background())
	require.Nil(t, msg)
	require.Nil(t, err)

	messagesCh <- Message{
		ApplicationProperties: map[string]interface{}{
			"prop": "hello",
		},
		settled: true,
	}

	require.NotEmpty(t, messagesCh)
	msg, err = receiver.Prefetched(context.Background())

	require.EqualValues(t, "hello", msg.ApplicationProperties["prop"].(string))
	require.Nil(t, err)
	require.Empty(t, messagesCh)
}

func TestReceiveMultiFrameMessageSuccess(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, linkHandle, ModeSecond, nil)
		case *frames.PerformFlow:
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
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver(LinkReceiverSettle(ModeSecond))
	assert.NoError(t, err)
	msgChan := make(chan *Message)
	errChan := make(chan error)
	go func() {
		msg, err := r.Receive(context.Background())
		msgChan <- msg
		errChan <- err
	}()
	// send multi-frame message
	payload := []byte("this should be split into three frames for a multi-frame transfer message")
	assert.NoError(t, conn.SendMultiFrameTransfer(0, linkHandle, deliveryID, payload, nil))
	msg := <-msgChan
	assert.NoError(t, <-errChan)
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// wait for the link to pause as we've consumed all available credit
	assert.NoError(t, waitForLink(r.link, true))
	// link credit must be zero since we only started with 1
	if c := r.link.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	result := []byte{}
	for i := range msg.Data {
		result = append(result, msg.Data[i]...)
	}
	assert.Equal(t, payload, result)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	assert.NoError(t, err)
	if c := r.link.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	require.Equal(t, true, msg.settled)
	// perform a dummy receive with short timeout to trigger flow
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, _ = r.Receive(ctx)
	cancel()
	// wait for the link to unpause as credit should now be available
	assert.NoError(t, waitForLink(r.link, false))
	// link credit should be back to 1
	if c := r.link.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	assert.NoError(t, client.Close())
}

func TestReceiveInvalidMultiFrameMessage(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, linkHandle, ModeSecond, nil)
		case *frames.PerformFlow:
			return nil, nil
		case *frames.PerformDetach:
			return mocks.PerformDetach(0, 0, nil)
		case *frames.PerformEnd:
			return mocks.PerformEnd(0, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver(LinkReceiverSettle(ModeSecond))
	assert.NoError(t, err)
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
	assert.NoError(t, conn.SendMultiFrameTransfer(0, linkHandle, deliveryID, payload, func(i int, fr *frames.PerformTransfer) {
		if i == 0 {
			return
		}
		// modify the second frame with mismatched data
		badID := uint32(123)
		fr.DeliveryID = &badID
	}))
	msg := <-msgChan
	assert.Nil(t, msg)
	var amqpErr *Error
	if err = <-errChan; !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}
	assert.Equal(t, ErrorNotAllowed, amqpErr.Condition)

	// mismatched MessageFormat
	r, err = session.NewReceiver(LinkReceiverSettle(ModeSecond))
	assert.NoError(t, err)
	go func() {
		msg, err := r.Receive(context.Background())
		msgChan <- msg
		errChan <- err
	}()
	assert.NoError(t, conn.SendMultiFrameTransfer(0, linkHandle, deliveryID, payload, func(i int, fr *frames.PerformTransfer) {
		if i == 0 {
			return
		}
		// modify the second frame with mismatched data
		badFormat := uint32(123)
		fr.MessageFormat = &badFormat
	}))
	msg = <-msgChan
	assert.Nil(t, msg)
	if err = <-errChan; !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}
	assert.Equal(t, ErrorNotAllowed, amqpErr.Condition)

	// mismatched DeliveryTag
	r, err = session.NewReceiver(LinkReceiverSettle(ModeSecond))
	assert.NoError(t, err)
	go func() {
		msg, err := r.Receive(context.Background())
		msgChan <- msg
		errChan <- err
	}()
	assert.NoError(t, conn.SendMultiFrameTransfer(0, linkHandle, deliveryID, payload, func(i int, fr *frames.PerformTransfer) {
		if i == 0 {
			return
		}
		// modify the second frame with mismatched data
		fr.DeliveryTag = []byte("bad_tag")
	}))
	msg = <-msgChan
	assert.Nil(t, msg)
	if err = <-errChan; !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}
	assert.Equal(t, ErrorNotAllowed, amqpErr.Condition)

	assert.NoError(t, client.Close())
}

func TestReceiveMultiFrameMessageAborted(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, linkHandle, ModeSecond, nil)
		case *frames.PerformFlow:
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
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver(LinkReceiverSettle(ModeSecond))
	assert.NoError(t, err)
	msgChan := make(chan *Message)
	errChan := make(chan error)
	go func() {
		msg, err := r.Receive(context.Background())
		errChan <- err
		msgChan <- msg
	}()
	// send multi-frame message
	payload := []byte("this should be split into three frames for a multi-frame transfer message")
	assert.NoError(t, conn.SendMultiFrameTransfer(0, linkHandle, deliveryID, payload, func(i int, fr *frames.PerformTransfer) {
		if i < 2 {
			return
		}
		// set abort flag on the last frame
		fr.Aborted = true
	}))
	// we shouldn't have received any message at this point, now send a single-frame message
	payload = []byte("single message")
	b, err := mocks.PerformTransfer(0, linkHandle, deliveryID+1, payload)
	assert.NoError(t, err)
	conn.SendFrame(b)
	assert.NoError(t, <-errChan)
	msg := <-msgChan
	assert.Equal(t, payload, msg.GetData())
	assert.NoError(t, client.Close())
}

func TestReceiveMessageTooBig(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, linkHandle, ModeSecond, nil)
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				bigPayload := make([]byte, 256)
				return mocks.PerformTransfer(0, linkHandle, deliveryID, bigPayload)
			}
			// ignore future flow frames as we have no response
			return nil, nil
		case *frames.PerformDetach:
			return mocks.PerformDetach(0, 0, nil)
		case *frames.PerformEnd:
			return mocks.PerformEnd(0, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver(LinkReceiverSettle(ModeSecond), LinkMaxMessageSize(128))
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	assert.Nil(t, msg)
	var amqpErr *Error
	if !errors.As(err, &amqpErr) {
		t.Fatalf("unexpected error %v", err)
	}
	assert.Equal(t, ErrorMessageSizeExceeded, amqpErr.Condition)
	assert.NoError(t, client.Close())
}

func TestReceiveSuccessAcceptFails(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, linkHandle, ModeSecond, nil)
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
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver(LinkReceiverSettle(ModeSecond))
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	assert.NoError(t, err)
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// wait for the link to pause as we've consumed all available credit
	assert.NoError(t, waitForLink(r.link, true))
	// link credit must be zero since we only started with 1
	if c := r.link.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	// close client before accepting the message
	assert.NoError(t, client.Close())
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	if !errors.Is(err, ErrConnClosed) {
		t.Fatalf("unexpected error %v", err)
	}
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
}

func TestReceiverDispositionBatcherTimer(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, linkHandle, ModeSecond, nil)
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
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver(LinkReceiverSettle(ModeSecond), LinkCredit(2), LinkBatchMaxAge(time.Second), LinkBatching(true))
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx)
	cancel()
	assert.NoError(t, err)
	if c := r.link.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	assert.NoError(t, err)
	if c := r.link.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	assert.Equal(t, 0, r.inFlight.len())
	assert.Equal(t, true, msg.settled)
	assert.NoError(t, client.Close())
}

func TestReceiverDispositionBatcherFull(t *testing.T) {
	const credit = 3
	const linkHandle = 0
	deliveryID := uint32(1)
	acceptCount := 0
	allAccepted := make(chan struct{})
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, linkHandle, ModeSecond, nil)
		case *frames.PerformFlow:
			return nil, nil
		case *frames.PerformDisposition:
			if _, ok := ff.State.(*encoding.StateAccepted); !ok {
				return nil, fmt.Errorf("unexpected State %T", ff.State)
			}
			if ff.Last == nil {
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
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver(LinkReceiverSettle(ModeSecond), LinkCredit(credit), LinkBatchMaxAge(time.Second), LinkBatching(true))
	assert.NoError(t, err)
	for i := 0; i < credit; i++ {
		b, err := mocks.PerformTransfer(0, linkHandle, deliveryID, []byte("hello"))
		assert.NoError(t, err)
		conn.SendFrame(b)
		deliveryID++
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		msg, err := r.Receive(ctx)
		cancel()
		assert.NoError(t, err)
		go func() {
			assert.NoError(t, r.AcceptMessage(context.Background(), msg))
			assert.Equal(t, true, msg.settled)
		}()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-allAccepted:
		// all messages were settled
	case <-ctx.Done():
		t.Fatalf("not all messages were settled within the allotted time: %d", acceptCount)
	}
	assert.Equal(t, 0, r.inFlight.len())
	assert.NoError(t, client.Close())
}

func TestReceiverDispositionBatcherRelease(t *testing.T) {
	const credit = 3
	const linkHandle = 0
	deliveryID := uint32(1)
	acceptCount := 0
	allAccepted := make(chan struct{})
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test")
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(0, ff.Name, linkHandle, ModeSecond, nil)
		case *frames.PerformFlow:
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
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver(LinkReceiverSettle(ModeSecond), LinkCredit(credit), LinkBatchMaxAge(time.Second), LinkBatching(true))
	assert.NoError(t, err)
	for i := 0; i < credit; i++ {
		b, err := mocks.PerformTransfer(0, linkHandle, deliveryID, []byte("hello"))
		assert.NoError(t, err)
		conn.SendFrame(b)
		deliveryID++
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
		msg, err := r.Receive(ctx)
		cancel()
		assert.NoError(t, err)
		go func(count int) {
			if count == credit-1 {
				assert.NoError(t, r.AcceptMessage(context.Background(), msg))
			} else {
				assert.NoError(t, r.ReleaseMessage(context.Background(), msg))
			}
			assert.Equal(t, true, msg.settled)
		}(i)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-allAccepted:
		// all messages were settled
	case <-ctx.Done():
		t.Fatalf("not all messages were settled within the allotted time: %d", acceptCount)
	}
	assert.Equal(t, 0, r.inFlight.len())
	assert.NoError(t, client.Close())
}
