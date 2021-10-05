package amqp

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/Azure/go-amqp/internal/mocks"
	"github.com/stretchr/testify/assert"
)

// helper to wait for a link to pause/resume
// returns an error if it times out waiting
func waitForLink(l *link, paused bool) error {
	state := uint32(0) // unpaused
	if paused {
		state = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	for {
		if atomic.LoadUint32(&l.Paused) == state {
			return nil
		} else if err := ctx.Err(); err != nil {
			return err
		}
		runtime.Gosched()
	}
}

func TestReceive_ModeFirst(t *testing.T) {
	const linkName = "test"
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test", 0)
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(linkName, linkHandle, ModeFirst)
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return mocks.PerformTransfer(linkHandle, deliveryID, []byte("hello"))
			}
			// ignore future flow frames as we have no response
			return nil, nil
		case *frames.PerformDisposition:
			return mocks.PerformDisposition(deliveryID, &encoding.StateAccepted{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver(LinkName(linkName), LinkReceiverSettle(ModeFirst))
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
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
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	assert.NoError(t, err)
	assert.NoError(t, client.Close())
}

func TestReceive_ModeSecond(t *testing.T) {
	const linkName = "test"
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch ff := req.(type) {
		case *mocks.AMQPProto:
			return mocks.ProtoHeader(mocks.ProtoAMQP)
		case *frames.PerformOpen:
			return mocks.PerformOpen("test", 0)
		case *frames.PerformBegin:
			return mocks.PerformBegin(0)
		case *frames.PerformAttach:
			return mocks.ReceiverAttach(linkName, linkHandle, ModeSecond)
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return mocks.PerformTransfer(linkHandle, deliveryID, []byte("hello"))
			}
			// ignore future flow frames as we have no response
			return nil, nil
		case *frames.PerformDisposition:
			return mocks.PerformDisposition(deliveryID, &encoding.StateAccepted{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := mocks.NewNetConn(responder)
	client, err := New(conn)
	assert.NoError(t, err)
	session, err := client.NewSession()
	assert.NoError(t, err)
	r, err := session.NewReceiver(LinkName(linkName), LinkReceiverSettle(ModeSecond))
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
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
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	err = r.AcceptMessage(ctx, msg)
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
	// subsequent dispositions should have no effect
	// TODO: https://github.com/Azure/go-amqp/issues/76
	/*ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	assert.NoError(t, err)*/
	assert.NoError(t, client.Close())
}
