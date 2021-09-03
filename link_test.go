package amqp

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLinkDrain(t *testing.T) {
	l := newTestLink(t)

	// without manual credit management...
	err := l.drain(context.Background())
	require.Error(t, err, "drain can only be used with links using manual credit management")

	err = l.addCredit(1)
	require.Error(t, err, "addCredit can only be used with links using manual credit management")

	// now initialize it as a manual credit link
	require.NoError(t, LinkWithManualCredits()(l))

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		<-l.receiverReady
		l.receiver.manualCreditor.EndDrain()
	}()

	require.NoError(t, l.drain(context.Background()))
}

func TestLinkFlowWithManualCreditor(t *testing.T) {
	l := newTestLink(t)
	require.NoError(t, LinkWithManualCredits()(l))

	l.linkCredit = 1
	require.NoError(t, l.addCredit(100))

	ok, enableOutgoingTransfers := l.doFlow()
	require.True(t, ok)
	require.False(t, enableOutgoingTransfers, "sender related state is not enabled")

	// flow happens immmediately in 'mux'
	txFrame := <-l.session.tx

	switch frame := txFrame.(type) {
	case *performFlow:
		require.False(t, frame.Drain)
		require.EqualValues(t, 100+1, *frame.LinkCredit)
	default:
		require.Fail(t, fmt.Sprintf("Unexpected frame was transferred: %+v", txFrame))
	}
}

func TestLinkFlowWithDrain(t *testing.T) {
	l := newTestLink(t)
	require.NoError(t, LinkWithManualCredits()(l))

	go func() {
		<-l.receiverReady

		ok, enableOutgoingTransfers := l.doFlow()
		require.True(t, ok)
		require.False(t, enableOutgoingTransfers, "sender related state is not enabled")

		// flow happens immmediately in 'mux'
		txFrame := <-l.session.tx

		switch frame := txFrame.(type) {
		case *performFlow:
			require.True(t, frame.Drain)
			require.EqualValues(t, 1, *frame.LinkCredit)
		default:
			require.Fail(t, fmt.Sprintf("Unexpected frame was transferred: %+v", txFrame))
		}

		// simulate the return of the flow from the service
		l.muxHandleFrame(&performFlow{
			Drain: true,
		})
	}()

	l.linkCredit = 1
	require.NoError(t, l.drain(context.Background()))
}

func TestLinkFlowWithManualCreditorAndNoFlowNeeded(t *testing.T) {
	l := newTestLink(t)
	require.NoError(t, LinkWithManualCredits()(l))

	l.linkCredit = 1

	ok, enableOutgoingTransfers := l.doFlow()
	require.True(t, ok)
	require.False(t, enableOutgoingTransfers, "sender related state is not enabled")

	// flow happens immmediately in 'mux'
	select {
	case fr := <-l.session.tx: // there won't be a flow this time.
		require.Failf(t, "No flow frame would be needed since no link credits were added and drain was not requested", "Frame was %+v", fr)
	case <-time.After(time.Second * 2):
		// this is the expected case since no frame will be sent.
	}
}

func newTestLink(t *testing.T) *link {
	l := &link{
		receiver: &Receiver{},
		session: &Session{
			tx:   make(chan frameBody, 100),
			done: make(chan struct{}),
		},
		rx:            make(chan frameBody, 100),
		receiverReady: make(chan struct{}, 1),
	}

	return l
}
