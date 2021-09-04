package amqp

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLinkFlowForSender(t *testing.T) {
	// senders don't actually send flow frames but they do enable require tranfers to be
	// assigned. We should refactor, this is just fallout from my "lift and shift" of the
	// flow logic in `mux`
	l := newTestLink(t)
	l.receiver = nil

	err := l.drain(context.Background())
	require.Error(t, err, "drain can only be used with receiver links using manual credit management")

	err = l.addCredit(1)
	require.Error(t, err, "addCredit can only be used with receiver links using manual credit management")

	// and flow goes through the non-manual credit path
	require.EqualValues(t, 0, l.linkCredit, "No link credits have been added")
	require.EqualValues(t, 0, l.paused, "Link not paused")

	// if we have link credit we can enable outgoing transfers
	l.linkCredit = 1
	ok, enableOutgoingTransfers := l.doFlow()

	require.True(t, ok, "no errors, should continue to process")
	require.True(t, enableOutgoingTransfers, "outgoing transfers needed for senders")
	require.EqualValues(t, 0, l.paused, "Link not paused")
}

func TestLinkFlowThatNeedsToReplenishCredits(t *testing.T) {
	l := newTestLink(t)

	err := l.drain(context.Background())
	require.Error(t, err, "drain can only be used with receiver links using manual credit management")

	err = l.addCredit(1)
	require.Error(t, err, "addCredit can only be used with receiver links using manual credit management")

	// and flow goes through the non-manual credit path
	require.EqualValues(t, 0, l.linkCredit, "No link credits have been added")
	require.EqualValues(t, 0, l.paused, "Link not paused")

	// we've consumed half of the maximum credit we're allowed to have - reflow!
	l.receiver.maxCredit = 2
	l.linkCredit = 1
	l.unsettledMessages = map[string]struct{}{}

	ok, enableOutgoingTransfers := l.doFlow()

	require.True(t, ok, "no errors, should continue to process")
	require.False(t, enableOutgoingTransfers, "outgoing transfers only needed for senders")
	require.EqualValues(t, 0, l.paused, "Link not paused")

	// flow happens immmediately in 'mux'
	txFrame := <-l.session.tx

	switch frame := txFrame.(type) {
	case *performFlow:
		require.False(t, frame.Drain)
		// replenished credits: l.receiver.maxCredit-uint32(l.countUnsettled())
		require.EqualValues(t, 2, *frame.LinkCredit)
	default:
		require.Fail(t, fmt.Sprintf("Unexpected frame was transferred: %+v", txFrame))
	}
}

func TestLinkFlowWithZeroCredits(t *testing.T) {
	l := newTestLink(t)

	err := l.drain(context.Background())
	require.Error(t, err, "drain can only be used with receiver links using manual credit management")

	err = l.addCredit(1)
	require.Error(t, err, "addCredit can only be used with receiver links using manual credit management")

	// and flow goes through the non-manual credit path
	require.EqualValues(t, 0, l.linkCredit, "No link credits have been added")
	require.EqualValues(t, 0, l.paused, "Link not paused...yet")

	l.receiver.maxCredit = 2
	l.linkCredit = 0
	l.unsettledMessages = map[string]struct{}{
		"hello":  struct{}{},
		"hello2": struct{}{},
	}

	ok, enableOutgoingTransfers := l.doFlow()

	require.True(t, ok)
	require.False(t, enableOutgoingTransfers)
	require.EqualValues(t, uint32(1), l.paused, "Link is paused because credits are zero")
}

func TestLinkFlowDrain(t *testing.T) {
	l := newTestLink(t)

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
		err := l.muxHandleFrame(&performFlow{
			Drain: true,
		})

		require.NoError(t, err)
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
		source: &source{},
		receiver: &Receiver{
			// adding just enough so the debug() print will still work...
			// debug(1, "FLOW Link Mux half: source: %s, inflight: %d, credit: %d, deliveryCount: %d, messages: %d, unsettled: %d, maxCredit : %d, settleMode: %s", l.source.Address, len(l.receiver.inFlight.m), l.linkCredit, l.deliveryCount, len(l.messages), l.countUnsettled(), l.receiver.maxCredit, l.receiverSettleMode.String())
			inFlight: inFlight{},
		},
		session: &Session{
			tx:   make(chan frameBody, 100),
			done: make(chan struct{}),
		},
		rx:            make(chan frameBody, 100),
		receiverReady: make(chan struct{}, 1),
	}

	return l
}
