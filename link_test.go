package amqp

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/Azure/go-amqp/internal/mocks"
	"github.com/stretchr/testify/require"
)

func TestLinkFlowForSender(t *testing.T) {
	// senders don't actually send flow frames but they do enable require tranfers to be
	// assigned. We should refactor, this is just fallout from my "lift and shift" of the
	// flow logic in `mux`
	l := newTestLink(t)
	l.receiver = nil

	err := l.DrainCredit(context.Background())
	require.Error(t, err, "drain can only be used with receiver links using manual credit management")

	err = l.IssueCredit(1)
	require.Error(t, err, "issueCredit can only be used with receiver links using manual credit management")

	// and flow goes through the non-manual credit path
	require.EqualValues(t, 0, l.linkCredit, "No link credits have been added")

	// if we have link credit we can enable outgoing transfers
	l.linkCredit = 1
	ok, enableOutgoingTransfers := l.doFlow()

	require.True(t, ok, "no errors, should continue to process")
	require.True(t, enableOutgoingTransfers, "outgoing transfers needed for senders")
}

func TestLinkFlowThatNeedsToReplenishCredits(t *testing.T) {
	l := newTestLink(t)

	err := l.DrainCredit(context.Background())
	require.Error(t, err, "drain can only be used with receiver links using manual credit management")

	err = l.IssueCredit(1)
	require.Error(t, err, "issueCredit can only be used with receiver links using manual credit management")

	// and flow goes through the non-manual credit path
	require.EqualValues(t, 0, l.linkCredit, "No link credits have been added")

	// we've consumed half of the maximum credit we're allowed to have - reflow!
	l.receiver.maxCredit = 2
	l.linkCredit = 1
	l.unsettledMessages = map[string]struct{}{}

	ok, enableOutgoingTransfers := l.doFlow()

	require.True(t, ok, "no errors, should continue to process")
	require.False(t, enableOutgoingTransfers, "outgoing transfers only needed for senders")

	// flow happens immmediately in 'mux'
	txFrame := <-l.Session.tx

	switch frame := txFrame.(type) {
	case *frames.PerformFlow:
		require.False(t, frame.Drain)
		// replenished credits: l.receiver.maxCredit-uint32(l.countUnsettled())
		require.EqualValues(t, 2, *frame.LinkCredit)
	default:
		require.Fail(t, fmt.Sprintf("Unexpected frame was transferred: %+v", txFrame))
	}
}

func TestLinkFlowWithZeroCredits(t *testing.T) {
	l := newTestLink(t)

	err := l.DrainCredit(context.Background())
	require.Error(t, err, "drain can only be used with receiver links using manual credit management")

	err = l.IssueCredit(1)
	require.Error(t, err, "issueCredit can only be used with receiver links using manual credit management")

	// and flow goes through the non-manual credit path
	require.EqualValues(t, 0, l.linkCredit, "No link credits have been added")

	l.receiver.maxCredit = 2
	l.linkCredit = 0
	l.unsettledMessages = map[string]struct{}{
		"hello":  {},
		"hello2": {},
	}

	ok, enableOutgoingTransfers := l.doFlow()

	require.True(t, ok)
	require.False(t, enableOutgoingTransfers)
}

func TestLinkFlowDrain(t *testing.T) {
	l := newTestLink(t)

	// now initialize it as a manual credit link
	require.NoError(t, LinkWithManualCredits()(l))

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		<-l.ReceiverReady
		l.receiver.manualCreditor.EndDrain()
	}()

	require.NoError(t, l.DrainCredit(context.Background()))
}

func TestLinkFlowWithManualCreditor(t *testing.T) {
	l := newTestLink(t)
	require.NoError(t, LinkWithManualCredits()(l))

	l.linkCredit = 1
	require.NoError(t, l.IssueCredit(100))

	ok, enableOutgoingTransfers := l.doFlow()
	require.True(t, ok)
	require.False(t, enableOutgoingTransfers, "sender related state is not enabled")

	// flow happens immmediately in 'mux'
	txFrame := <-l.Session.tx

	switch frame := txFrame.(type) {
	case *frames.PerformFlow:
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
		<-l.ReceiverReady

		ok, enableOutgoingTransfers := l.doFlow()
		require.True(t, ok)
		require.False(t, enableOutgoingTransfers, "sender related state is not enabled")

		// flow happens immmediately in 'mux'
		txFrame := <-l.Session.tx

		switch frame := txFrame.(type) {
		case *frames.PerformFlow:
			require.True(t, frame.Drain)
			// When we're draining we just automatically set the flow link credit to 0.
			// This should allow any outstanding messages to get flushed.
			require.EqualValues(t, 0, *frame.LinkCredit)
		default:
			require.Fail(t, fmt.Sprintf("Unexpected frame was transferred: %+v", txFrame))
		}

		// simulate the return of the flow from the service
		err := l.muxHandleFrame(&frames.PerformFlow{
			Drain: true,
		})

		require.NoError(t, err)
	}()

	l.linkCredit = 1
	require.NoError(t, l.DrainCredit(context.Background()))
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
	case fr := <-l.Session.tx: // there won't be a flow this time.
		require.Failf(t, "No flow frame would be needed since no link credits were added and drain was not requested", "Frame was %+v", fr)
	case <-time.After(time.Second * 2):
		// this is the expected case since no frame will be sent.
	}
}

func TestMuxFlowHandlesDrainProperly(t *testing.T) {
	l := newTestLink(t)
	require.NoError(t, LinkWithManualCredits()(l))

	l.linkCredit = 101

	// simulate what our 'drain' call to muxFlow would look like
	// when draining
	require.NoError(t, l.muxFlow(0, true))
	require.EqualValues(t, 101, l.linkCredit, "credits are untouched when draining")

	// when doing a non-drain flow we update the linkCredit to our new link credit total.
	require.NoError(t, l.muxFlow(501, false))
	require.EqualValues(t, 501, l.linkCredit, "credits are untouched when draining")
}

func newTestLink(t *testing.T) *link {
	l := &link{
		Source: &frames.Source{},
		receiver: &Receiver{
			// adding just enough so the debug() print will still work...
			// debug(1, "FLOW Link Mux half: source: %s, inflight: %d, credit: %d, deliveryCount: %d, messages: %d, unsettled: %d, maxCredit : %d, settleMode: %s", l.source.Address, l.receiver.inFlight.len(), l.linkCredit, l.deliveryCount, len(l.messages), l.countUnsettled(), l.receiver.maxCredit, l.receiverSettleMode.String())
			inFlight: inFlight{},
		},
		Detached: make(chan struct{}),
		Session: &Session{
			tx:   make(chan frames.FrameBody, 100),
			done: make(chan struct{}),
		},
		RX:            make(chan frames.FrameBody, 100),
		ReceiverReady: make(chan struct{}, 1),
	}

	return l
}

func TestLinkOptions(t *testing.T) {
	tests := []struct {
		label string
		opts  []LinkOption

		wantSource     *frames.Source
		wantTarget     *frames.Target
		wantProperties map[encoding.Symbol]interface{}
	}{
		{
			label: "no options",
		},
		{
			label: "link-filters",
			opts: []LinkOption{
				LinkSelectorFilter("amqp.annotation.x-opt-offset > '100'"),
				LinkProperty("x-opt-test1", "test1"),
				LinkProperty("x-opt-test2", "test2"),
				LinkProperty("x-opt-test1", "test3"),
				LinkPropertyInt64("x-opt-test4", 1),
				LinkPropertyInt32("x-opt-test5", 2),
				LinkSourceFilter("com.microsoft:session-filter", 0x00000137000000C, "123"),
			},

			wantSource: &frames.Source{
				Filter: map[encoding.Symbol]*encoding.DescribedType{
					"apache.org:selector-filter:string": {
						Descriptor: binary.BigEndian.Uint64([]byte{0x00, 0x00, 0x46, 0x8C, 0x00, 0x00, 0x00, 0x04}),
						Value:      "amqp.annotation.x-opt-offset > '100'",
					},
					"com.microsoft:session-filter": {
						Descriptor: binary.BigEndian.Uint64([]byte{0x00, 0x00, 0x00, 0x13, 0x70, 0x00, 0x00, 0x0C}),
						Value:      "123",
					},
				},
			},
			wantProperties: map[encoding.Symbol]interface{}{
				"x-opt-test1": "test3",
				"x-opt-test2": "test2",
				"x-opt-test4": int64(1),
				"x-opt-test5": int32(2),
			},
		},
		{
			label: "more-link-filters",
			opts: []LinkOption{
				LinkSourceFilter("com.microsoft:session-filter", 0x00000137000000C, nil),
			},

			wantSource: &frames.Source{
				Filter: map[encoding.Symbol]*encoding.DescribedType{
					"com.microsoft:session-filter": {
						Descriptor: binary.BigEndian.Uint64([]byte{0x00, 0x00, 0x00, 0x13, 0x70, 0x00, 0x00, 0x0C}),
						Value:      nil,
					},
				},
			},
		},
		{
			label: "link-source-capabilities",
			opts: []LinkOption{
				LinkSourceCapabilities("cap1", "cap2", "cap3"),
			},
			wantSource: &frames.Source{
				Capabilities: []encoding.Symbol{"cap1", "cap2", "cap3"},
			},
		},
		{
			label: "link-target-capabilities",
			opts: []LinkOption{
				LinkTargetCapabilities("cap1", "cap2", "cap3"),
			},
			wantTarget: &frames.Target{
				Capabilities: []encoding.Symbol{"cap1", "cap2", "cap3"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			got, err := newLink(nil, nil, tt.opts)
			if err != nil {
				t.Fatal(err)
			}

			if !testEqual(got.Source, tt.wantSource) {
				t.Errorf("Source properties don't match expected:\n %s", testDiff(got.Source, tt.wantSource))
			}

			if !testEqual(got.properties, tt.wantProperties) {
				t.Errorf("Link properties don't match expected:\n %s", testDiff(got.properties, tt.wantProperties))
			}
		})
	}
}

func TestSourceName(t *testing.T) {
	expectedSourceName := "source-name"
	opts := []LinkOption{
		LinkName(expectedSourceName),
	}

	got, err := newLink(nil, nil, opts)
	if err != nil {
		t.Fatal(err)
	}

	if got.Key.name != expectedSourceName {
		t.Errorf("Link Source Name does not match expected: %v got: %v", expectedSourceName, got.Key.name)
	}
}

func TestSessionFlowDisablesTransfer(t *testing.T) {
	t.Skip("TODO: finish for link testing")
	nextIncomingID := uint32(0)
	netConn := mocks.NewNetConn(senderFrameHandlerNoUnhandled(ModeUnsettled))

	client, err := New(netConn)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	session, err := client.NewSession(ctx)
	cancel()
	require.NoError(t, err)

	b, err := mocks.EncodeFrame(mocks.FrameAMQP, 0, &frames.PerformFlow{
		NextIncomingID: &nextIncomingID,
		IncomingWindow: 0,
		OutgoingWindow: 100,
		NextOutgoingID: 1,
	})
	require.NoError(t, err)
	netConn.SendFrame(b)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = session.Close(ctx)
	cancel()
	require.NoError(t, err)

	require.NoError(t, client.Close())
}

func TestExactlyOnceDoesntWork(t *testing.T) {
	netConn := mocks.NewNetConn(senderFrameHandlerNoUnhandled(ModeUnsettled))

	client, err := New(netConn)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	session, err := client.NewSession(ctx)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	snd, err := session.NewSender(
		ctx,
		LinkSenderSettle(ModeMixed),
		LinkReceiverSettle(ModeSecond),
		LinkTargetAddress("doesntwork"),
	)
	cancel()
	require.Error(t, err)
	require.Nil(t, snd)
	require.NoError(t, client.Close())
}

// TODO: echo flow frame
