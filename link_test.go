package amqp

import (
	"context"
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
	l.receiver.manualCreditor = &manualCreditor{}

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
	l.receiver.manualCreditor = &manualCreditor{}

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
	l.receiver.manualCreditor = &manualCreditor{}

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
	l.receiver.manualCreditor = &manualCreditor{}

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
	l.receiver.manualCreditor = &manualCreditor{}

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

func TestNewSendingLink(t *testing.T) {
	const (
		name       = "mysender"
		targetAddr = "target"
	)
	tests := []struct {
		label    string
		opts     SenderOptions
		validate func(t *testing.T, l *link)
	}{
		{
			label: "default options",
			validate: func(t *testing.T, l *link) {
				require.Empty(t, l.Target.Capabilities)
				require.Equal(t, DurabilityNone, l.Source.Durable)
				require.False(t, l.dynamicAddr)
				require.Empty(t, l.Source.ExpiryPolicy)
				require.Zero(t, l.Source.Timeout)
				require.True(t, l.detachOnDispositionError)
				require.NotEmpty(t, l.Key.name)
				require.Empty(t, l.properties)
				require.Nil(t, l.SenderSettleMode)
				require.Nil(t, l.ReceiverSettleMode)
				require.Equal(t, targetAddr, l.Target.Address)
			},
		},
		{
			label: "with options",
			opts: SenderOptions{
				Capabilities:            []string{"foo", "bar"},
				Durability:              DurabilityUnsettledState,
				DynamicAddress:          true,
				ExpiryPolicy:            ExpiryLinkDetach,
				ExpiryTimeout:           5,
				IgnoreDispositionErrors: true,
				Name:                    name,
				Properties: map[string]interface{}{
					"property": 123,
				},
				RequestedReceiverSettleMode: ModeFirst.Ptr(),
				SettlementMode:              ModeSettled.Ptr(),
			},
			validate: func(t *testing.T, l *link) {
				require.Equal(t, encoding.MultiSymbol{"foo", "bar"}, l.Source.Capabilities)
				require.Equal(t, DurabilityUnsettledState, l.Source.Durable)
				require.True(t, l.dynamicAddr)
				require.Equal(t, ExpiryLinkDetach, l.Source.ExpiryPolicy)
				require.Equal(t, uint32(5), l.Source.Timeout)
				require.False(t, l.detachOnDispositionError)
				require.Equal(t, name, l.Key.name)
				require.Equal(t, map[encoding.Symbol]interface{}{
					"property": 123,
				}, l.properties)
				require.NotNil(t, l.SenderSettleMode)
				require.Equal(t, ModeSettled, *l.SenderSettleMode)
				require.NotNil(t, l.ReceiverSettleMode)
				require.Equal(t, ModeFirst, *l.ReceiverSettleMode)
				require.Empty(t, l.Target.Address)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			got, err := newSendingLink(targetAddr, nil, &tt.opts)
			require.NoError(t, err)
			require.NotNil(t, got)
			tt.validate(t, got)
		})
	}
}

func TestNewReceivingLink(t *testing.T) {
	const (
		name       = "myreceiver"
		sourceAddr = "source"
	)
	// skip validating any fields on l.receiver as they are
	// populated in Session.NewReceiver()

	tests := []struct {
		label    string
		opts     ReceiverOptions
		validate func(t *testing.T, l *link)

		wantSource     *frames.Source
		wantTarget     *frames.Target
		wantProperties map[encoding.Symbol]interface{}
	}{
		{
			label: "default options",
			validate: func(t *testing.T, l *link) {
				//require.False(t, l.receiver.batching)
				//require.Equal(t, defaultLinkBatchMaxAge, l.receiver.batchMaxAge)
				require.Empty(t, l.Target.Capabilities)
				//require.Equal(t, defaultLinkCredit, l.receiver.maxCredit)
				require.Equal(t, DurabilityNone, l.Target.Durable)
				require.False(t, l.dynamicAddr)
				require.Empty(t, l.Target.ExpiryPolicy)
				require.Zero(t, l.Target.Timeout)
				require.Empty(t, l.Source.Filter)
				require.True(t, l.detachOnDispositionError)
				//require.Nil(t, l.receiver.manualCreditor)
				require.Zero(t, l.MaxMessageSize)
				require.NotEmpty(t, l.Key.name)
				require.Empty(t, l.properties)
				require.Nil(t, l.SenderSettleMode)
				require.Nil(t, l.ReceiverSettleMode)
				require.Equal(t, sourceAddr, l.Source.Address)
			},
		},
		{
			label: "with options",
			opts: ReceiverOptions{
				//Batching:                  true,
				//BatchMaxAge:               1 * time.Minute,
				Capabilities: []string{"foo", "bar"},
				//Credit:                    32,
				Durability:     DurabilityConfiguration,
				DynamicAddress: true,
				ExpiryPolicy:   ExpiryNever,
				ExpiryTimeout:  3,
				Filters: []LinkFilter{
					LinkFilterSelector("amqp.annotation.x-opt-offset > '100'"),
					LinkFilterSource("com.microsoft:session-filter", 0x00000137000000C, "123"),
				},
				IgnoreDispositionErrors: true,
				//ManualCredits:             true,
				MaxMessageSize: 1024,
				Name:           name,
				Properties: map[string]interface{}{
					"property": 123,
				},
				RequestedSenderSettleMode: ModeMixed.Ptr(),
				SettlementMode:            ModeSecond.Ptr(),
			},
			validate: func(t *testing.T, l *link) {
				//require.True(t, l.receiver.batching)
				//require.Equal(t, 1*time.Minute, l.receiver.batchMaxAge)
				require.Equal(t, encoding.MultiSymbol{"foo", "bar"}, l.Target.Capabilities)
				//require.Equal(t, uint32(32), l.receiver.maxCredit)
				require.Equal(t, DurabilityConfiguration, l.Target.Durable)
				require.True(t, l.dynamicAddr)
				require.Equal(t, ExpiryNever, l.Target.ExpiryPolicy)
				require.Equal(t, uint32(3), l.Target.Timeout)
				require.Equal(t, encoding.Filter{
					selectorFilter: &encoding.DescribedType{
						Descriptor: selectorFilterCode,
						Value:      "amqp.annotation.x-opt-offset > '100'",
					},
					"com.microsoft:session-filter": &encoding.DescribedType{
						Descriptor: uint64(0x00000137000000C),
						Value:      "123",
					},
				}, l.Source.Filter)
				require.False(t, l.detachOnDispositionError)
				//require.NotNil(t, l.receiver.manualCreditor)
				require.Equal(t, uint64(1024), l.MaxMessageSize)
				require.Equal(t, name, l.Key.name)
				require.Equal(t, map[encoding.Symbol]interface{}{
					"property": 123,
				}, l.properties)
				require.NotNil(t, l.SenderSettleMode)
				require.Equal(t, ModeMixed, *l.SenderSettleMode)
				require.NotNil(t, l.ReceiverSettleMode)
				require.Equal(t, ModeSecond, *l.ReceiverSettleMode)
				require.Empty(t, l.Source.Address)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			got, err := newReceivingLink(sourceAddr, nil, &Receiver{}, &tt.opts)
			require.NoError(t, err)
			require.NotNil(t, got)
			tt.validate(t, got)
		})
	}
}

func TestSessionFlowDisablesTransfer(t *testing.T) {
	t.Skip("TODO: finish for link testing")
	nextIncomingID := uint32(0)
	netConn := mocks.NewNetConn(senderFrameHandlerNoUnhandled(ModeUnsettled))

	client, err := New(netConn, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	session, err := client.NewSession(ctx, nil)
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

	client, err := New(netConn, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	snd, err := session.NewSender(ctx, "doesntwork", &SenderOptions{
		SettlementMode:              ModeMixed.Ptr(),
		RequestedReceiverSettleMode: ModeSecond.Ptr(),
	})
	cancel()
	require.Error(t, err)
	require.Nil(t, snd)
	require.NoError(t, client.Close())
}

// TODO: echo flow frame
