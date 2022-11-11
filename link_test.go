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
	"github.com/stretchr/testify/require"
)

func TestLinkFlowThatNeedsToReplenishCredits(t *testing.T) {
	l := newTestLink(t)
	go l.mux()
	defer close(l.close)

	err := l.DrainCredit(context.Background())
	require.Error(t, err, "drain can only be used with receiver links using manual credit management")

	err = l.IssueCredit(1)
	require.Error(t, err, "issueCredit can only be used with receiver links using manual credit management")

	// and flow goes through the non-manual credit path
	require.EqualValues(t, 0, l.linkCredit, "No link credits have been added")

	// we've consumed half of the maximum credit we're allowed to have - reflow!
	l.maxCredit = 2
	l.linkCredit = 1
	l.unsettledMessages = map[string]struct{}{}

	select {
	case l.receiverReady <- struct{}{}:
		// woke up mux
	default:
		t.Fatal("failed to wake up mux")
	}

	// flow happens immmediately in 'mux'
	txFrame := <-l.session.tx

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
	go l.mux()
	defer close(l.close)

	err := l.DrainCredit(context.Background())
	require.Error(t, err, "drain can only be used with receiver links using manual credit management")

	err = l.IssueCredit(1)
	require.Error(t, err, "issueCredit can only be used with receiver links using manual credit management")

	// and flow goes through the non-manual credit path
	require.EqualValues(t, 0, l.linkCredit, "No link credits have been added")

	l.maxCredit = 2
	l.linkCredit = 0
	l.unsettledMessages = map[string]struct{}{
		"hello":  {},
		"hello2": {},
	}

	select {
	case l.receiverReady <- struct{}{}:
		// woke up mux
	default:
		t.Fatal("failed to wake up mux")
	}

	require.Zero(t, l.linkCredit)
}

func TestLinkFlowDrain(t *testing.T) {
	l := newTestLink(t)
	// now initialize it as a manual credit link
	l.manualCreditor = &manualCreditor{}
	go l.mux()
	defer close(l.close)

	go func() {
		<-l.receiverReady
		l.manualCreditor.EndDrain()
	}()

	require.NoError(t, l.DrainCredit(context.Background()))
}

func TestLinkFlowWithManualCreditor(t *testing.T) {
	l := newTestLink(t)
	l.manualCreditor = &manualCreditor{}
	l.linkCredit = 1
	go l.mux()
	defer close(l.close)

	require.NoError(t, l.IssueCredit(100))

	// flow happens immmediately in 'mux'
	txFrame := <-l.session.tx

	switch frame := txFrame.(type) {
	case *frames.PerformFlow:
		require.False(t, frame.Drain)
		require.EqualValues(t, 100+1, *frame.LinkCredit)
	default:
		require.Fail(t, fmt.Sprintf("Unexpected frame was transferred: %+v", txFrame))
	}
}

func TestLinkFlowWithDrain(t *testing.T) {
	t.Skip("for now")
	l := newTestLink(t)
	l.manualCreditor = &manualCreditor{}
	go l.mux()
	defer close(l.close)

	errChan := make(chan error)

	go func(errChan chan error) {
		<-l.receiverReady

		select {
		case l.receiverReady <- struct{}{}:
			// woke up mux
		default:
			errChan <- errors.New("failed to wake up mux")
			return
		}

		// flow happens immmediately in 'mux'
		txFrame := <-l.session.tx

		switch frame := txFrame.(type) {
		case *frames.PerformFlow:
			if !frame.Drain {
				errChan <- errors.New("expected drain to be true")
				return
			}

			// When we're draining we just automatically set the flow link credit to 0.
			// This should allow any outstanding messages to get flushed.
			if lc := *frame.LinkCredit; lc != 0 {
				errChan <- fmt.Errorf("unepxected LinkCredit: %d", lc)
				return
			}

		default:
			errChan <- fmt.Errorf("Unexpected frame was transferred: %+v", txFrame)
			return
		}

		// simulate the return of the flow from the service
		if err := l.muxHandleFrame(&frames.PerformFlow{Drain: true}); err != nil {
			errChan <- err
			return
		}

		close(errChan)
	}(errChan)

	l.linkCredit = 1
	require.NoError(t, l.DrainCredit(context.Background()))
	require.NoError(t, <-errChan)
}

func TestLinkFlowWithManualCreditorAndNoFlowNeeded(t *testing.T) {
	l := newTestLink(t)
	l.manualCreditor = &manualCreditor{}
	l.linkCredit = 1
	go l.mux()
	defer close(l.close)

	select {
	case l.receiverReady <- struct{}{}:
		// woke up mux
	default:
		t.Fatal("failed to wake up mux")
	}

	// flow happens immmediately in 'mux'
	select {
	case fr := <-l.session.tx: // there won't be a flow this time.
		require.Failf(t, "No flow frame would be needed since no link credits were added and drain was not requested", "Frame was %+v", fr)
	case <-time.After(time.Second * 2):
		// this is the expected case since no frame will be sent.
	}
}

func TestMuxFlowHandlesDrainProperly(t *testing.T) {
	l := newTestLink(t)
	l.manualCreditor = &manualCreditor{}
	l.linkCredit = 101
	go l.mux()
	defer close(l.close)

	// simulate what our 'drain' call to muxFlow would look like
	// when draining
	require.NoError(t, l.muxFlow(0, true))
	require.EqualValues(t, 101, l.linkCredit, "credits are untouched when draining")

	// when doing a non-drain flow we update the linkCredit to our new link credit total.
	require.NoError(t, l.muxFlow(501, false))
	require.EqualValues(t, 501, l.linkCredit, "credits are untouched when draining")
}

func newTestLink(t *testing.T) *Receiver {
	l := &Receiver{
		link: link{
			source: &frames.Source{},
			// adding just enough so the debug() print will still work...
			// debug(1, "FLOW Link Mux half: source: %s, inflight: %d, credit: %d, deliveryCount: %d, messages: %d, unsettled: %d, maxCredit : %d, settleMode: %s", l.source.Address, l.receiver.inFlight.len(), l.linkCredit, l.deliveryCount, len(l.messages), l.countUnsettled(), l.receiver.maxCredit, l.receiverSettleMode.String())
			detached: make(chan struct{}),
			session: &Session{
				tx:   make(chan frames.FrameBody, 100),
				done: make(chan struct{}),
			},
			rx:    make(chan frames.FrameBody, 100),
			close: make(chan struct{}),
		},
		inFlight:      inFlight{},
		receiverReady: make(chan struct{}, 1),
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
		validate func(t *testing.T, l *Sender)
	}{
		{
			label: "default options",
			validate: func(t *testing.T, l *Sender) {
				require.Empty(t, l.target.Capabilities)
				require.Equal(t, DurabilityNone, l.source.Durable)
				require.False(t, l.dynamicAddr)
				require.Empty(t, l.source.ExpiryPolicy)
				require.Zero(t, l.source.Timeout)
				require.True(t, l.detachOnDispositionError)
				require.NotEmpty(t, l.key.name)
				require.Empty(t, l.properties)
				require.Nil(t, l.senderSettleMode)
				require.Nil(t, l.receiverSettleMode)
				require.Equal(t, targetAddr, l.target.Address)
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
			validate: func(t *testing.T, l *Sender) {
				require.Equal(t, encoding.MultiSymbol{"foo", "bar"}, l.source.Capabilities)
				require.Equal(t, DurabilityUnsettledState, l.source.Durable)
				require.True(t, l.dynamicAddr)
				require.Equal(t, ExpiryLinkDetach, l.source.ExpiryPolicy)
				require.Equal(t, uint32(5), l.source.Timeout)
				require.False(t, l.detachOnDispositionError)
				require.Equal(t, name, l.key.name)
				require.Equal(t, map[encoding.Symbol]interface{}{
					"property": 123,
				}, l.properties)
				require.NotNil(t, l.senderSettleMode)
				require.Equal(t, ModeSettled, *l.senderSettleMode)
				require.NotNil(t, l.receiverSettleMode)
				require.Equal(t, ModeFirst, *l.receiverSettleMode)
				require.Empty(t, l.target.Address)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			got, err := newSender(targetAddr, nil, &tt.opts)
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
		validate func(t *testing.T, l *Receiver)

		wantSource     *frames.Source
		wantTarget     *frames.Target
		wantProperties map[encoding.Symbol]interface{}
	}{
		{
			label: "default options",
			validate: func(t *testing.T, l *Receiver) {
				//require.False(t, l.receiver.batching)
				//require.Equal(t, defaultLinkBatchMaxAge, l.receiver.batchMaxAge)
				require.Empty(t, l.target.Capabilities)
				//require.Equal(t, defaultLinkCredit, l.receiver.maxCredit)
				require.Equal(t, DurabilityNone, l.target.Durable)
				require.False(t, l.dynamicAddr)
				require.Empty(t, l.target.ExpiryPolicy)
				require.Zero(t, l.target.Timeout)
				require.Empty(t, l.source.Filter)
				//require.Nil(t, l.receiver.manualCreditor)
				require.Zero(t, l.maxMessageSize)
				require.NotEmpty(t, l.key.name)
				require.Empty(t, l.properties)
				require.Nil(t, l.senderSettleMode)
				require.Nil(t, l.receiverSettleMode)
				require.Equal(t, sourceAddr, l.source.Address)
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
				//ManualCredits:             true,
				MaxMessageSize: 1024,
				Name:           name,
				Properties: map[string]interface{}{
					"property": 123,
				},
				RequestedSenderSettleMode: ModeMixed.Ptr(),
				SettlementMode:            ModeSecond.Ptr(),
			},
			validate: func(t *testing.T, l *Receiver) {
				//require.True(t, l.receiver.batching)
				//require.Equal(t, 1*time.Minute, l.receiver.batchMaxAge)
				require.Equal(t, encoding.MultiSymbol{"foo", "bar"}, l.target.Capabilities)
				//require.Equal(t, uint32(32), l.receiver.maxCredit)
				require.Equal(t, DurabilityConfiguration, l.target.Durable)
				require.True(t, l.dynamicAddr)
				require.Equal(t, ExpiryNever, l.target.ExpiryPolicy)
				require.Equal(t, uint32(3), l.target.Timeout)
				require.Equal(t, encoding.Filter{
					selectorFilter: &encoding.DescribedType{
						Descriptor: selectorFilterCode,
						Value:      "amqp.annotation.x-opt-offset > '100'",
					},
					"com.microsoft:session-filter": &encoding.DescribedType{
						Descriptor: uint64(0x00000137000000C),
						Value:      "123",
					},
				}, l.source.Filter)
				//require.NotNil(t, l.receiver.manualCreditor)
				require.Equal(t, uint64(1024), l.maxMessageSize)
				require.Equal(t, name, l.key.name)
				require.Equal(t, map[encoding.Symbol]interface{}{
					"property": 123,
				}, l.properties)
				require.NotNil(t, l.senderSettleMode)
				require.Equal(t, ModeMixed, *l.senderSettleMode)
				require.NotNil(t, l.receiverSettleMode)
				require.Equal(t, ModeSecond, *l.receiverSettleMode)
				require.Empty(t, l.source.Address)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			got, err := newReceiver(sourceAddr, nil, &tt.opts)
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
