package amqp

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestManualCreditorIssueCredits(t *testing.T) {
	mc := manualCreditor{}
	require.NoError(t, mc.IssueCredit(3))

	drain, credits := mc.FlowBits()
	require.False(t, drain)
	require.EqualValues(t, 3, credits)

	// flow clears the previous data once it's been called.
	drain, credits = mc.FlowBits()
	require.False(t, drain)
	require.EqualValues(t, 0, credits)
}

func TestManualCreditorDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	mc := manualCreditor{}
	require.NoError(t, mc.IssueCredit(3))

	// only one drain allowed at a time.
	drainRoutines := sync.WaitGroup{}
	drainRoutines.Add(2)

	var err1, err2 error

	go func() {
		defer drainRoutines.Done()
		err1 = mc.Drain(ctx)
	}()

	go func() {
		defer drainRoutines.Done()
		err2 = mc.Drain(ctx)
	}()

	// one of the drain calls will have succeeded, the other one should still be blocking.
	time.Sleep(time.Second * 2)

	// the next time someone requests a flow frame it'll drain and take
	// any accumulated credits (this doesn't affect the blocked Drain() calls)
	drain, credits := mc.FlowBits()
	require.True(t, drain)
	require.EqualValues(t, 3, credits)

	// unblock the last of the drainers
	mc.EndDrain()
	require.Nil(t, mc.drained, "drain completes and removes the drained channel")

	// wait for all the drain routines to end
	drainRoutines.Wait()

	// one of them should have failed (if both succeeded we've somehow let them both run)
	require.False(t, err1 == nil && err2 == nil)

	if err1 == nil {
		require.Error(t, err2, errAlreadyDraining.Error())
	} else {
		require.Error(t, err1, errAlreadyDraining.Error())
	}
}

func TestManualCreditorIssueCreditsWhileDrainingFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	mc := manualCreditor{}
	require.NoError(t, mc.IssueCredit(3))

	// only one drain allowed at a time.
	drainRoutines := sync.WaitGroup{}
	drainRoutines.Add(2)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := mc.Drain(ctx)
		require.NoError(t, err)
	}()

	time.Sleep(time.Second * 2)

	// drain is still active, so...
	require.Error(t, mc.IssueCredit(1), errLinkDraining.Error())

	mc.EndDrain()
	wg.Wait()
}

func TestManualCreditorDrainRespectsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	mc := manualCreditor{}

	cancel()

	require.Error(t, mc.Drain(ctx), context.Canceled.Error())
}
