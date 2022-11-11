package amqp

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/stretchr/testify/require"
)

func TestManualCreditorIssueCredits(t *testing.T) {
	mc := manualCreditor{}
	require.NoError(t, mc.IssueCredit(3))

	drain, credits := mc.FlowBits(1)
	require.False(t, drain)
	require.EqualValues(t, 3+1, credits, "flow frame includes the pending credits and  our current credits")

	// flow clears the previous data once it's been called.
	drain, credits = mc.FlowBits(4)
	require.False(t, drain)
	require.EqualValues(t, 0, credits, "drain flow frame always sets link-credit to 0")
}

func TestManualCreditorDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	mc := manualCreditor{}

	require.NoError(t, mc.IssueCredit(3))

	// only one drain allowed at a time.
	drainRoutines := sync.WaitGroup{}
	drainRoutines.Add(2)

	l := newTestLink(t)
	var err1, err2 error

	go func() {
		defer drainRoutines.Done()
		err1 = mc.Drain(ctx, l)
	}()

	go func() {
		defer drainRoutines.Done()
		err2 = mc.Drain(ctx, l)
	}()

	// one of the drain calls will have succeeded, the other one should still be blocking.
	time.Sleep(time.Second * 2)

	// the next time someone requests a flow frame it'll drain (this doesn't affect the blocked Drain() calls)
	drain, credits := mc.FlowBits(101)
	require.True(t, drain)
	require.EqualValues(t, 0, credits, "Drain always drains with 0 credit")

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
		err := mc.Drain(ctx, newTestLink(t))
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

	require.Error(t, mc.Drain(ctx, newTestLink(t)), context.Canceled.Error())
}

func TestManualCreditorDrainReturnsProperError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	errs := []*Error{
		&encoding.Error{
			Condition: ErrorDecodeError,
		},
		nil,
	}

	for i, err := range errs {
		t.Run(fmt.Sprintf("Error[%d]", i), func(t *testing.T) {
			mc := manualCreditor{}
			link := newTestLink(t)

			link.detachError = err
			close(link.detached)

			detachErr := mc.Drain(ctx, link)
			require.Equal(t, detachErr, &DetachError{RemoteError: err})
		})
	}
}
