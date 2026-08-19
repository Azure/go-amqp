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

func TestCreditorIssueCredits(t *testing.T) {
	r := newTestLink(t)
	require.NoError(t, r.creditor.IssueCredit(3))

	drain, credits, properties := r.creditor.FlowBits(1)
	require.False(t, drain)
	require.EqualValues(t, 3+1, credits, "flow frame includes the pending credits and our current credits")
	require.Nil(t, properties, "no properties were queued")

	// flow clears the previous data once it's been called.
	drain, credits, properties = r.creditor.FlowBits(4)
	require.False(t, drain)
	require.EqualValues(t, 0, credits, "drain flow frame always sets link-credit to 0")
	require.Nil(t, properties)
}

func TestCreditorIssueCreditWithProperties(t *testing.T) {
	r := newTestLink(t)
	props := map[encoding.Symbol]any{"foo:bar": []string{"tok1", "tok2"}}
	require.NoError(t, r.creditor.IssueCreditWithProperties(2, props))

	drain, credits, gotProps := r.creditor.FlowBits(0)
	require.False(t, drain)
	require.EqualValues(t, 2, credits)
	require.Equal(t, props, gotProps)

	// properties (and credits) are cleared once read.
	drain, credits, gotProps = r.creditor.FlowBits(0)
	require.False(t, drain)
	require.EqualValues(t, 0, credits)
	require.Nil(t, gotProps)
}

func TestCreditorIssueCreditWithPropertiesMergesWithPlainCredit(t *testing.T) {
	r := newTestLink(t)
	require.NoError(t, r.creditor.IssueCredit(3))
	props := map[encoding.Symbol]any{"foo:bar": []string{"tok1"}}
	require.NoError(t, r.creditor.IssueCreditWithProperties(2, props))

	drain, credits, gotProps := r.creditor.FlowBits(1)
	require.False(t, drain)
	require.EqualValues(t, 3+2+1, credits, "credit from IssueCredit and IssueCreditWithProperties accumulate together")
	require.Equal(t, props, gotProps)
}

func TestCreditorIssueCreditWithPropertiesWhileDrainingFails(t *testing.T) {
	r := newTestLink(t)
	r.creditor.drained = make(chan struct{})
	defer close(r.creditor.drained)

	err := r.creditor.IssueCreditWithProperties(1, map[encoding.Symbol]any{"x": "y"})
	require.ErrorIs(t, err, errLinkDraining)
}

func TestCreditorDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	r := newTestLink(t)
	require.NoError(t, r.creditor.IssueCredit(3))

	// only one drain allowed at a time.
	drainRoutines := sync.WaitGroup{}
	drainRoutines.Add(2)

	var err1, err2 error

	go func() {
		defer drainRoutines.Done()
		err1 = r.creditor.Drain(ctx, r)
	}()

	go func() {
		defer drainRoutines.Done()
		err2 = r.creditor.Drain(ctx, r)
	}()

	// one of the drain calls will have succeeded, the other one should still be blocking.
	time.Sleep(time.Second * 2)

	// the next time someone requests a flow frame it'll drain (this doesn't affect the blocked Drain() calls)
	drain, credits, _ := r.creditor.FlowBits(101)
	require.True(t, drain)
	require.EqualValues(t, 0, credits, "Drain always drains with 0 credit")

	// unblock the last of the drainers
	r.creditor.EndDrain()
	require.Nil(t, r.creditor.drained, "drain completes and removes the drained channel")

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

func TestCreditorIssueCreditsWhileDrainingFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	r := newTestLink(t)
	require.NoError(t, r.creditor.IssueCredit(3))

	// only one drain allowed at a time.
	drainRoutines := sync.WaitGroup{}
	drainRoutines.Add(2)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := r.creditor.Drain(ctx, newTestLink(t))
		require.NoError(t, err)
	}()

	time.Sleep(time.Second * 2)

	// drain is still active, so...
	require.Error(t, r.creditor.IssueCredit(1), errLinkDraining.Error())

	r.creditor.EndDrain()
	wg.Wait()
}

func TestCreditorDrainRespectsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	mc := creditor{}

	cancel()

	require.Error(t, mc.Drain(ctx, newTestLink(t)), context.Canceled.Error())
}

func TestCreditorDrainReturnsProperError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	errs := []*Error{
		&encoding.Error{
			Condition: ErrCondDecodeError,
		},
		nil,
	}

	for i, err := range errs {
		t.Run(fmt.Sprintf("Error[%d]", i), func(t *testing.T) {
			mc := creditor{}
			link := newTestLink(t)

			link.l.doneErr = err
			close(link.l.done)

			detachErr := mc.Drain(ctx, link)
			require.Equal(t, err, detachErr)
		})
	}
}
