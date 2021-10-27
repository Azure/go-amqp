package amqp

import (
	"context"
	"errors"
	"sync"
)

type manualCreditor struct {
	mu sync.Mutex

	// future values for the next flow frame.
	pendingDrain bool
	creditsToAdd uint32

	// drained is set when a drain is active and we're waiting
	// for the corresponding flow from the remote.
	drained chan struct{}
}

var (
	errLinkDraining    = errors.New("link is currently draining, no credits can be added")
	errAlreadyDraining = errors.New("drain already in process")
)

// ErrCreditLimitExceeded is returned from Receiver.IssueCredit when manual credit
// management is enabled.  It indicates that the incoming rate of messages is greater
// than the rate which messages are received, and no more credit should be issued
// until the messages have been processed (call Receiver.Receive).
var ErrCreditLimitExceeded = errors.New("link credit exceeded, too many outstanding messages")

// EndDrain ends the current drain, unblocking any active Drain calls.
func (mc *manualCreditor) EndDrain() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.drained != nil {
		close(mc.drained)
		mc.drained = nil
	}
}

// FlowBits gets gets the proper values for the next flow frame
// and resets the internal state.
func (mc *manualCreditor) FlowBits() (bool, uint32) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	drain := mc.drained != nil
	credits := mc.creditsToAdd

	mc.creditsToAdd = 0
	mc.pendingDrain = false

	return drain, credits
}

// Drain initiates a drain and blocks until EndDrain is called.
func (mc *manualCreditor) Drain(ctx context.Context, l *link) error {
	mc.mu.Lock()

	if mc.drained != nil {
		mc.mu.Unlock()
		return errAlreadyDraining
	}

	mc.drained = make(chan struct{})
	// use a local copy to avoid racing with EndDrain()
	drained := mc.drained

	mc.mu.Unlock()

	// send drain, wait for responding flow frame
	select {
	case <-drained:
		return nil
	case <-l.close:
		return l.err
	case <-l.Detached:
		return l.detachError
	case <-ctx.Done():
		return ctx.Err()
	}
}

// IssueCredit queues up additional credits to be requested at the next
// call of FlowBits()
func (mc *manualCreditor) IssueCredit(credits uint32, l *link) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.drained != nil {
		return errLinkDraining
	}

	// don't continue to issue credit if it will exhauste our buffer as
	// that will just lead to a hard-to-diagnose hang in link.muxReceive
	if len(l.Messages)+int(l.linkCredit)+int(credits) > cap(l.Messages) {
		return ErrCreditLimitExceeded
	}

	mc.creditsToAdd += credits
	return nil
}
