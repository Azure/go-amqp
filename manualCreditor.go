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

var ErrLinkDraining = errors.New("link is currently draining, no credits can be added")
var ErrAlreadyDraining = errors.New("drain already in process")

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
func (mc *manualCreditor) Drain(ctx context.Context) error {
	mc.mu.Lock()

	if mc.drained != nil {
		mc.mu.Unlock()
		return ErrAlreadyDraining
	}

	mc.drained = make(chan struct{})

	mc.mu.Unlock()

	// send drain, wait for responding flow frame
	select {
	case <-mc.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// AddCredit queues up additional credits to be requested at the next
// call of FlowBits()
func (mc *manualCreditor) AddCredit(credits uint32) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.drained != nil {
		return ErrLinkDraining
	}

	mc.creditsToAdd += credits
	return nil
}
