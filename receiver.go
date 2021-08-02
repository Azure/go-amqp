package amqp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type messageDisposition struct {
	id    uint32
	state interface{}
}

// Receiver receives messages on a single AMQP link.
type Receiver struct {
	link         *link                   // underlying link
	batching     bool                    // enable batching of message dispositions
	batchMaxAge  time.Duration           // maximum time between the start n batch and sending the batch to the server
	dispositions chan messageDisposition // message dispositions are sent on this channel when batching is enabled
	maxCredit    uint32                  // maximum allowed inflight messages
	inFlight     inFlight                // used to track message disposition when rcv-settle-mode == second
}

// HandleMessage takes in a func to handle the incoming message.
// Blocks until a message is received, ctx completes, or an error occurs.
// When using ModeSecond, You must take an action on the message in the provided handler (Accept/Reject/Release/Modify)
// or the unsettled message tracker will get out of sync, and reduce the flow.
// When using ModeFirst, the message is spontaneously Accepted at reception.
func (r *Receiver) HandleMessage(ctx context.Context, handle func(*Message) error) error {
	debug(3, "Entering link %s Receive()", r.link.key.name)

	trackCompletion := func(msg *Message) {
		if msg.doneSignal == nil {
			msg.doneSignal = make(chan struct{})
		}
		<-msg.doneSignal
		r.link.deleteUnsettled(msg)
		debug(3, "Receive() deleted unsettled %d", msg.deliveryID)
		if atomic.LoadUint32(&r.link.paused) == 1 {
			select {
			case r.link.receiverReady <- struct{}{}:
				debug(3, "Receive() unpause link on completion")
			default:
			}
		}
	}
	callHandler := func(msg *Message) error {
		debug(3, "Receive() blocking %d", msg.deliveryID)
		msg.receiver = r
		// we only need to track message disposition for mode second
		// spec : http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-receiver-settle-mode
		if r.link.receiverSettleMode.value() == ModeSecond {
			go trackCompletion(msg)
		}
		// tracks messages until exiting handler
		if err := handle(msg); err != nil {
			debug(3, "Receive() blocking %d - error: %s", msg.deliveryID, err.Error())
			return err
		}
		return nil
	}

	select {
	case msg := <-r.link.messages:
		return callHandler(&msg)
	case <-ctx.Done():
		return ctx.Err()
	default:
		// pass through, to buffer msgs when the handler is busy
	}

	select {
	case msg := <-r.link.messages:
		return callHandler(&msg)
	case <-r.link.detached:
		return r.link.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Receive returns the next message from the sender.
//
// Blocks until a message is received, ctx completes, or an error occurs.
// Deprecated: prefer HandleMessage
func (r *Receiver) Receive(ctx context.Context) (*Message, error) {
	if atomic.LoadUint32(&r.link.paused) == 1 {
		select {
		case r.link.receiverReady <- struct{}{}:
		default:
		}
	}

	// non-blocking receive to ensure buffered messages are
	// delivered regardless of whether the link has been closed.
	select {
	case msg := <-r.link.messages:
		// we remove the message from unsettled map as soon as it's popped off the channel
		// This makes the unsettled count the same as messages buffer count
		// and keeps the behavior the same as before the unsettled messages tracking was introduced
		defer r.link.deleteUnsettled(&msg)
		debug(3, "Receive() non blocking %d", msg.deliveryID)
		msg.receiver = r
		return &msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// wait for the next message
	select {
	case msg := <-r.link.messages:
		// we remove the message from unsettled map as soon as it's popped off the channel
		// This makes the unsettled count the same as messages buffer count
		// and keeps the behavior the same as before the unsettled messages tracking was introduced
		defer r.link.deleteUnsettled(&msg)
		debug(3, "Receive() blocking %d", msg.deliveryID)
		msg.receiver = r
		return &msg, nil
	case <-r.link.detached:
		return nil, r.link.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Address returns the link's address.
func (r *Receiver) Address() string {
	if r.link.source == nil {
		return ""
	}
	return r.link.source.Address
}

// LinkSourceFilterValue retrieves the specified link source filter value or nil if it doesn't exist.
func (r *Receiver) LinkSourceFilterValue(name string) interface{} {
	if r.link.source == nil {
		return nil
	}
	filter, ok := r.link.source.Filter[symbol(name)]
	if !ok {
		return nil
	}
	return filter.value
}

// Close closes the Receiver and AMQP link.
//
// If ctx expires while waiting for servers response, ctx.Err() will be returned.
// The session will continue to wait for the response until the Session or Client
// is closed.
func (r *Receiver) Close(ctx context.Context) error {
	return r.link.Close(ctx)
}

func (r *Receiver) dispositionBatcher() {
	// batch operations:
	// Keep track of the first and last delivery ID, incrementing as
	// Accept() is called. After last-first == batchSize, send disposition.
	// If Reject()/Release() is called, send one disposition for previously
	// accepted, and one for the rejected/released message. If messages are
	// accepted out of order, send any existing batch and the current message.
	var (
		batchSize    = r.maxCredit
		batchStarted bool
		first        uint32
		last         uint32
	)

	// create an unstarted timer
	batchTimer := time.NewTimer(1 * time.Minute)
	batchTimer.Stop()
	defer batchTimer.Stop()

	for {
		select {
		case msgDis := <-r.dispositions:

			// not accepted or batch out of order
			_, isAccept := msgDis.state.(*stateAccepted)
			if !isAccept || (batchStarted && last+1 != msgDis.id) {
				// send the current batch, if any
				if batchStarted {
					lastCopy := last
					err := r.sendDisposition(first, &lastCopy, &stateAccepted{})
					if err != nil {
						r.inFlight.remove(first, &lastCopy, err)
					}
					batchStarted = false
				}

				// send the current message
				err := r.sendDisposition(msgDis.id, nil, msgDis.state)
				if err != nil {
					r.inFlight.remove(msgDis.id, nil, err)
				}
				continue
			}

			if batchStarted {
				// increment last
				last++
			} else {
				// start new batch
				batchStarted = true
				first = msgDis.id
				last = msgDis.id
				batchTimer.Reset(r.batchMaxAge)
			}

			// send batch if current size == batchSize
			if last-first+1 >= batchSize {
				lastCopy := last
				err := r.sendDisposition(first, &lastCopy, &stateAccepted{})
				if err != nil {
					r.inFlight.remove(first, &lastCopy, err)
				}
				batchStarted = false
				if !batchTimer.Stop() {
					<-batchTimer.C // batch timer must be drained if stop returns false
				}
			}

		// maxBatchAge elapsed, send batch
		case <-batchTimer.C:
			lastCopy := last
			err := r.sendDisposition(first, &lastCopy, &stateAccepted{})
			if err != nil {
				r.inFlight.remove(first, &lastCopy, err)
			}
			batchStarted = false
			batchTimer.Stop()

		case <-r.link.detached:
			return
		}
	}
}

// sendDisposition sends a disposition frame to the peer
func (r *Receiver) sendDisposition(first uint32, last *uint32, state interface{}) error {
	fr := &performDisposition{
		Role:    roleReceiver,
		First:   first,
		Last:    last,
		Settled: r.link.receiverSettleMode == nil || *r.link.receiverSettleMode == ModeFirst,
		State:   state,
	}

	debug(1, "TX: %s", fr)
	return r.link.session.txFrame(fr, nil)
}

func (r *Receiver) messageDisposition(ctx context.Context, id uint32, state interface{}) error {
	var wait chan error
	if r.link.receiverSettleMode != nil && *r.link.receiverSettleMode == ModeSecond {
		debug(3, "RX: add %d to inflight", id)
		wait = r.inFlight.add(id)
	}

	if r.batching {
		r.dispositions <- messageDisposition{id: id, state: state}
	} else {
		err := r.sendDisposition(id, nil, state)
		if err != nil {
			return err
		}
	}

	if wait == nil {
		return nil
	}

	select {
	case err := <-wait:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// inFlight tracks in-flight message dispositions allowing receivers
// to block waiting for the server to respond when an appropriate
// settlement mode is configured.
type inFlight struct {
	mu sync.Mutex
	m  map[uint32]chan error
}

func (f *inFlight) add(id uint32) chan error {
	wait := make(chan error, 1)

	f.mu.Lock()
	if f.m == nil {
		f.m = map[uint32]chan error{id: wait}
	} else {
		f.m[id] = wait
	}
	f.mu.Unlock()

	return wait
}

func (f *inFlight) remove(first uint32, last *uint32, err error) {
	f.mu.Lock()

	if f.m == nil {
		f.mu.Unlock()
		return
	}

	ll := first
	if last != nil {
		ll = *last
	}

	for i := first; i <= ll; i++ {
		wait, ok := f.m[i]
		if ok {
			wait <- err
			delete(f.m, i)
		}
	}

	f.mu.Unlock()
}

func (f *inFlight) clear(err error) {
	f.mu.Lock()
	for id, wait := range f.m {
		wait <- err
		delete(f.m, id)
	}
	f.mu.Unlock()
}
