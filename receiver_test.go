package amqp

import (
	"context"
	"testing"
	"time"
)

func makeLink(mode ReceiverSettleMode) *link {
	return &link{
		close:              make(chan struct{}),
		done:               make(chan struct{}),
		receiverReady:      make(chan struct{}, 1),
		messages:           make(chan Message, 1),
		receiverSettleMode: &mode,
		unsettledMessages:  map[string]struct{}{},
	}
}

func doNothing(msg *Message) error {
	return nil
}

func accept(msg *Message) error {
	return msg.Accept(context.TODO())
}

func makeMessage(mode ReceiverSettleMode) Message {
	var tag []byte
	var done chan struct{}
	if mode == ModeSecond {
		tag = []byte("one")
		done = make(chan struct{})
	}
	return Message{
		deliveryID:  uint32(1),
		DeliveryTag: tag,
		doneSignal:  done,
	}
}

func TestReceiver_HandleMessageModeFirst_AutoAccept(t *testing.T) {
	r := &Receiver{
		link:         makeLink(ModeFirst),
		batching:     true, // allows to  avoid making the outgoing call on dispostion
		dispositions: make(chan messageDisposition, 2),
	}
	msg := makeMessage(ModeFirst)
	r.link.messages <- msg
	if r.link.countUnsettled() != 0 {
		// mode first messages have no delivery tag, thus there should be no unsettled message
		t.Fatal("expected zero unsettled count")
	}
	if err := r.HandleMessage(context.TODO(), doNothing); err != nil {
		t.Errorf("HandleMessage() error = %v", err)
	}

	if len(r.dispositions) != 0 {
		t.Errorf("the message should not have triggered a disposition")
	}
}

func TestReceiver_HandleMessageModeSecond_DontDispose(t *testing.T) {
	r := &Receiver{
		link:         makeLink(ModeSecond),
		batching:     true, // allows to  avoid making the outgoing call on dispostion
		dispositions: make(chan messageDisposition, 2),
	}
	msg := makeMessage(ModeSecond)
	r.link.messages <- msg
	r.link.addUnsettled(&msg)
	if err := r.HandleMessage(context.TODO(), doNothing); err != nil {
		t.Errorf("HandleMessage() error = %v", err)
	}
	if len(r.dispositions) != 0 {
		t.Errorf("it is up to the message handler to settle messages")
	}
	if r.link.countUnsettled() == 0 {
		t.Errorf("the message should still be tracked until settled")
	}
	// ensure channel wasn't closed
	select {
	case _, ok := <-msg.doneSignal:
		if !ok {
			t.Fatal("unexpected closing of doneSignal")
		}
	default:
		// channel wasn't closed
	}
}

func TestReceiver_HandleMessageModeSecond_removeFromUnsettledMapOnDisposition(t *testing.T) {
	r := &Receiver{
		link:         makeLink(ModeSecond),
		batching:     true, // allows to  avoid making the outgoing call on dispostion
		dispositions: make(chan messageDisposition, 1),
	}
	msg := makeMessage(ModeSecond)
	r.link.messages <- msg
	r.link.addUnsettled(&msg)
	// unblock the accept waiting on inflight disposition for modeSecond
	loop := true
	// call handle with the accept handler in a goroutine because it will block on inflight disposition.
	go func() {
		if err := r.HandleMessage(context.TODO(), accept); err != nil {
			t.Errorf("HandleMessage() error = %v", err)
		}
	}()

	// simulate batch disposition.
	// when the inflight has an entry, we know the Accept has been called
	for loop {
		r.inFlight.mu.Lock()
		inflightCount := len(r.inFlight.m)
		r.inFlight.mu.Unlock()
		if inflightCount > 0 {
			r.inFlight.remove(msg.deliveryID, nil, nil)
			loop = false
		}
		time.Sleep(1 * time.Millisecond)
	}

	if len(r.dispositions) == 0 {
		t.Errorf("the message should have triggered a disposition")
	}
	if r.link.countUnsettled() != 0 {
		t.Errorf("the message should be removed from unsettled map")
	}
	loop = false
	// ensure channel was closed
	select {
	case _, ok := <-msg.doneSignal:
		if !ok {
			// channel was closed
		}
	default:
		t.Fatal("expected closed of doneSignal")
	}
}
