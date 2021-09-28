package amqp

import (
	"context"
	"testing"
	"time"
)

func makeLink(mode ReceiverSettleMode) *link {
	return &link{
		close:              make(chan struct{}),
		Detached:           make(chan struct{}),
		ReceiverReady:      make(chan struct{}, 1),
		Messages:           make(chan Message, 1),
		ReceiverSettleMode: &mode,
		unsettledMessages:  map[string]struct{}{},
	}
}

func makeMessage(mode ReceiverSettleMode) Message {
	var tag []byte
	if mode == ModeSecond {
		tag = []byte("one")
	}
	return Message{
		deliveryID:  uint32(1),
		DeliveryTag: tag,
	}
}

func TestReceiver_HandleMessageModeFirst_AutoAccept(t *testing.T) {
	r := &Receiver{
		link:         makeLink(ModeFirst),
		batching:     true, // allows to  avoid making the outgoing call on dispostion
		dispositions: make(chan messageDisposition, 2),
	}
	msg := makeMessage(ModeFirst)
	r.link.Messages <- msg
	if r.link.countUnsettled() != 0 {
		// mode first messages have no delivery tag, thus there should be no unsettled message
		t.Fatal("expected zero unsettled count")
	}
	if _, err := r.Receive(context.TODO()); err != nil {
		t.Errorf("Receive() error = %v", err)
	}

	if len(r.dispositions) != 1 {
		t.Errorf("the message should have triggered a disposition")
	}
}

func TestReceiver_HandleMessageModeSecond_DontDispose(t *testing.T) {
	r := &Receiver{
		link:         makeLink(ModeSecond),
		batching:     true, // allows to  avoid making the outgoing call on dispostion
		dispositions: make(chan messageDisposition, 2),
	}
	msg := makeMessage(ModeSecond)
	r.link.Messages <- msg
	r.link.addUnsettled(&msg)
	if _, err := r.Receive(context.TODO()); err != nil {
		t.Errorf("Receive() error = %v", err)
	}
	if len(r.dispositions) != 0 {
		t.Errorf("it is up to the message handler to settle messages")
	}
	if r.link.countUnsettled() == 0 {
		t.Errorf("the message should still be tracked until settled")
	}
}

func TestReceiver_HandleMessageModeSecond_removeFromUnsettledMapOnDisposition(t *testing.T) {
	r := &Receiver{
		link:         makeLink(ModeSecond),
		batching:     true, // allows to  avoid making the outgoing call on dispostion
		dispositions: make(chan messageDisposition, 1),
	}
	msg := makeMessage(ModeSecond)
	r.link.Messages <- msg
	r.link.addUnsettled(&msg)
	// unblock the accept waiting on inflight disposition for modeSecond
	loop := true
	// call handle with the accept handler in a goroutine because it will block on inflight disposition.
	go func() {
		msg, err := r.Receive(context.TODO())
		if err != nil {
			t.Errorf("Receive() error = %v", err)
		}
		if err = r.AcceptMessage(context.TODO(), msg); err != nil {
			t.Errorf("AcceptMessage() error = %v", err)
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
}
