package amqp

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

// Sender sends messages on a single AMQP link.
type Sender struct {
	link *link

	mu              sync.Mutex // protects buf and nextDeliveryTag
	buf             buffer
	nextDeliveryTag uint64
}

// ID() is the ID of the link used for this Sender.
func (s *Sender) ID() string {
	return s.link.key.name
}

// Send sends a Message.
//
// Blocks until the message is sent, ctx completes, or an error occurs.
//
// Send is safe for concurrent use. Since only a single message can be
// sent on a link at a time, this is most useful when settlement confirmation
// has been requested (receiver settle mode is "Second"). In this case,
// additional messages can be sent while the current goroutine is waiting
// for the confirmation.
func (s *Sender) Send(ctx context.Context, msg *Message) error {
	if err := s.link.Check(); err != nil {
		return err
	}

	done, err := s.send(ctx, msg)
	if err != nil {
		return err
	}

	// wait for transfer to be confirmed
	select {
	case state := <-done:
		if state, ok := state.(*stateRejected); ok {
			return state.Error
		}
		return nil
	case <-s.link.detached:
		return s.link.err
	case <-ctx.Done():
		return errorWrapf(ctx.Err(), "awaiting send")
	}
}

// send is separated from Send so that the mutex unlock can be deferred without
// locking the transfer confirmation that happens in Send.
func (s *Sender) send(ctx context.Context, msg *Message) (chan deliveryState, error) {
	if len(msg.DeliveryTag) > maxDeliveryTagLength {
		return nil, errorErrorf("delivery tag is over the allowed %v bytes, len: %v", maxDeliveryTagLength, len(msg.DeliveryTag))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.buf.reset()
	err := msg.marshal(&s.buf)
	if err != nil {
		return nil, err
	}

	if s.link.maxMessageSize != 0 && uint64(s.buf.len()) > s.link.maxMessageSize {
		return nil, errorErrorf("encoded message size exceeds max of %d", s.link.maxMessageSize)
	}

	var (
		maxPayloadSize = int64(s.link.session.conn.peerMaxFrameSize) - maxTransferFrameHeader
		sndSettleMode  = s.link.senderSettleMode
		senderSettled  = sndSettleMode != nil && (*sndSettleMode == ModeSettled || (*sndSettleMode == ModeMixed && msg.SendSettled))
		deliveryID     = atomic.AddUint32(&s.link.session.nextDeliveryID, 1)
	)

	deliveryTag := msg.DeliveryTag
	if len(deliveryTag) == 0 {
		// use uint64 encoded as []byte as deliveryTag
		deliveryTag = make([]byte, 8)
		binary.BigEndian.PutUint64(deliveryTag, s.nextDeliveryTag)
		s.nextDeliveryTag++
	}

	fr := performTransfer{
		Handle:        s.link.handle,
		DeliveryID:    &deliveryID,
		DeliveryTag:   deliveryTag,
		MessageFormat: &msg.Format,
		More:          s.buf.len() > 0,
	}

	for fr.More {
		buf, _ := s.buf.next(maxPayloadSize)
		fr.Payload = append([]byte(nil), buf...)
		fr.More = s.buf.len() > 0
		if !fr.More {
			// SSM=settled: overrides RSM; no acks.
			// SSM=unsettled: sender should wait for receiver to ack
			// RSM=first: receiver considers it settled immediately, but must still send ack (SSM=unsettled only)
			// RSM=second: receiver sends ack and waits for return ack from sender (SSM=unsettled only)

			// mark final transfer as settled when sender mode is settled
			fr.Settled = senderSettled

			// set done on last frame
			fr.done = make(chan deliveryState, 1)
		}

		select {
		case s.link.transfers <- fr:
		case <-s.link.detached:
			return nil, s.link.err
		case <-ctx.Done():
			return nil, errorWrapf(ctx.Err(), "awaiting send")
		}

		// clear values that are only required on first message
		fr.DeliveryID = nil
		fr.DeliveryTag = nil
		fr.MessageFormat = nil
	}

	return fr.done, nil
}

// Address returns the link's address.
func (s *Sender) Address() string {
	if s.link.target == nil {
		return ""
	}
	return s.link.target.Address
}

// Close closes the Sender and AMQP link.
func (s *Sender) Close(ctx context.Context) error {
	return s.link.Close(ctx)
}
