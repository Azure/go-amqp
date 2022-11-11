package amqp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Azure/go-amqp/internal/buffer"
	"github.com/Azure/go-amqp/internal/debug"
	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/Azure/go-amqp/internal/shared"
)

// Sender sends messages on a single AMQP link.
type Sender struct {
	link
	transfers chan frames.PerformTransfer // sender uses to send transfer frames

	// Indicates whether we should allow detaches on disposition errors or not.
	// Some AMQP servers (like Event Hubs) benefit from keeping the link open on disposition errors
	// (for instance, if you're doing many parallel sends over the same link and you get back a
	// throttling error, which is not fatal)
	detachOnDispositionError bool

	mu              sync.Mutex // protects buf and nextDeliveryTag
	buf             buffer.Buffer
	nextDeliveryTag uint64
}

// LinkName() is the name of the link used for this Sender.
func (s *Sender) LinkName() string {
	return s.key.name
}

// MaxMessageSize is the maximum size of a single message.
func (s *Sender) MaxMessageSize() uint64 {
	return s.maxMessageSize
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
	// check if the link is dead.  while it's safe to call s.send
	// in this case, this will avoid some allocations etc.
	select {
	case <-s.detached:
		return s.err
	default:
		// link is still active
	}
	done, err := s.send(ctx, msg)
	if err != nil {
		return err
	}

	// wait for transfer to be confirmed
	select {
	case state := <-done:
		if state, ok := state.(*encoding.StateRejected); ok {
			if s.detachOnRejectDisp() {
				return &DetachError{state.Error}
			}
			return state.Error
		}
		return nil
	case <-s.detached:
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// send is separated from Send so that the mutex unlock can be deferred without
// locking the transfer confirmation that happens in Send.
func (s *Sender) send(ctx context.Context, msg *Message) (chan encoding.DeliveryState, error) {
	const maxDeliveryTagLength = 32
	if len(msg.DeliveryTag) > maxDeliveryTagLength {
		return nil, fmt.Errorf("delivery tag is over the allowed %v bytes, len: %v", maxDeliveryTagLength, len(msg.DeliveryTag))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.buf.Reset()
	err := msg.Marshal(&s.buf)
	if err != nil {
		return nil, err
	}

	if s.maxMessageSize != 0 && uint64(s.buf.Len()) > s.maxMessageSize {
		return nil, fmt.Errorf("encoded message size exceeds max of %d", s.maxMessageSize)
	}

	var (
		maxPayloadSize = int64(s.session.conn.PeerMaxFrameSize) - maxTransferFrameHeader
		sndSettleMode  = s.senderSettleMode
		senderSettled  = sndSettleMode != nil && (*sndSettleMode == ModeSettled || (*sndSettleMode == ModeMixed && msg.SendSettled))
		deliveryID     = atomic.AddUint32(&s.session.nextDeliveryID, 1)
	)

	deliveryTag := msg.DeliveryTag
	if len(deliveryTag) == 0 {
		// use uint64 encoded as []byte as deliveryTag
		deliveryTag = make([]byte, 8)
		binary.BigEndian.PutUint64(deliveryTag, s.nextDeliveryTag)
		s.nextDeliveryTag++
	}

	fr := frames.PerformTransfer{
		Handle:        s.handle,
		DeliveryID:    &deliveryID,
		DeliveryTag:   deliveryTag,
		MessageFormat: &msg.Format,
		More:          s.buf.Len() > 0,
	}

	for fr.More {
		buf, _ := s.buf.Next(maxPayloadSize)
		fr.Payload = append([]byte(nil), buf...)
		fr.More = s.buf.Len() > 0
		if !fr.More {
			// SSM=settled: overrides RSM; no acks.
			// SSM=unsettled: sender should wait for receiver to ack
			// RSM=first: receiver considers it settled immediately, but must still send ack (SSM=unsettled only)
			// RSM=second: receiver sends ack and waits for return ack from sender (SSM=unsettled only)

			// mark final transfer as settled when sender mode is settled
			fr.Settled = senderSettled

			// set done on last frame
			fr.Done = make(chan encoding.DeliveryState, 1)
		}

		select {
		case s.transfers <- fr:
		case <-s.detached:
			return nil, s.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		// clear values that are only required on first message
		fr.DeliveryID = nil
		fr.DeliveryTag = nil
		fr.MessageFormat = nil
	}

	return fr.Done, nil
}

// Address returns the link's address.
func (s *Sender) Address() string {
	if s.target == nil {
		return ""
	}
	return s.target.Address
}

// Close closes the Sender and AMQP link.
func (s *Sender) Close(ctx context.Context) error {
	return s.closeLink(ctx)
}

// newSendingLink creates a new sending link and attaches it to the session
func newSender(target string, s *Session, opts *SenderOptions) (*Sender, error) {
	l := &Sender{
		link: link{
			key:      linkKey{shared.RandString(40), encoding.RoleSender},
			session:  s,
			close:    make(chan struct{}),
			detached: make(chan struct{}),
			target:   &frames.Target{Address: target},
			source:   new(frames.Source),
		},
		detachOnDispositionError: true,
	}

	if opts == nil {
		return l, nil
	}

	for _, v := range opts.Capabilities {
		l.source.Capabilities = append(l.source.Capabilities, encoding.Symbol(v))
	}
	if opts.Durability > DurabilityUnsettledState {
		return nil, fmt.Errorf("invalid Durability %d", opts.Durability)
	}
	l.source.Durable = opts.Durability
	if opts.DynamicAddress {
		l.target.Address = ""
		l.dynamicAddr = opts.DynamicAddress
	}
	if opts.ExpiryPolicy != "" {
		if err := encoding.ValidateExpiryPolicy(opts.ExpiryPolicy); err != nil {
			return nil, err
		}
		l.source.ExpiryPolicy = opts.ExpiryPolicy
	}
	l.source.Timeout = opts.ExpiryTimeout
	l.detachOnDispositionError = !opts.IgnoreDispositionErrors
	if opts.Name != "" {
		l.key.name = opts.Name
	}
	if opts.Properties != nil {
		l.properties = make(map[encoding.Symbol]interface{})
		for k, v := range opts.Properties {
			if k == "" {
				return nil, errors.New("link property key must not be empty")
			}
			l.properties[encoding.Symbol(k)] = v
		}
	}
	if opts.RequestedReceiverSettleMode != nil {
		if rsm := *opts.RequestedReceiverSettleMode; rsm > ModeSecond {
			return nil, fmt.Errorf("invalid RequestedReceiverSettleMode %d", rsm)
		}
		l.receiverSettleMode = opts.RequestedReceiverSettleMode
	}
	if opts.SettlementMode != nil {
		if ssm := *opts.SettlementMode; ssm > ModeMixed {
			return nil, fmt.Errorf("invalid SettlementMode %d", ssm)
		}
		l.senderSettleMode = opts.SettlementMode
	}
	l.source.Address = opts.SourceAddress
	return l, nil
}

func (s *Sender) attach(ctx context.Context, session *Session) error {
	// sending unsettled messages when the receiver is in mode-second is currently
	// broken and causes a hang after sending, so just disallow it for now.
	if senderSettleModeValue(s.senderSettleMode) != ModeSettled && receiverSettleModeValue(s.receiverSettleMode) == ModeSecond {
		return errors.New("sender does not support exactly-once guarantee")
	}

	s.rx = make(chan frames.FrameBody, 1)

	if err := s.attachLink(ctx, session, func(pa *frames.PerformAttach) {
		pa.Role = encoding.RoleSender
		if pa.Target == nil {
			pa.Target = new(frames.Target)
		}
		pa.Target.Dynamic = s.dynamicAddr
	}, func(pa *frames.PerformAttach) {
		if s.target == nil {
			s.target = new(frames.Target)
		}

		// if dynamic address requested, copy assigned name to address
		if s.dynamicAddr && pa.Target != nil {
			s.target.Address = pa.Target.Address
		}
	}); err != nil {
		return err
	}

	s.transfers = make(chan frames.PerformTransfer)

	go s.mux()

	return nil
}

func (s *Sender) mux() {
	defer s.muxDetach(nil, nil)

Loop:
	for {
		var outgoingTransfers chan frames.PerformTransfer
		if s.linkCredit > 0 {
			debug.Log(1, "sender: credit: %d, deliveryCount: %d", s.linkCredit, s.deliveryCount)
			outgoingTransfers = s.transfers
		}

		select {
		// received frame
		case fr := <-s.rx:
			s.err = s.muxHandleFrame(fr)
			if s.err != nil {
				return
			}

		// send data
		case tr := <-outgoingTransfers:
			debug.Log(3, "TX (sender): %s", tr)

			// Ensure the session mux is not blocked
			for {
				select {
				case s.session.txTransfer <- &tr:
					// decrement link-credit after entire message transferred
					if !tr.More {
						s.deliveryCount++
						s.linkCredit--
						// we are the sender and we keep track of the peer's link credit
						debug.Log(3, "TX (sender): key:%s, decremented linkCredit: %d", s.key.name, s.linkCredit)
					}
					continue Loop
				case fr := <-s.rx:
					s.err = s.muxHandleFrame(fr)
					if s.err != nil {
						return
					}
				case <-s.close:
					s.err = ErrLinkClosed
					return
				case <-s.session.done:
					s.err = s.session.err
					return
				}
			}

		case <-s.close:
			s.err = ErrLinkClosed
			return
		case <-s.session.done:
			s.err = s.session.err
			return
		}
	}
}

// muxHandleFrame processes fr based on type.
func (s *Sender) muxHandleFrame(fr frames.FrameBody) error {
	switch fr := fr.(type) {
	// flow control frame
	case *frames.PerformFlow:
		debug.Log(3, "RX (sender): %s", fr)
		linkCredit := *fr.LinkCredit - s.deliveryCount
		if fr.DeliveryCount != nil {
			// DeliveryCount can be nil if the receiver hasn't processed
			// the attach. That shouldn't be the case here, but it's
			// what ActiveMQ does.
			linkCredit += *fr.DeliveryCount
		}
		s.linkCredit = linkCredit

		if !fr.Echo {
			return nil
		}

		var (
			// copy because sent by pointer below; prevent race
			deliveryCount = s.deliveryCount
		)

		// send flow
		// TODO: missing Available and session info
		resp := &frames.PerformFlow{
			Handle:        &s.handle,
			DeliveryCount: &deliveryCount,
			LinkCredit:    &linkCredit, // max number of messages
		}
		debug.Log(1, "TX (sender): %s", resp)
		_ = s.session.txFrame(resp, nil)

	case *frames.PerformDisposition:
		debug.Log(3, "RX (sender): %s", fr)
		// If sending async and a message is rejected, cause a link error.
		//
		// This isn't ideal, but there isn't a clear better way to handle it.
		if fr, ok := fr.State.(*encoding.StateRejected); ok && s.detachOnRejectDisp() {
			return &DetachError{fr.Error}
		}

		if fr.Settled {
			return nil
		}

		resp := &frames.PerformDisposition{
			Role:    encoding.RoleSender,
			First:   fr.First,
			Last:    fr.Last,
			Settled: true,
		}
		debug.Log(1, "TX (sender): %s", resp)
		_ = s.session.txFrame(resp, nil)

	default:
		return s.link.muxHandleFrame(fr)
	}

	return nil
}

func (s *Sender) detachOnRejectDisp() bool {
	// only detach on rejection when no RSM was requested or in ModeFirst.
	// if the receiver is in ModeSecond, it will send an explicit rejection disposition
	// that we'll have to ack. so in that case, we don't treat it as a link error.
	if s.detachOnDispositionError && (s.receiverSettleMode == nil || *s.receiverSettleMode == ModeFirst) {
		return true
	}
	return false
}
