package amqp

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// link is a unidirectional route.
//
// May be used for sending or receiving.
type link struct {
	key          linkKey              // Name and direction
	handle       uint32               // our handle
	remoteHandle uint32               // remote's handle
	dynamicAddr  bool                 // request a dynamic link address from the server
	rx           chan frameBody       // sessions sends frames for this link on this channel
	transfers    chan performTransfer // sender uses to send transfer frames
	closeOnce    sync.Once            // closeOnce protects close from being closed multiple times

	// NOTE: `close` and `detached` BOTH need to be checked to determine if the link
	// is not in a "closed" state

	// close signals the mux to shutdown. This indicates that `Close()` was called on this link.
	close chan struct{}
	// detached is closed by mux/muxDetach when the link is fully detached.
	// This will be initiated if the service sends back an error or requests the link detach.
	detached chan struct{}

	detachErrorMu sync.Mutex // protects detachError
	detachError   *Error     // error to send to remote on detach, set by closeWithError
	session       *Session   // parent session
	receiver      *Receiver  // allows link options to modify Receiver
	source        *source
	target        *target
	properties    map[symbol]interface{} // additional properties sent upon link attach
	// Indicates whether we should allow detaches on disposition errors or not.
	// Some AMQP servers (like Event Hubs) benefit from keeping the link open on disposition errors
	// (for instance, if you're doing many parallel sends over the same link and you get back a
	// throttling error, which is not fatal)
	detachOnDispositionError bool

	// "The delivery-count is initialized by the sender when a link endpoint is created,
	// and is incremented whenever a message is sent. Only the sender MAY independently
	// modify this field. The receiver's value is calculated based on the last known
	// value from the sender and any subsequent messages received on the link. Note that,
	// despite its name, the delivery-count is not a count but a sequence number
	// initialized at an arbitrary point by the sender."
	deliveryCount      uint32
	linkCredit         uint32 // maximum number of messages allowed between flow updates
	senderSettleMode   *SenderSettleMode
	receiverSettleMode *ReceiverSettleMode
	maxMessageSize     uint64
	detachReceived     bool
	err                error // err returned on Close()

	// message receiving
	paused                uint32              // atomically accessed; indicates that all link credits have been used by sender
	receiverReady         chan struct{}       // receiver sends on this when mux is paused to indicate it can handle more messages
	messages              chan Message        // used to send completed messages to receiver
	unsettledMessages     map[string]struct{} // used to keep track of messages being handled downstream
	unsettledMessagesLock sync.RWMutex        // lock to protect concurrent access to unsettledMessages
	buf                   buffer              // buffered bytes for current message
	more                  bool                // if true, buf contains a partial message
	msg                   Message             // current message being decoded
}

func newLink(s *Session, r *Receiver, opts []LinkOption) (*link, error) {
	l := &link{
		key:                      linkKey{randString(40), role(r != nil)},
		session:                  s,
		receiver:                 r,
		close:                    make(chan struct{}),
		detached:                 make(chan struct{}),
		receiverReady:            make(chan struct{}, 1),
		detachOnDispositionError: true,
	}

	// configure options
	for _, o := range opts {
		err := o(l)
		if err != nil {
			return nil, err
		}
	}

	return l, nil
}

// attachLink is used by Receiver and Sender to create new links
func attachLink(s *Session, r *Receiver, opts []LinkOption) (*link, error) {
	l, err := newLink(s, r, opts)
	if err != nil {
		return nil, err
	}

	isReceiver := r != nil

	// buffer rx to linkCredit so that conn.mux won't block
	// attempting to send to a slow reader
	if isReceiver {
		l.rx = make(chan frameBody, l.linkCredit)
	} else {
		l.rx = make(chan frameBody, 1)
	}

	// request handle from Session.mux
	select {
	case <-s.done:
		return nil, s.err
	case s.allocateHandle <- l:
	}

	// wait for handle allocation
	select {
	case <-s.done:
		return nil, s.err
	case <-l.rx:
	}

	// check for link request error
	if l.err != nil {
		return nil, l.err
	}

	attach := &performAttach{
		Name:               l.key.name,
		Handle:             l.handle,
		ReceiverSettleMode: l.receiverSettleMode,
		SenderSettleMode:   l.senderSettleMode,
		MaxMessageSize:     l.maxMessageSize,
		Source:             l.source,
		Target:             l.target,
		Properties:         l.properties,
	}

	if isReceiver {
		attach.Role = roleReceiver
		if attach.Source == nil {
			attach.Source = new(source)
		}
		attach.Source.Dynamic = l.dynamicAddr
	} else {
		attach.Role = roleSender
		if attach.Target == nil {
			attach.Target = new(target)
		}
		attach.Target.Dynamic = l.dynamicAddr
	}

	// send Attach frame
	debug(1, "TX: %s", attach)
	s.txFrame(attach, nil)

	// wait for response
	var fr frameBody
	select {
	case <-s.done:
		return nil, s.err
	case fr = <-l.rx:
	}
	debug(3, "RX: %s", fr)
	resp, ok := fr.(*performAttach)
	if !ok {
		return nil, errorErrorf("unexpected attach response: %#v", fr)
	}

	// If the remote encounters an error during the attach it returns an Attach
	// with no Source or Target. The remote then sends a Detach with an error.
	//
	//   Note that if the application chooses not to create a terminus, the session
	//   endpoint will still create a link endpoint and issue an attach indicating
	//   that the link endpoint has no associated local terminus. In this case, the
	//   session endpoint MUST immediately detach the newly created link endpoint.
	//
	// http://docs.oasis-open.org/amqp/core/v1.0/csprd01/amqp-core-transport-v1.0-csprd01.html#doc-idp386144
	if resp.Source == nil && resp.Target == nil {
		// wait for detach
		select {
		case <-s.done:
			return nil, s.err
		case fr = <-l.rx:
		}

		detach, ok := fr.(*performDetach)
		if !ok {
			return nil, errorErrorf("unexpected frame while waiting for detach: %#v", fr)
		}

		// send return detach
		fr = &performDetach{
			Handle: l.handle,
			Closed: true,
		}
		debug(1, "TX: %s", fr)
		s.txFrame(fr, nil)

		if detach.Error == nil {
			return nil, errorErrorf("received detach with no error specified")
		}
		return nil, detach.Error
	}

	if l.maxMessageSize == 0 || resp.MaxMessageSize < l.maxMessageSize {
		l.maxMessageSize = resp.MaxMessageSize
	}

	if isReceiver {
		// if dynamic address requested, copy assigned name to address
		if l.dynamicAddr && resp.Source != nil {
			l.source.Address = resp.Source.Address
		}
		// deliveryCount is a sequence number, must initialize to sender's initial sequence number
		l.deliveryCount = resp.InitialDeliveryCount
		// buffer receiver so that link.mux doesn't block
		l.messages = make(chan Message, l.receiver.maxCredit)
		l.unsettledMessages = map[string]struct{}{}
		// copy the received filter values
		l.source.Filter = resp.Source.Filter
	} else {
		// if dynamic address requested, copy assigned name to address
		if l.dynamicAddr && resp.Target != nil {
			l.target.Address = resp.Target.Address
		}
		l.transfers = make(chan performTransfer)
	}

	err = l.setSettleModes(resp)
	if err != nil {
		l.muxDetach()
		return nil, err
	}

	go l.mux()

	return l, nil
}

func (l *link) addUnsettled(msg *Message) {
	l.unsettledMessagesLock.Lock()
	l.unsettledMessages[string(msg.DeliveryTag)] = struct{}{}
	l.unsettledMessagesLock.Unlock()
}

func (l *link) deleteUnsettled(msg *Message) {
	l.unsettledMessagesLock.Lock()
	delete(l.unsettledMessages, string(msg.DeliveryTag))
	l.unsettledMessagesLock.Unlock()
}

func (l *link) countUnsettled() int {
	l.unsettledMessagesLock.RLock()
	count := len(l.unsettledMessages)
	l.unsettledMessagesLock.RUnlock()
	return count
}

// setSettleModes sets the settlement modes based on the resp performAttach.
//
// If a settlement mode has been explicitly set locally and it was not honored by the
// server an error is returned.
func (l *link) setSettleModes(resp *performAttach) error {
	var (
		localRecvSettle = l.receiverSettleMode.value()
		respRecvSettle  = resp.ReceiverSettleMode.value()
	)
	if l.receiverSettleMode != nil && localRecvSettle != respRecvSettle {
		return fmt.Errorf("amqp: receiver settlement mode %q requested, received %q from server", l.receiverSettleMode, &respRecvSettle)
	}
	l.receiverSettleMode = &respRecvSettle

	var (
		localSendSettle = l.senderSettleMode.value()
		respSendSettle  = resp.SenderSettleMode.value()
	)
	if l.senderSettleMode != nil && localSendSettle != respSendSettle {
		return fmt.Errorf("amqp: sender settlement mode %q requested, received %q from server", l.senderSettleMode, &respSendSettle)
	}
	l.senderSettleMode = &respSendSettle

	return nil
}

func (l *link) mux() {
	defer l.muxDetach()

	var (
		isReceiver = l.receiver != nil
		isSender   = !isReceiver
	)

Loop:
	for {
		var outgoingTransfers chan performTransfer
		switch {
		// enable outgoing transfers case if sender and credits are available
		case isSender && l.linkCredit > 0:
			debug(1, "Link Mux isSender: credit: %d, deliveryCount: %d, messages: %d, unsettled: %d", l.linkCredit, l.deliveryCount, len(l.messages), l.countUnsettled())
			outgoingTransfers = l.transfers

		// if receiver && half maxCredits have been processed, send more credits
		case isReceiver && l.linkCredit+uint32(l.countUnsettled()) <= l.receiver.maxCredit/2:
			debug(1, "FLOW Link Mux half: source: %s, inflight: %d, credit: %d, deliveryCount: %d, messages: %d, unsettled: %d, maxCredit : %d, settleMode: %s", l.source.Address, len(l.receiver.inFlight.m), l.linkCredit, l.deliveryCount, len(l.messages), l.countUnsettled(), l.receiver.maxCredit, l.receiverSettleMode.String())
			l.err = l.muxFlow()
			if l.err != nil {
				return
			}
			atomic.StoreUint32(&l.paused, 0)

		case isReceiver && l.linkCredit == 0:
			debug(1, "PAUSE Link Mux pause: inflight: %d, credit: %d, deliveryCount: %d, messages: %d, unsettled: %d, maxCredit : %d, settleMode: %s", len(l.receiver.inFlight.m), l.linkCredit, l.deliveryCount, len(l.messages), l.countUnsettled(), l.receiver.maxCredit, l.receiverSettleMode.String())
			atomic.StoreUint32(&l.paused, 1)
		}

		select {
		// received frame
		case fr := <-l.rx:
			l.err = l.muxHandleFrame(fr)
			if l.err != nil {
				return
			}

		// send data
		case tr := <-outgoingTransfers:
			debug(3, "TX(link): %s", tr)

			// Ensure the session mux is not blocked
			for {
				select {
				case l.session.txTransfer <- &tr:
					// decrement link-credit after entire message transferred
					if !tr.More {
						l.deliveryCount++
						l.linkCredit--
						// we are the sender and we keep track of the peer's link credit
						debug(3, "TX(link): key:%s, decremented linkCredit: %d", l.key.name, l.linkCredit)
					}
					continue Loop
				case fr := <-l.rx:
					l.err = l.muxHandleFrame(fr)
					if l.err != nil {
						return
					}
				case <-l.close:
					l.err = ErrLinkClosed
					return
				case <-l.session.done:
					l.err = l.session.err
					return
				}
			}

		case <-l.receiverReady:
			continue
		case <-l.close:
			l.err = ErrLinkClosed
			return
		case <-l.session.done:
			l.err = l.session.err
			return
		}
	}
}

// muxFlow sends tr to the session mux.
func (l *link) muxFlow() error {
	// copy because sent by pointer below; prevent race
	var (
		linkCredit    = l.receiver.maxCredit - uint32(l.countUnsettled())
		deliveryCount = l.deliveryCount
	)

	debug(3, "link.muxFlow(): len(l.messages):%d - linkCredit: %d - deliveryCount: %d, inFlight: %d", len(l.messages), l.linkCredit, deliveryCount, len(l.receiver.inFlight.m))

	fr := &performFlow{
		Handle:        &l.handle,
		DeliveryCount: &deliveryCount,
		LinkCredit:    &linkCredit, // max number of messages
	}
	debug(3, "TX: %s", fr)

	// Update credit. This must happen before entering loop below
	// because incoming messages handled while waiting to transmit
	// flow increment deliveryCount. This causes the credit to become
	// out of sync with the server.
	l.linkCredit = linkCredit

	// Ensure the session mux is not blocked
	for {
		select {
		case l.session.tx <- fr:
			return nil
		case fr := <-l.rx:
			err := l.muxHandleFrame(fr)
			if err != nil {
				return err
			}
		case <-l.close:
			return ErrLinkClosed
		case <-l.session.done:
			return l.session.err
		}
	}
}

func (l *link) muxReceive(fr performTransfer) error {
	if !l.more {
		// this is the first transfer of a message,
		// record the delivery ID, message format,
		// and delivery Tag
		if fr.DeliveryID != nil {
			l.msg.deliveryID = *fr.DeliveryID
		}
		if fr.MessageFormat != nil {
			l.msg.Format = *fr.MessageFormat
		}
		l.msg.DeliveryTag = fr.DeliveryTag

		// these fields are required on first transfer of a message
		if fr.DeliveryID == nil {
			msg := "received message without a delivery-id"
			l.closeWithError(&Error{
				Condition:   ErrorNotAllowed,
				Description: msg,
			})
			return errorNew(msg)
		}
		if fr.MessageFormat == nil {
			msg := "received message without a message-format"
			l.closeWithError(&Error{
				Condition:   ErrorNotAllowed,
				Description: msg,
			})
			return errorNew(msg)
		}
		if fr.DeliveryTag == nil {
			msg := "received message without a delivery-tag"
			l.closeWithError(&Error{
				Condition:   ErrorNotAllowed,
				Description: msg,
			})
			return errorNew(msg)
		}
	} else {
		// this is a continuation of a multipart message
		// some fields may be omitted on continuation transfers,
		// but if they are included they must be consistent
		// with the first.

		if fr.DeliveryID != nil && *fr.DeliveryID != l.msg.deliveryID {
			msg := fmt.Sprintf(
				"received continuation transfer with inconsistent delivery-id: %d != %d",
				*fr.DeliveryID, l.msg.deliveryID,
			)
			l.closeWithError(&Error{
				Condition:   ErrorNotAllowed,
				Description: msg,
			})
			return errorNew(msg)
		}
		if fr.MessageFormat != nil && *fr.MessageFormat != l.msg.Format {
			msg := fmt.Sprintf(
				"received continuation transfer with inconsistent message-format: %d != %d",
				*fr.MessageFormat, l.msg.Format,
			)
			l.closeWithError(&Error{
				Condition:   ErrorNotAllowed,
				Description: msg,
			})
			return errorNew(msg)
		}
		if fr.DeliveryTag != nil && !bytes.Equal(fr.DeliveryTag, l.msg.DeliveryTag) {
			msg := fmt.Sprintf(
				"received continuation transfer with inconsistent delivery-tag: %q != %q",
				fr.DeliveryTag, l.msg.DeliveryTag,
			)
			l.closeWithError(&Error{
				Condition:   ErrorNotAllowed,
				Description: msg,
			})
			return errorNew(msg)
		}
	}

	// discard message if it's been aborted
	if fr.Aborted {
		l.buf.reset()
		l.msg = Message{
			doneSignal: make(chan struct{}),
		}
		l.more = false
		return nil
	}

	// ensure maxMessageSize will not be exceeded
	if l.maxMessageSize != 0 && uint64(l.buf.len())+uint64(len(fr.Payload)) > l.maxMessageSize {
		msg := fmt.Sprintf("received message larger than max size of %d", l.maxMessageSize)
		l.closeWithError(&Error{
			Condition:   ErrorMessageSizeExceeded,
			Description: msg,
		})
		return errorNew(msg)
	}

	// add the payload the the buffer
	l.buf.write(fr.Payload)

	// mark as settled if at least one frame is settled
	l.msg.settled = l.msg.settled || fr.Settled

	// save in-progress status
	l.more = fr.More

	if fr.More {
		return nil
	}

	// last frame in message
	err := l.msg.unmarshal(&l.buf)
	if err != nil {
		return err
	}
	debug(1, "deliveryID %d before push to receiver - deliveryCount : %d - linkCredit: %d, len(messages): %d, len(inflight): %d", l.msg.deliveryID, l.deliveryCount, l.linkCredit, len(l.messages), len(l.receiver.inFlight.m))
	// send to receiver, this should never block due to buffering
	// and flow control.
	if l.receiverSettleMode.value() == ModeSecond {
		l.addUnsettled(&l.msg)
	}
	l.messages <- l.msg

	debug(1, "deliveryID %d after push to receiver - deliveryCount : %d - linkCredit: %d, len(messages): %d, len(inflight): %d", l.msg.deliveryID, l.deliveryCount, l.linkCredit, len(l.messages), len(l.receiver.inFlight.m))

	// reset progress
	l.buf.reset()
	l.msg = Message{}

	// decrement link-credit after entire message received
	l.deliveryCount++
	l.linkCredit--
	debug(1, "deliveryID %d before exit - deliveryCount : %d - linkCredit: %d, len(messages): %d", l.msg.deliveryID, l.deliveryCount, l.linkCredit, len(l.messages))
	return nil
}

// muxHandleFrame processes fr based on type.
func (l *link) muxHandleFrame(fr frameBody) error {
	var (
		isSender               = l.receiver == nil
		errOnRejectDisposition = l.detachOnDispositionError && (isSender && (l.receiverSettleMode == nil || *l.receiverSettleMode == ModeFirst))
	)

	switch fr := fr.(type) {
	// message frame
	case *performTransfer:
		debug(3, "RX: %s", fr)
		if isSender {
			// Senders should never receive transfer frames, but handle it just in case.
			l.closeWithError(&Error{
				Condition:   ErrorNotAllowed,
				Description: "sender cannot process transfer frame",
			})
			return errorErrorf("sender received transfer frame")
		}

		return l.muxReceive(*fr)

	// flow control frame
	case *performFlow:
		debug(3, "RX: %s", fr)
		if isSender {
			linkCredit := *fr.LinkCredit - l.deliveryCount
			if fr.DeliveryCount != nil {
				// DeliveryCount can be nil if the receiver hasn't processed
				// the attach. That shouldn't be the case here, but it's
				// what ActiveMQ does.
				linkCredit += *fr.DeliveryCount
			}
			l.linkCredit = linkCredit
		}

		if !fr.Echo {
			return nil
		}

		var (
			// copy because sent by pointer below; prevent race
			linkCredit    = l.linkCredit
			deliveryCount = l.deliveryCount
		)

		// send flow
		resp := &performFlow{
			Handle:        &l.handle,
			DeliveryCount: &deliveryCount,
			LinkCredit:    &linkCredit, // max number of messages
		}
		debug(1, "TX: %s", resp)
		l.session.txFrame(resp, nil)

	// remote side is closing links
	case *performDetach:
		debug(1, "RX: %s", fr)
		// don't currently support link detach and reattach
		if !fr.Closed {
			return errorErrorf("non-closing detach not supported: %+v", fr)
		}

		// set detach received and close link
		l.detachReceived = true

		return errorWrapf(&DetachError{fr.Error}, "received detach frame")

	case *performDisposition:
		debug(3, "RX: %s", fr)

		// Unblock receivers waiting for message disposition
		if l.receiver != nil {
			// bubble disposition error up to the receiver
			var dispositionError error
			if state, ok := fr.State.(*stateRejected); ok {
				dispositionError = state.Error
			}
			l.receiver.inFlight.remove(fr.First, fr.Last, dispositionError)
		}

		// If sending async and a message is rejected, cause a link error.
		//
		// This isn't ideal, but there isn't a clear better way to handle it.
		if fr, ok := fr.State.(*stateRejected); ok && errOnRejectDisposition {
			return fr.Error
		}

		if fr.Settled {
			return nil
		}

		resp := &performDisposition{
			Role:    roleSender,
			First:   fr.First,
			Last:    fr.Last,
			Settled: true,
		}
		debug(1, "TX: %s", resp)
		l.session.txFrame(resp, nil)

	default:
		debug(1, "RX: %s", fr)
		fmt.Printf("Unexpected frame: %s\n", fr)
	}

	return nil
}

// Check checks the link state, returning an error if the link is closed (ErrLinkClosed) or if
// it is in a detached state (ErrLinkDetached)
func (l *link) Check() error {
	select {
	case <-l.detached:
		return ErrLinkDetached
	case <-l.close:
		return ErrLinkClosed
	default:
		return nil
	}
}

// close closes and requests deletion of the link.
//
// No operations on link are valid after close.
//
// If ctx expires while waiting for servers response, ctx.Err() will be returned.
// The session will continue to wait for the response until the Session or Client
// is closed.
func (l *link) Close(ctx context.Context) error {
	l.closeOnce.Do(func() { close(l.close) })
	select {
	case <-l.detached:
	case <-ctx.Done():
		return ctx.Err()
	}
	if l.err == ErrLinkClosed {
		return nil
	}
	return l.err
}

func (l *link) closeWithError(de *Error) {
	l.closeOnce.Do(func() {
		l.detachErrorMu.Lock()
		l.detachError = de
		l.detachErrorMu.Unlock()
		close(l.close)
	})
}

func (l *link) muxDetach() {
	defer func() {
		// final cleanup and signaling

		// deallocate handle
		select {
		case l.session.deallocateHandle <- l:
		case <-l.session.done:
			if l.err == nil {
				l.err = l.session.err
			}
		}

		// signal other goroutines that link is detached
		close(l.detached)

		// unblock any in flight message dispositions
		if l.receiver != nil {
			l.receiver.inFlight.clear(l.err)
		}
	}()

	// "A peer closes a link by sending the detach frame with the
	// handle for the specified link, and the closed flag set to
	// true. The partner will destroy the corresponding link
	// endpoint, and reply with its own detach frame with the
	// closed flag set to true.
	//
	// Note that one peer MAY send a closing detach while its
	// partner is sending a non-closing detach. In this case,
	// the partner MUST signal that it has closed the link by
	// reattaching and then sending a closing detach."

	l.detachErrorMu.Lock()
	detachError := l.detachError
	l.detachErrorMu.Unlock()

	fr := &performDetach{
		Handle: l.handle,
		Closed: true,
		Error:  detachError,
	}

Loop:
	for {
		select {
		case l.session.tx <- fr:
			// after sending the detach frame, break the read loop
			break Loop
		case fr := <-l.rx:
			// discard incoming frames to avoid blocking session.mux
			if fr, ok := fr.(*performDetach); ok && fr.Closed {
				l.detachReceived = true
			}
		case <-l.session.done:
			if l.err == nil {
				l.err = l.session.err
			}
			return
		}
	}

	// don't wait for remote to detach when already
	// received or closing due to error
	if l.detachReceived || detachError != nil {
		return
	}

	for {
		select {
		// read from link until detach with Close == true is received,
		// other frames are discarded.
		case fr := <-l.rx:
			if fr, ok := fr.(*performDetach); ok && fr.Closed {
				return
			}

		// connection has ended
		case <-l.session.done:
			if l.err == nil {
				l.err = l.session.err
			}
			return
		}
	}
}
