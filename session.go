package amqp

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Session is an AMQP session.
//
// A session multiplexes Receivers.
type Session struct {
	channel       uint16                // session's local channel
	remoteChannel uint16                // session's remote channel, owned by conn.mux
	conn          *conn                 // underlying conn
	rx            chan frame            // frames destined for this session are sent on this chan by conn.mux
	tx            chan frameBody        // non-transfer frames to be sent; session must track disposition
	txTransfer    chan *performTransfer // transfer frames to be sent; session must track disposition

	// flow control
	incomingWindow uint32
	outgoingWindow uint32

	handleMax        uint32
	allocateHandle   chan *link // link handles are allocated by sending a link on this channel, nil is sent on link.rx once allocated
	deallocateHandle chan *link // link handles are deallocated by sending a link on this channel

	nextDeliveryID uint32 // atomically accessed sequence for deliveryIDs

	// used for gracefully closing link
	close     chan struct{}
	closeOnce sync.Once
	done      chan struct{}
	err       error
}

func newSession(c *conn, channel uint16) *Session {
	return &Session{
		conn:             c,
		channel:          channel,
		rx:               make(chan frame),
		tx:               make(chan frameBody),
		txTransfer:       make(chan *performTransfer),
		incomingWindow:   DefaultWindow,
		outgoingWindow:   DefaultWindow,
		handleMax:        DefaultMaxLinks - 1,
		allocateHandle:   make(chan *link),
		deallocateHandle: make(chan *link),
		close:            make(chan struct{}),
		done:             make(chan struct{}),
	}
}

// Close gracefully closes the session.
//
// If ctx expires while waiting for servers response, ctx.Err() will be returned.
// The session will continue to wait for the response until the Client is closed.
func (s *Session) Close(ctx context.Context) error {
	s.closeOnce.Do(func() { close(s.close) })
	select {
	case <-s.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if s.err == ErrSessionClosed {
		return nil
	}
	return s.err
}

// txFrame sends a frame to the connWriter
func (s *Session) txFrame(p frameBody, done chan deliveryState) error {
	return s.conn.wantWriteFrame(frame{
		type_:   frameTypeAMQP,
		channel: s.channel,
		body:    p,
		done:    done,
	})
}

// NewReceiver opens a new receiver link on the session.
func (s *Session) NewReceiver(opts ...LinkOption) (*Receiver, error) {
	r := &Receiver{
		batching:    DefaultLinkBatching,
		batchMaxAge: DefaultLinkBatchMaxAge,
		maxCredit:   DefaultLinkCredit,
	}

	l, err := attachLink(s, r, opts)
	if err != nil {
		return nil, err
	}

	r.link = l

	// batching is just extra overhead when maxCredits == 1
	if r.maxCredit == 1 {
		r.batching = false
	}

	// create dispositions channel and start dispositionBatcher if batching enabled
	if r.batching {
		// buffer dispositions chan to prevent disposition sends from blocking
		r.dispositions = make(chan messageDisposition, r.maxCredit)
		go r.dispositionBatcher()
	}

	return r, nil
}

// NewSender opens a new sender link on the session.
func (s *Session) NewSender(opts ...LinkOption) (*Sender, error) {
	l, err := attachLink(s, nil, opts)
	if err != nil {
		return nil, err
	}

	return &Sender{link: l}, nil
}

func (s *Session) mux(remoteBegin *performBegin) {
	defer func() {
		// clean up session record in conn.mux()
		select {
		case s.conn.delSession <- s:
		case <-s.conn.done:
			s.err = s.conn.getErr()
		}
		if s.err == nil {
			s.err = ErrSessionClosed
		}
		// Signal goroutines waiting on the session.
		close(s.done)
	}()

	var (
		links      = make(map[uint32]*link)    // mapping of remote handles to links
		linksByKey = make(map[linkKey]*link)   // mapping of name+role link
		handles    = &bitmap{max: s.handleMax} // allocated handles

		handlesByDeliveryID       = make(map[uint32]uint32) // mapping of deliveryIDs to handles
		deliveryIDByHandle        = make(map[uint32]uint32) // mapping of handles to latest deliveryID
		handlesByRemoteDeliveryID = make(map[uint32]uint32) // mapping of remote deliveryID to handles

		settlementByDeliveryID = make(map[uint32]chan deliveryState)

		// flow control values
		nextOutgoingID       uint32
		nextIncomingID       = remoteBegin.NextOutgoingID
		remoteIncomingWindow = remoteBegin.IncomingWindow
		remoteOutgoingWindow = remoteBegin.OutgoingWindow
	)

	for {
		txTransfer := s.txTransfer
		// disable txTransfer if flow control windows have been exceeded
		if remoteIncomingWindow == 0 || s.outgoingWindow == 0 {
			debug(1, "TX(Session): Disabling txTransfer - window exceeded. remoteIncomingWindow:",
				remoteIncomingWindow,
				"outgoingWindow:",
				s.outgoingWindow)
			txTransfer = nil
		}

		select {
		// conn has completed, exit
		case <-s.conn.done:
			s.err = s.conn.getErr()
			return

		// session is being closed by user
		case <-s.close:
			s.txFrame(&performEnd{}, nil)

			// discard frames until End is received or conn closed
		EndLoop:
			for {
				select {
				case fr := <-s.rx:
					_, ok := fr.body.(*performEnd)
					if ok {
						break EndLoop
					}
				case <-s.conn.done:
					s.err = s.conn.getErr()
					return
				}
			}
			return

		// handle allocation request
		case l := <-s.allocateHandle:
			// Check if link name already exists, if so then an error should be returned
			if linksByKey[l.key] != nil {
				l.err = errorErrorf("link with name '%v' already exists", l.key.name)
				l.rx <- nil
				continue
			}

			next, ok := handles.next()
			if !ok {
				l.err = errorErrorf("reached session handle max (%d)", s.handleMax)
				l.rx <- nil
				continue
			}

			l.handle = next       // allocate handle to the link
			linksByKey[l.key] = l // add to mapping
			l.rx <- nil           // send nil on channel to indicate allocation complete

		// handle deallocation request
		case l := <-s.deallocateHandle:
			delete(links, l.remoteHandle)
			delete(deliveryIDByHandle, l.handle)
			delete(linksByKey, l.key)
			handles.remove(l.handle)
			close(l.rx) // close channel to indicate deallocation

		// incoming frame for link
		case fr := <-s.rx:
			debug(1, "RX(Session): %s", fr.body)

			switch body := fr.body.(type) {
			// Disposition frames can reference transfers from more than one
			// link. Send this frame to all of them.
			case *performDisposition:
				start := body.First
				end := start
				if body.Last != nil {
					end = *body.Last
				}
				for deliveryID := start; deliveryID <= end; deliveryID++ {
					handles := handlesByDeliveryID
					if body.Role == roleSender {
						handles = handlesByRemoteDeliveryID
					}

					handle, ok := handles[deliveryID]
					if !ok {
						continue
					}
					delete(handles, deliveryID)

					if body.Settled && body.Role == roleReceiver {
						// check if settlement confirmation was requested, if so
						// confirm by closing channel
						if done, ok := settlementByDeliveryID[deliveryID]; ok {
							delete(settlementByDeliveryID, deliveryID)
							select {
							case done <- body.State:
							default:
							}
							close(done)
						}
					}

					link, ok := links[handle]
					if !ok {
						continue
					}

					s.muxFrameToLink(link, fr.body)
				}
				continue
			case *performFlow:
				if body.NextIncomingID == nil {
					// This is a protocol error:
					//       "[...] MUST be set if the peer has received
					//        the begin frame for the session"
					s.txFrame(&performEnd{
						Error: &Error{
							Condition:   ErrorNotAllowed,
							Description: "next-incoming-id not set after session established",
						},
					}, nil)
					s.err = errors.New("protocol error: received flow without next-incoming-id after session established")
					return
				}

				// "When the endpoint receives a flow frame from its peer,
				// it MUST update the next-incoming-id directly from the
				// next-outgoing-id of the frame, and it MUST update the
				// remote-outgoing-window directly from the outgoing-window
				// of the frame."
				nextIncomingID = body.NextOutgoingID
				remoteOutgoingWindow = body.OutgoingWindow

				// "The remote-incoming-window is computed as follows:
				//
				// next-incoming-id(flow) + incoming-window(flow) - next-outgoing-id(endpoint)
				//
				// If the next-incoming-id field of the flow frame is not set, then remote-incoming-window is computed as follows:
				//
				// initial-outgoing-id(endpoint) + incoming-window(flow) - next-outgoing-id(endpoint)"
				remoteIncomingWindow = body.IncomingWindow - nextOutgoingID
				remoteIncomingWindow += *body.NextIncomingID
				debug(3, "RX(Session) Flow - remoteOutgoingWindow: %d remoteIncomingWindow: %d nextOutgoingID: %d", remoteOutgoingWindow, remoteIncomingWindow, nextOutgoingID)

				// Send to link if handle is set
				if body.Handle != nil {
					link, ok := links[*body.Handle]
					if !ok {
						continue
					}

					s.muxFrameToLink(link, fr.body)
					continue
				}

				if body.Echo {
					niID := nextIncomingID
					resp := &performFlow{
						NextIncomingID: &niID,
						IncomingWindow: s.incomingWindow,
						NextOutgoingID: nextOutgoingID,
						OutgoingWindow: s.outgoingWindow,
					}
					debug(1, "TX: %s", resp)
					s.txFrame(resp, nil)
				}

			case *performAttach:
				// On Attach response link should be looked up by name, then added
				// to the links map with the remote's handle contained in this
				// attach frame.
				//
				// Note body.Role is the remote peer's role, we reverse for the local key.
				link, linkOk := linksByKey[linkKey{name: body.Name, role: !body.Role}]
				if !linkOk {
					break
				}

				link.remoteHandle = body.Handle
				links[link.remoteHandle] = link

				s.muxFrameToLink(link, fr.body)

			case *performTransfer:
				// "Upon receiving a transfer, the receiving endpoint will
				// increment the next-incoming-id to match the implicit
				// transfer-id of the incoming transfer plus one, as well
				// as decrementing the remote-outgoing-window, and MAY
				// (depending on policy) decrement its incoming-window."
				nextIncomingID++
				// don't loop to intmax
				if remoteOutgoingWindow > 0 {
					remoteOutgoingWindow--
				}
				link, ok := links[body.Handle]
				if !ok {
					continue
				}

				select {
				case <-s.conn.done:
				case link.rx <- fr.body:
				}

				// if this message is received unsettled and link rcv-settle-mode == second, add to handlesByRemoteDeliveryID
				if !body.Settled && body.DeliveryID != nil && link.receiverSettleMode != nil && *link.receiverSettleMode == ModeSecond {
					debug(1, "TX(Session): adding handle to handlesByRemoteDeliveryID. linkCredit: %d", link.linkCredit)
					handlesByRemoteDeliveryID[*body.DeliveryID] = body.Handle
				}

				debug(3, "TX(Session) Flow? remoteOutgoingWindow(%d) < s.incomingWindow(%d)/2\n", remoteOutgoingWindow, s.incomingWindow)
				// Update peer's outgoing window if half has been consumed.
				if remoteOutgoingWindow < s.incomingWindow/2 {
					nID := nextIncomingID
					flow := &performFlow{
						NextIncomingID: &nID,
						IncomingWindow: s.incomingWindow,
						NextOutgoingID: nextOutgoingID,
						OutgoingWindow: s.outgoingWindow,
					}
					debug(1, "TX(Session): %s", flow)
					s.txFrame(flow, nil)
				}

			case *performDetach:
				link, ok := links[body.Handle]
				if !ok {
					continue
				}
				s.muxFrameToLink(link, fr.body)

			case *performEnd:
				s.txFrame(&performEnd{}, nil)
				s.err = errorErrorf("session ended by server: %s", body.Error)
				return

			default:
				fmt.Printf("Unexpected frame: %s\n", body)
			}

		case fr := <-txTransfer:

			// record current delivery ID
			var deliveryID uint32
			if fr.DeliveryID != nil {
				deliveryID = *fr.DeliveryID
				deliveryIDByHandle[fr.Handle] = deliveryID

				// add to handleByDeliveryID if not sender-settled
				if !fr.Settled {
					handlesByDeliveryID[deliveryID] = fr.Handle
				}
			} else {
				// if fr.DeliveryID is nil it must have been added
				// to deliveryIDByHandle already
				deliveryID = deliveryIDByHandle[fr.Handle]
			}

			// frame has been sender-settled, remove from map
			if fr.Settled {
				delete(handlesByDeliveryID, deliveryID)
			}

			// if not settled, add done chan to map
			// and clear from frame so conn doesn't close it.
			if !fr.Settled && fr.done != nil {
				settlementByDeliveryID[deliveryID] = fr.done
				fr.done = nil
			}

			debug(2, "TX(Session) - txtransfer: %s", fr)
			s.txFrame(fr, fr.done)

			// "Upon sending a transfer, the sending endpoint will increment
			// its next-outgoing-id, decrement its remote-incoming-window,
			// and MAY (depending on policy) decrement its outgoing-window."
			nextOutgoingID++
			// don't decrement if we're at 0 or we could loop to int max
			if remoteIncomingWindow != 0 {
				remoteIncomingWindow--
			}

		case fr := <-s.tx:
			switch fr := fr.(type) {
			case *performFlow:
				niID := nextIncomingID
				fr.NextIncomingID = &niID
				fr.IncomingWindow = s.incomingWindow
				fr.NextOutgoingID = nextOutgoingID
				fr.OutgoingWindow = s.outgoingWindow
				debug(1, "TX(Session) - tx: %s", fr)
				s.txFrame(fr, nil)
			case *performTransfer:
				panic("transfer frames must use txTransfer")
			default:
				debug(1, "TX(Session) - default: %s", fr)
				s.txFrame(fr, nil)
			}
		}
	}
}

func (s *Session) muxFrameToLink(l *link, fr frameBody) {
	select {
	case l.rx <- fr:
	case <-l.detached:
	case <-s.conn.done:
	}
}
