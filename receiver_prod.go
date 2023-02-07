//go:build !debug
// +build !debug

package amqp

// muxMsg sends the current decoded message to the channel of incoming messages.
// it returns false if a client-side close has been initiated.
func (r *Receiver) muxMsg() bool {
	select {
	case r.messages <- r.msg:
		// message received
		// NOTE: writing to this should NEVER block.
		// if it does, it means we have a flow control
		// bug so our peer sent a message exceeding
		// the link credit
		return true
	case <-r.l.close:
		// client-side close
		return false
	}
}
