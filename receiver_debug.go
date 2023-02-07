//go:build debug
// +build debug

package amqp

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
	default:
		panic("writing to r.messages should not block")
	}
}
