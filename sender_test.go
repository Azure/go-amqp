package amqp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClosedSenderReturnsErrClosed(t *testing.T) {
	// this feels a bit _too_ fake, should revisit.
	link, err := newLink(newSession(nil, 0), &Receiver{}, nil)
	require.NoError(t, err)

	sender := &Sender{link: link}

	// simulate the detach happening before the send. This happens in cases
	// where we get an error back from the AMQP service (for instance, throttling)
	// which calls link.muxDetach() (artifical detach)
	close(link.detached)
	require.NoError(t, err)

	err = sender.Send(context.TODO(), &Message{})
	require.EqualError(t, ErrLinkDetached, err.Error())
}

func TestSenderId(t *testing.T) {
	link, err := newLink(newSession(nil, 0), &Receiver{}, nil)
	require.NoError(t, err)

	sender := &Sender{link: link}
	require.NotEmpty(t, sender.ID())
}
