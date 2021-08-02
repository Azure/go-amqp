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

	// simulate a detach happening before the send()
	// link.muxDetach()
	close(link.done)

	err = sender.Send(context.TODO(), &Message{})
	require.EqualError(t, ErrLinkClosed, err.Error())
}

func TestSenderId(t *testing.T) {
	link, err := newLink(newSession(nil, 0), &Receiver{}, nil)
	require.NoError(t, err)

	sender := &Sender{link: link}
	require.NotEmpty(t, sender.ID())
}
