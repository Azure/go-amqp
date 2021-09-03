package amqp

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLinkDrain(t *testing.T) {
	l := &link{
		receiver: &Receiver{},
	}

	// without manual credit management...
	err := l.drain(context.Background())
	require.Error(t, err, "drain can only be used with links using manual credit management")

	fn := LinkWithManualCredits()
	fn(l)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		require.NoError(t, l.drain(context.Background()))
	}()

	time.Sleep(time.Second * 2)
	l.receiver.manualCreditor.EndDrain()

	wg.Wait()
}

func TestLinkFlowWithManualCreditor(t *testing.T) {
	sessionTx := make(chan frameBody, 100)

	l := &link{
		receiver: &Receiver{
			manualCreditor: &manualCreditor{},
		},
		session: &Session{
			tx:   sessionTx,
			done: make(chan struct{}),
		},
		closeOnce: sync.Once{},

		close:         make(chan struct{}),
		detached:      make(chan struct{}),
		detachErrorMu: sync.Mutex{},
	}

	l.linkCredit = 1
	require.NoError(t, l.addCredit(100))

	go func() {
		l.mux()
	}()

	time.Sleep(time.Second * 2)

	// flow happens immmediately in 'mux'
	txFrame := <-sessionTx

	switch frame := txFrame.(type) {
	case *performFlow:
		require.False(t, frame.Drain)
		require.EqualValues(t, 100+1, *frame.LinkCredit)
	default:
		require.Fail(t, fmt.Sprintf("Unexpected frame was transferred: %+v", txFrame))
	}
}
