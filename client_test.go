package amqp

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/Azure/go-amqp/internal/mocks"
	"github.com/stretchr/testify/require"
)

type mockDialer struct {
	resp func(frames.FrameBody) ([]byte, error)
}

func (m mockDialer) NetDialerDial(c *conn, host, port string) error {
	c.net = mocks.NewNetConn(m.resp)
	return nil
}

func (mockDialer) TLSDialWithDialer(c *conn, host, port string) error {
	panic("nyi")
}

func TestClientDial(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	client, err := Dial("amqp://localhost", connDialer(mockDialer{resp: responder}))
	require.NoError(t, err)
	require.NotNil(t, client)
	// error case
	responder = func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return nil, errors.New("mock read failed")
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	client, err = Dial("amqp://localhost", connDialer(mockDialer{resp: responder}))
	require.Error(t, err)
	require.Nil(t, client)
}

func TestClientClose(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	client, err := Dial("amqp://localhost", connDialer(mockDialer{resp: responder}))
	require.NoError(t, err)
	require.NotNil(t, client)
	require.NoError(t, client.Close())
	require.NoError(t, client.Close())
}

func TestSessionOptions(t *testing.T) {
	const (
		// MaxInt added in Go 1.17, this is copied from there
		intSize = 32 << (^uint(0) >> 63) // 32 or 64
		MaxInt  = 1<<(intSize-1) - 1
	)
	tests := []struct {
		label  string
		opt    SessionOption
		verify func(t *testing.T, s *Session)
		fails  bool
	}{
		{
			label: "SessionIncomingWindow",
			opt:   SessionIncomingWindow(5000),
			verify: func(t *testing.T, s *Session) {
				if s.incomingWindow != 5000 {
					t.Errorf("unexpected incoming window %d", s.incomingWindow)
				}
			},
		},
		{
			label: "SessionOutgoingWindow",
			opt:   SessionOutgoingWindow(6000),
			verify: func(t *testing.T, s *Session) {
				if s.outgoingWindow != 6000 {
					t.Errorf("unexpected outgoing window %d", s.outgoingWindow)
				}
			},
		},
		{
			label: "SessionMaxLinksTooSmall",
			opt:   SessionMaxLinks(0),
			fails: true,
		},
		{
			label: "SessionMaxLinksTooLarge",
			opt:   SessionMaxLinks(MaxInt),
			fails: true,
		},
		{
			label: "SessionMaxLinks",
			opt:   SessionMaxLinks(4096),
			verify: func(t *testing.T, s *Session) {
				if s.handleMax != 4096-1 {
					t.Errorf("unexpected max links %d", s.handleMax)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			session := newSession(nil, 0)
			err := tt.opt(session)
			if err != nil && !tt.fails {
				t.Error(err)
			}
			if !tt.fails {
				tt.verify(t, session)
			}
		})
	}
}

func TestClientNewSession(t *testing.T) {
	const channelNum = 0
	const incomingWindow = 5000
	const outgoingWindow = 6000
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch tt := req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformBegin:
			if tt.RemoteChannel != nil {
				return nil, errors.New("expected nil remote channel")
			}
			if tt.IncomingWindow != incomingWindow {
				return nil, fmt.Errorf("unexpected incoming window %d", tt.IncomingWindow)
			}
			if tt.OutgoingWindow != outgoingWindow {
				return nil, fmt.Errorf("unexpected incoming window %d", tt.OutgoingWindow)
			}
			return mocks.PerformBegin(channelNum)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

	client, err := New(netConn)
	require.NoError(t, err)
	session, err := client.NewSession(SessionIncomingWindow(incomingWindow), SessionOutgoingWindow(outgoingWindow))
	require.NoError(t, err)
	require.NotNil(t, session)
	require.Equal(t, uint16(channelNum), session.channel)
	require.NoError(t, client.Close())
	// creating a session after the connection has been closed returns nothing
	session, err = client.NewSession()
	require.Equal(t, ErrConnClosed, err)
	require.Nil(t, session)
}

func TestClientMultipleSessions(t *testing.T) {
	channelNum := uint16(0)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformBegin:
			b, err := mocks.PerformBegin(channelNum)
			channelNum++
			return b, err
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

	client, err := New(netConn)
	require.NoError(t, err)
	// first session
	session1, err := client.NewSession()
	require.NoError(t, err)
	require.NotNil(t, session1)
	require.Equal(t, channelNum-1, session1.channel)
	// second session
	session2, err := client.NewSession()
	require.NoError(t, err)
	require.NotNil(t, session2)
	require.Equal(t, channelNum-1, session2.channel)
	require.NoError(t, client.Close())
}

func TestClientTooManySessions(t *testing.T) {
	channelNum := uint16(0)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			// return small number of max channels
			return mocks.EncodeFrame(mocks.FrameAMQP, 0, &frames.PerformOpen{
				ChannelMax:   1,
				ContainerID:  "test",
				IdleTimeout:  time.Minute,
				MaxFrameSize: 4294967295,
			})
		case *frames.PerformBegin:
			b, err := mocks.PerformBegin(channelNum)
			channelNum++
			return b, err
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

	client, err := New(netConn)
	require.NoError(t, err)
	for i := uint16(0); i < 3; i++ {
		session, err := client.NewSession()
		if i < 2 {
			require.NoError(t, err)
			require.NotNil(t, session)
		} else {
			// third channel should fail
			require.Error(t, err)
			require.Nil(t, session)
		}
	}
	require.NoError(t, client.Close())
}

func TestClientNewSessionInvalidOption(t *testing.T) {
	netConn := mocks.NewNetConn(standardFrameHandlerNoUnhandled)

	client, err := New(netConn)
	require.NoError(t, err)
	session, err := client.NewSession(SessionMaxLinks(0))
	require.Error(t, err)
	require.Nil(t, session)
	require.NoError(t, client.Close())
}

func TestClientNewSessionMissingRemoteChannel(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformBegin:
			// return begin with nil RemoteChannel
			return mocks.EncodeFrame(mocks.FrameAMQP, 0, &frames.PerformBegin{
				NextOutgoingID: 1,
				IncomingWindow: 5000,
				OutgoingWindow: 1000,
				HandleMax:      math.MaxInt16,
			})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

	client, err := New(netConn)
	require.NoError(t, err)
	session, err := client.NewSession(SessionMaxLinks(1))
	require.Error(t, err)
	require.Nil(t, session)
	require.Error(t, client.Close())
}

func TestClientNewSessionInvalidInitialResponse(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformBegin:
			// respond with the wrong frame type
			return mocks.PerformOpen("bad")
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

	client, err := New(netConn)
	require.NoError(t, err)
	session, err := client.NewSession()
	require.Error(t, err)
	require.Nil(t, session)
}

func TestClientNewSessionInvalidSecondResponseSameChannel(t *testing.T) {
	t.Skip("test hangs due to session mux eating unexpected frames")
	firstChan := true
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformBegin:
			if firstChan {
				firstChan = false
				return mocks.PerformBegin(0)
			}
			// respond with the wrong frame type
			return mocks.PerformOpen("bad")
		case *frames.PerformEnd:
			return mocks.PerformEnd(0, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

	client, err := New(netConn)
	require.NoError(t, err)
	// fisrt session succeeds
	session, err := client.NewSession()
	require.NoError(t, err)
	require.NotNil(t, session)
	// second session fails
	session, err = client.NewSession()
	require.Error(t, err)
	require.Nil(t, session)
	require.NoError(t, client.Close())
}

func TestClientNewSessionInvalidSecondResponseDifferentChannel(t *testing.T) {
	firstChan := true
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformBegin:
			if firstChan {
				firstChan = false
				return mocks.PerformBegin(0)
			}
			// respond with the wrong frame type
			// note that it has to be for the next channel
			return mocks.PerformDisposition(encoding.RoleSender, 1, 0, nil, nil)
		case *frames.PerformEnd:
			return mocks.PerformEnd(0, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

	client, err := New(netConn)
	require.NoError(t, err)
	// fisrt session succeeds
	session, err := client.NewSession()
	require.NoError(t, err)
	require.NotNil(t, session)
	// second session fails
	session, err = client.NewSession()
	require.Error(t, err)
	require.Nil(t, session)
	require.Error(t, client.Close())
}
