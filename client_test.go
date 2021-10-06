package amqp

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/frames"
	"github.com/Azure/go-amqp/internal/mocks"
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
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatal("unexpected nil client")
	}
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
	if err == nil {
		t.Fatal("unexpected nil error")
	}
	if client != nil {
		t.Fatal("unexpected nil client")
	}
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
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatal("unexpected nil client")
	}
	time.Sleep(100 * time.Millisecond)
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSessionOptions(t *testing.T) {
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
			opt:   SessionMaxLinks(math.MaxInt),
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
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewSession(SessionIncomingWindow(incomingWindow), SessionOutgoingWindow(outgoingWindow))
	if err != nil {
		t.Fatal(err)
	}
	if session == nil {
		t.Fatal("unexpected nil session")
	}
	if sc := session.channel; sc != channelNum {
		t.Fatalf("unexpected channel number %d", sc)
	}
	time.Sleep(100 * time.Millisecond)
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	// creating a session after the connection has been closed returns nothing
	session, err = client.NewSession()
	if !errors.Is(err, ErrConnClosed) {
		t.Fatalf("unexpected error %v", err)
	}
	if session != nil {
		t.Fatal("expected nil session")
	}
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
	if err != nil {
		t.Fatal(err)
	}
	// first session
	session1, err := client.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	if session1 == nil {
		t.Fatal("unexpected nil session")
	}
	if sc := session1.channel; sc != channelNum-1 {
		t.Fatalf("unexpected channel number %d", sc)
	}
	// second session
	time.Sleep(100 * time.Millisecond)
	session2, err := client.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	if session2 == nil {
		t.Fatal("unexpected nil session")
	}
	if sc := session2.channel; sc != channelNum-1 {
		t.Fatalf("unexpected channel number %d", sc)
	}
	time.Sleep(100 * time.Millisecond)
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClientTooManySessions(t *testing.T) {
	channelNum := uint16(0)

	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			// return small number of max channels
			return mocks.EncodeFrame(mocks.FrameAMQP, &frames.PerformOpen{
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
	if err != nil {
		t.Fatal(err)
	}
	for i := uint16(0); i < 3; i++ {
		session, err := client.NewSession()
		if i < 2 {
			if err != nil {
				t.Fatal(err)
			}
			if session == nil {
				t.Fatal("unexpected nil session")
			}
		} else {
			// third channel should fail
			if err == nil {
				t.Fatal("unexpected nil error")
			}
			if session != nil {
				t.Fatal("expected nil session")
			}
		}
	}
	time.Sleep(100 * time.Millisecond)
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClientNewSessionInvalidOption(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container")
		case *frames.PerformEnd:
			return mocks.PerformEnd(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

	client, err := New(netConn)
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewSession(SessionMaxLinks(0))
	if err == nil {
		t.Fatal("unexpected nil error")
	}
	if session != nil {
		t.Fatal("expected nil session")
	}
	time.Sleep(100 * time.Millisecond)
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
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
			return mocks.EncodeFrame(mocks.FrameAMQP, &frames.PerformBegin{
				NextOutgoingID: 1,
				IncomingWindow: 5000,
				OutgoingWindow: 1000,
				HandleMax:      math.MaxInt16,
			})
		case *frames.PerformEnd:
			return mocks.PerformEnd(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

	client, err := New(netConn)
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewSession(SessionMaxLinks(0))
	if err == nil {
		t.Fatal("unexpected nil error")
	}
	if session != nil {
		t.Fatal("expected nil session")
	}
	time.Sleep(100 * time.Millisecond)
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
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
		case *frames.PerformEnd:
			return mocks.PerformEnd(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

	client, err := New(netConn)
	if err != nil {
		t.Fatal(err)
	}
	// fisrt session succeeds
	session, err := client.NewSession()
	if err == nil {
		t.Fatal("unexpected nil error")
	}
	if session != nil {
		t.Fatal("expected nil session")
	}
}

func TestClientNewSessionInvalidSecondResponse(t *testing.T) {
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
			return mocks.PerformEnd(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := mocks.NewNetConn(responder)

	client, err := New(netConn)
	if err != nil {
		t.Fatal(err)
	}
	// fisrt session succeeds
	session, err := client.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	if session == nil {
		t.Fatal("unexpected nil session")
	}
	// second session fails
	session, err = client.NewSession()
	if err == nil {
		t.Fatal("unexpected nil error")
	}
	if session != nil {
		t.Fatal("expected nil session")
	}
	time.Sleep(100 * time.Millisecond)
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
}

// TODO: failure cases including link number greater than max links (another possible weird error message)
