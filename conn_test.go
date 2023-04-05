package amqp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/fake"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/Azure/go-amqp/internal/test"
	"github.com/stretchr/testify/require"
)

func TestConnOptions(t *testing.T) {
	tests := []struct {
		label  string
		opts   ConnOptions
		verify func(t *testing.T, c *Conn)
		fails  bool
	}{
		{
			label:  "no options",
			verify: func(t *testing.T, c *Conn) {},
		},
		{
			label: "multiple properties",
			opts: ConnOptions{
				Properties: map[string]any{
					"x-opt-test1": "test3",
					"x-opt-test2": "test2",
				},
			},
			verify: func(t *testing.T, c *Conn) {
				wantProperties := map[encoding.Symbol]any{
					"x-opt-test1": "test3",
					"x-opt-test2": "test2",
				}
				if !test.Equal(c.properties, wantProperties) {
					require.Equal(t, wantProperties, c.properties)
				}
			},
		},
		{
			label: "ConnServerHostname",
			opts: ConnOptions{
				HostName: "testhost",
			},
			verify: func(t *testing.T, c *Conn) {
				if c.hostname != "testhost" {
					t.Errorf("unexpected host name %s", c.hostname)
				}
			},
		},
		{
			label: "ConnTLSConfig",
			opts: ConnOptions{
				TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13},
			},
			verify: func(t *testing.T, c *Conn) {
				if c.tlsConfig.MinVersion != tls.VersionTLS13 {
					t.Errorf("unexpected TLS min version %d", c.tlsConfig.MinVersion)
				}
			},
		},
		{
			label: "ConnIdleTimeout_Valid",
			opts: ConnOptions{
				IdleTimeout: 15 * time.Minute,
			},
			verify: func(t *testing.T, c *Conn) {
				if c.idleTimeout != 15*time.Minute {
					t.Errorf("unexpected idle timeout %s", c.idleTimeout)
				}
			},
		},
		{
			label: "ConnIdleTimeout_Invalid",
			fails: true,
			opts: ConnOptions{
				IdleTimeout: -15 * time.Minute,
			},
		},
		{
			label: "ConnMaxFrameSize_Valid",
			opts: ConnOptions{
				MaxFrameSize: 1024,
			},
			verify: func(t *testing.T, c *Conn) {
				if c.maxFrameSize != 1024 {
					t.Errorf("unexpected max frame size %d", c.maxFrameSize)
				}
			},
		},
		{
			label: "ConnMaxFrameSize_Invalid",
			fails: true,
			opts: ConnOptions{
				MaxFrameSize: 128,
			},
		},
		{
			label: "ConnMaxSessions_Success",
			opts: ConnOptions{
				MaxSessions: 32768,
			},
			verify: func(t *testing.T, c *Conn) {
				if c.channelMax != 32768 {
					t.Errorf("unexpected session count %d", c.channelMax)
				}
			},
		},
		{
			label: "ConnMaxSessions_TooSmall",
			fails: true,
			opts: ConnOptions{
				MaxSessions: 0,
			},
		},
		{
			label: "ConnContainerID",
			opts: ConnOptions{
				ContainerID: "myid",
			},
			verify: func(t *testing.T, c *Conn) {
				if c.containerID != "myid" {
					t.Errorf("unexpected container ID %s", c.containerID)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			got, err := newConn(nil, &tt.opts)
			if err != nil && !tt.fails {
				t.Fatal(err)
			}
			if !tt.fails {
				tt.verify(t, got)
			}
		})
	}
}

type fakeDialer struct {
	fail bool
}

func (f fakeDialer) NetDialerDial(ctx context.Context, c *Conn, host, port string) (err error) {
	err = f.error()
	return
}

func (f fakeDialer) TLSDialWithDialer(ctx context.Context, c *Conn, host, port string) (err error) {
	err = f.error()
	return
}

func (f fakeDialer) error() error {
	if f.fail {
		return errors.New("failed")
	}
	return nil
}

func TestDialConn(t *testing.T) {
	c, err := dialConn(context.Background(), ":bad url/ value", &ConnOptions{dialer: fakeDialer{}})
	require.Error(t, err)
	require.Nil(t, c)
	c, err = dialConn(context.Background(), "http://localhost", &ConnOptions{dialer: fakeDialer{}})
	require.Error(t, err)
	require.Nil(t, c)
	c, err = dialConn(context.Background(), "amqp://localhost", &ConnOptions{dialer: fakeDialer{}})
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Nil(t, c.tlsConfig)
	c, err = dialConn(context.Background(), "amqps://localhost", &ConnOptions{dialer: fakeDialer{}})
	require.NoError(t, err)
	require.NotNil(t, c)
	require.NotNil(t, c.tlsConfig)
	c, err = dialConn(context.Background(), "amqp://localhost:12345", &ConnOptions{dialer: fakeDialer{}})
	require.NoError(t, err)
	require.NotNil(t, c)
	c, err = dialConn(context.Background(), "amqp://username:password@localhost", &ConnOptions{dialer: fakeDialer{}})
	require.NoError(t, err)
	require.NotNil(t, c)
	if _, ok := c.saslHandlers[saslMechanismPLAIN]; !ok {
		t.Fatal("missing SASL plain handler")
	}
	c, err = dialConn(context.Background(), "amqp://localhost", &ConnOptions{dialer: fakeDialer{fail: true}})
	require.Error(t, err)
	require.Nil(t, c)
}

func TestStart(t *testing.T) {
	tests := []struct {
		label     string
		fails     bool
		responder func(uint16, frames.FrameBody) ([]byte, error)
	}{
		{
			label: "bad header",
			fails: true,
			responder: func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
				switch req.(type) {
				case *fake.AMQPProto:
					return []byte{'B', 'A', 'A', 'D', 0, 1, 0, 0}, nil
				default:
					return nil, fmt.Errorf("unhandled frame %T", req)
				}
			},
		},
		{
			label: "incorrect version",
			fails: true,
			responder: func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
				switch req.(type) {
				case *fake.AMQPProto:
					return []byte{'A', 'M', 'Q', 'P', 0, 2, 0, 0}, nil
				default:
					return nil, fmt.Errorf("unhandled frame %T", req)
				}
			},
		},
		{
			label: "failed PerformOpen",
			fails: true,
			responder: func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
				switch req.(type) {
				case *fake.AMQPProto:
					return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
				case *frames.PerformOpen:
					return nil, errors.New("mock write failure")
				default:
					return nil, fmt.Errorf("unhandled frame %T", req)
				}
			},
		},
		{
			label: "unexpected PerformOpen response",
			fails: true,
			responder: func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
				switch req.(type) {
				case *fake.AMQPProto:
					return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
				case *frames.PerformOpen:
					return fake.PerformBegin(0, 1)
				default:
					return nil, fmt.Errorf("unhandled frame %T", req)
				}
			},
		},
		{
			label: "success",
			responder: func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
				switch req.(type) {
				case *fake.AMQPProto:
					return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
				case *frames.PerformOpen:
					return fake.PerformOpen("container")
				default:
					return nil, fmt.Errorf("unhandled frame %T", req)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			netConn := fake.NewNetConn(tt.responder)
			conn, err := newConn(netConn, nil)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = conn.start(ctx)
			cancel()
			if tt.fails && err == nil {
				t.Error("unexpected nil error")
			} else if !tt.fails && err != nil {
				t.Error(err)
			}
		})
	}
}

func TestClose(t *testing.T) {
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled))
	conn, err := newConn(netConn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	require.NoError(t, conn.start(ctx))
	cancel()
	require.NoError(t, conn.Close())
	// with Close error
	netConn = fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled))
	conn, err = newConn(netConn, nil)
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	require.NoError(t, conn.start(ctx))
	cancel()
	netConn.OnClose = func() error {
		return errors.New("mock close failed")
	}
	// wait a bit for connReader to read from the mock
	time.Sleep(100 * time.Millisecond)
	require.Error(t, conn.Close())
}

func TestServerSideClose(t *testing.T) {
	closeReceived := make(chan struct{})
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformClose:
			close(closeReceived)
			return fake.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)
	conn, err := newConn(netConn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	require.NoError(t, conn.start(ctx))
	cancel()
	fr, err := fake.PerformClose(nil)
	require.NoError(t, err)
	netConn.SendFrame(fr)
	<-closeReceived
	err = conn.Close()
	require.NoError(t, err)

	// with error
	closeReceived = make(chan struct{})
	netConn = fake.NewNetConn(responder)
	conn, err = newConn(netConn, nil)
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	require.NoError(t, conn.start(ctx))
	cancel()
	fr, err = fake.PerformClose(&Error{Condition: "Close", Description: "mock server error"})
	require.NoError(t, err)
	netConn.SendFrame(fr)
	<-closeReceived
	err = conn.Close()
	var connErr *ConnError
	require.ErrorAs(t, err, &connErr)
	require.Equal(t, "*Error{Condition: Close, Description: mock server error, Info: map[]}", connErr.Error())
}

func TestKeepAlives(t *testing.T) {
	// closing conn can race with keep-alive ticks, so sometimes we get
	// two in this test.  the test needs to receive at least one keep-alive,
	// so use a buffered channel to absorb any extras.
	keepAlives := make(chan struct{}, 3)
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			// specify small idle timeout so we receive a lot of keep-alives
			return fake.EncodeFrame(frames.TypeAMQP, 0, &frames.PerformOpen{ContainerID: "container", IdleTimeout: 100 * time.Millisecond})
		case *fake.KeepAlive:
			keepAlives <- struct{}{}
			return nil, nil
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}

	netConn := fake.NewNetConn(responder)
	conn, err := newConn(netConn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	require.NoError(t, conn.start(ctx))
	cancel()
	// send keepalive
	netConn.SendKeepAlive()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-keepAlives:
		// got keep-alive
	case <-ctx.Done():
		t.Fatal("didn't receive any keepalive frames")
	}
	require.NoError(t, conn.Close())
}

func TestKeepAlivesIdleTimeout(t *testing.T) {
	start := make(chan struct{})
	done := make(chan struct{})

	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			close(start)
			return fake.EncodeFrame(frames.TypeAMQP, 0, &frames.PerformOpen{ContainerID: "container", IdleTimeout: time.Minute})
		case *fake.KeepAlive:
			return nil, nil
		case *frames.PerformClose:
			close(done)
			return fake.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}

	const idleTimeout = 100 * time.Millisecond

	netConn := fake.NewNetConn(responder)
	conn, err := newConn(netConn, &ConnOptions{
		IdleTimeout: idleTimeout,
	})
	require.NoError(t, err)

	go func() {
		<-start
		for {
			select {
			case <-time.After(idleTimeout / 2):
				netConn.SendKeepAlive()
			case <-done:
				return
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	require.NoError(t, conn.start(ctx))
	cancel()

	time.Sleep(2 * idleTimeout)
	require.NoError(t, conn.Close())
}

func TestConnReaderError(t *testing.T) {
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled))
	conn, err := newConn(netConn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	require.NoError(t, conn.start(ctx))
	cancel()
	// trigger some kind of error
	netConn.ReadErr <- errors.New("failed")
	// wait a bit for the connReader goroutine to read from the mock
	time.Sleep(100 * time.Millisecond)
	err = conn.Close()
	var connErr *ConnError
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
}

func TestConnWriterError(t *testing.T) {
	netConn := fake.NewNetConn(senderFrameHandlerNoUnhandled(0, SenderSettleModeUnsettled))
	conn, err := newConn(netConn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	require.NoError(t, conn.start(ctx))
	cancel()
	netConn.WriteErr <- errors.New("boom")
	// wait a bit for connReader to read from the mock
	time.Sleep(100 * time.Millisecond)
	err = conn.Close()
	var connErr *ConnError
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
}

func TestConnWithZeroByteReads(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}

	netConn := fake.NewNetConn(responder)
	netConn.SendFrame([]byte{})

	conn, err := newConn(netConn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	require.NoError(t, conn.start(ctx))
	cancel()
	require.NoError(t, conn.Close())
}

func TestConnNegotiationTimeout(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		return nil, nil
	}

	netConn := fake.NewNetConn(responder)
	conn, err := newConn(netConn, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	require.ErrorIs(t, conn.start(ctx), context.DeadlineExceeded)
	cancel()
}

type mockDialer struct {
	resp func(uint16, frames.FrameBody) ([]byte, error)
}

func (m mockDialer) NetDialerDial(ctx context.Context, c *Conn, host, port string) error {
	c.net = fake.NewNetConn(m.resp)
	return nil
}

func (mockDialer) TLSDialWithDialer(ctx context.Context, c *Conn, host, port string) error {
	panic("nyi")
}

func TestClientDial(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := Dial(ctx, "amqp://localhost", &ConnOptions{dialer: mockDialer{resp: responder}})
	cancel()
	require.NoError(t, err)
	require.NotNil(t, client)
	// error case
	responder = func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return nil, errors.New("mock read failed")
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	client, err = Dial(ctx, "amqp://localhost", &ConnOptions{dialer: mockDialer{resp: responder}})
	cancel()
	require.Error(t, err)
	require.Nil(t, client)
}

func TestClientClose(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := Dial(ctx, "amqp://localhost", &ConnOptions{dialer: mockDialer{resp: responder}})
	cancel()
	require.NoError(t, err)
	require.NotNil(t, client)
	require.NoError(t, client.Close())
	require.NoError(t, client.Close())
}

func TestSessionOptions(t *testing.T) {
	tests := []struct {
		label  string
		opt    SessionOptions
		verify func(t *testing.T, s *Session)
	}{
		{
			label: "SessionMaxLinks",
			opt: SessionOptions{
				MaxLinks: 4096,
			},
			verify: func(t *testing.T, s *Session) {
				if s.handleMax != 4096-1 {
					t.Errorf("unexpected max links %d", s.handleMax)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			session := newSession(nil, 0, &tt.opt)
			tt.verify(t, session)
		})
	}
}

func TestClientNewSession(t *testing.T) {
	const channelNum = 0
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch tt := req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformBegin:
			if tt.RemoteChannel != nil {
				return nil, errors.New("expected nil remote channel")
			}
			return fake.PerformBegin(channelNum, remoteChannel)
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	require.NotNil(t, session)
	require.Equal(t, uint16(channelNum), session.channel)
	require.NoError(t, client.Close())
	// creating a session after the connection has been closed returns nothing
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err = client.NewSession(ctx, nil)
	cancel()
	var connErr *ConnError
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	require.Equal(t, "amqp: connection closed", connErr.Error())
	require.Nil(t, session)
}

func TestClientMultipleSessions(t *testing.T) {
	channelNum := uint16(0)
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformBegin:
			b, err := fake.PerformBegin(channelNum, remoteChannel)
			channelNum++
			return b, err
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)
	// first session
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session1, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	require.NotNil(t, session1)
	require.Equal(t, channelNum-1, session1.channel)
	// second session
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session2, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	require.NotNil(t, session2)
	require.Equal(t, channelNum-1, session2.channel)
	require.NoError(t, client.Close())
}

func TestClientTooManySessions(t *testing.T) {
	channelNum := uint16(0)
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			// return small number of max channels
			return fake.EncodeFrame(frames.TypeAMQP, 0, &frames.PerformOpen{
				ChannelMax:   1,
				ContainerID:  "test",
				IdleTimeout:  time.Minute,
				MaxFrameSize: 4294967295,
			})
		case *frames.PerformBegin:
			b, err := fake.PerformBegin(channelNum, remoteChannel)
			channelNum++
			return b, err
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)
	for i := uint16(0); i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		session, err := client.NewSession(ctx, nil)
		cancel()
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

func TestClientNewSessionMissingRemoteChannel(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformBegin:
			// return begin with nil RemoteChannel
			return fake.EncodeFrame(frames.TypeAMQP, 0, &frames.PerformBegin{
				NextOutgoingID: 1,
				IncomingWindow: 5000,
				OutgoingWindow: 1000,
				HandleMax:      math.MaxInt16,
			})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, &SessionOptions{
		MaxLinks: 1,
	})
	cancel()
	require.Error(t, err)
	require.Nil(t, session)
	require.Error(t, client.Close())
}

func TestClientNewSessionInvalidInitialResponse(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformBegin:
			// respond with the wrong frame type
			return fake.PerformOpen("bad")
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.Error(t, err)
	require.Nil(t, session)
}

func TestClientNewSessionInvalidSecondResponseSameChannel(t *testing.T) {
	firstChan := true
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		case *frames.PerformBegin:
			if firstChan {
				firstChan = false
				return fake.PerformBegin(0, remoteChannel)
			}
			// respond with the wrong frame type
			return fake.PerformOpen("bad")
		case *frames.PerformEnd:
			return fake.PerformEnd(0, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)
	// fisrt session succeeds
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	require.NotNil(t, session)
	// second session fails - times out as the ack is never received
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err = client.NewSession(ctx, nil)
	cancel()
	require.Error(t, err)
	require.Nil(t, session)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, client.Close())
}

func TestClientNewSessionInvalidSecondResponseDifferentChannel(t *testing.T) {
	firstChan := true
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformBegin:
			if firstChan {
				firstChan = false
				return fake.PerformBegin(0, remoteChannel)
			}
			// respond with the wrong frame type
			// note that it has to be for the next channel
			return fake.PerformDisposition(encoding.RoleSender, 1, 0, nil, nil)
		case *frames.PerformEnd:
			return fake.PerformEnd(0, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)
	// fisrt session succeeds
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	require.NotNil(t, session)
	// second session fails
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err = client.NewSession(ctx, nil)
	cancel()
	require.Error(t, err)
	require.Nil(t, session)
	require.Error(t, client.Close())
}

func TestNewSessionTimedOut(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformClose:
			return fake.PerformClose(nil)
		case *frames.PerformBegin:
			// swallow the frame so NewSession never gets an ack
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)
	// fisrt session succeeds
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, session)
}

func TestNewSessionWriteError(t *testing.T) {
	endAck := make(chan struct{})
	responder := func(remoteChannel uint16, req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *fake.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return fake.PerformOpen("container")
		case *frames.PerformBegin:
			return nil, errors.New("write error")
		case *frames.PerformEnd:
			close(endAck)
			return fake.PerformEnd(0, nil)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)
	// fisrt session succeeds
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	var connErr *ConnError
	require.ErrorAs(t, err, &connErr)
	require.Equal(t, "write error", connErr.Error())
	require.Nil(t, session)

	select {
	case <-time.After(time.Second):
		// expected
	case <-endAck:
		t.Fatal("unexpected ack")
	}
}
