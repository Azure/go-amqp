package amqp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/Azure/go-amqp/internal/mocks"
	"github.com/stretchr/testify/require"
)

func TestConnOptions(t *testing.T) {
	tests := []struct {
		label  string
		opts   []ConnOption
		verify func(t *testing.T, c *conn)
		fails  bool
	}{
		{
			label:  "no options",
			verify: func(t *testing.T, c *conn) {},
		},
		{
			label: "multiple properties",
			opts: []ConnOption{
				ConnProperty("x-opt-test1", "test1"),
				ConnProperty("x-opt-test2", "test2"),
				ConnProperty("x-opt-test1", "test3"),
			},
			verify: func(t *testing.T, c *conn) {
				wantProperties := map[encoding.Symbol]interface{}{
					"x-opt-test1": "test3",
					"x-opt-test2": "test2",
				}
				if !testEqual(c.properties, wantProperties) {
					t.Errorf("Properties don't match expected:\n %s", testDiff(c.properties, wantProperties))
				}
			},
		},
		{
			label: "ConnServerHostname",
			opts: []ConnOption{
				ConnServerHostname("testhost"),
			},
			verify: func(t *testing.T, c *conn) {
				if c.hostname != "testhost" {
					t.Errorf("unexpected host name %s", c.hostname)
				}
			},
		},
		{
			label: "ConnTLS",
			opts: []ConnOption{
				ConnTLS(true),
			},
			verify: func(t *testing.T, c *conn) {
				if !c.tlsNegotiation {
					t.Error("expected TLS enabled")
				}
			},
		},
		{
			label: "ConnTLSConfig",
			opts: []ConnOption{
				ConnTLSConfig(&tls.Config{MinVersion: tls.VersionTLS13}),
			},
			verify: func(t *testing.T, c *conn) {
				if c.tlsConfig.MinVersion != tls.VersionTLS13 {
					t.Errorf("unexpected TLS min version %d", c.tlsConfig.MinVersion)
				}
			},
		},
		{
			label: "ConnIdleTimeout_Valid",
			opts: []ConnOption{
				ConnIdleTimeout(15 * time.Minute),
			},
			verify: func(t *testing.T, c *conn) {
				if c.idleTimeout != 15*time.Minute {
					t.Errorf("unexpected idle timeout %s", c.idleTimeout)
				}
			},
		},
		{
			label: "ConnIdleTimeout_Invalid",
			fails: true,
			opts: []ConnOption{
				ConnIdleTimeout(-15 * time.Minute),
			},
		},
		{
			label: "ConnMaxFrameSize_Valid",
			opts: []ConnOption{
				ConnMaxFrameSize(1024),
			},
			verify: func(t *testing.T, c *conn) {
				if c.maxFrameSize != 1024 {
					t.Errorf("unexpected max frame size %d", c.maxFrameSize)
				}
			},
		},
		{
			label: "ConnMaxFrameSize_Invalid",
			fails: true,
			opts: []ConnOption{
				ConnMaxFrameSize(128),
			},
		},
		{
			label: "ConnConnectTimeout",
			opts: []ConnOption{
				ConnConnectTimeout(5 * time.Minute),
			},
			verify: func(t *testing.T, c *conn) {
				if c.connectTimeout != 5*time.Minute {
					t.Errorf("unexpected timeout %s", c.connectTimeout)
				}
			},
		},
		{
			label: "ConnMaxSessions_Success",
			opts: []ConnOption{
				ConnMaxSessions(32768),
			},
			verify: func(t *testing.T, c *conn) {
				if c.channelMax != 32768-1 { // zero-based
					t.Errorf("unexpected session count %d", c.channelMax)
				}
			},
		},
		{
			label: "ConnMaxSessions_TooSmall",
			fails: true,
			opts: []ConnOption{
				ConnMaxSessions(0),
			},
		},
		{
			label: "ConnMaxSessions_TooBig",
			fails: true,
			opts: []ConnOption{
				ConnMaxSessions(70000),
			},
		},
		{
			label: "ConnProperty_Invalid",
			fails: true,
			opts: []ConnOption{
				ConnProperty("", "value"),
			},
		},
		{
			label: "ConnContainerID",
			opts: []ConnOption{
				ConnContainerID("myid"),
			},
			verify: func(t *testing.T, c *conn) {
				if c.containerID != "myid" {
					t.Errorf("unexpected container ID %s", c.containerID)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			got, err := newConn(nil, tt.opts...)
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

func (f fakeDialer) NetDialerDial(c *conn, host, port string) (err error) {
	err = f.error()
	return
}

func (f fakeDialer) TLSDialWithDialer(c *conn, host, port string) (err error) {
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
	c, err := dialConn(":bad url/ value", connDialer(fakeDialer{}))
	require.Error(t, err)
	require.Nil(t, c)
	c, err = dialConn("http://localhost", connDialer(fakeDialer{}))
	require.Error(t, err)
	require.Nil(t, c)
	c, err = dialConn("amqp://localhost", connDialer(fakeDialer{}))
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Nil(t, c.tlsConfig)
	c, err = dialConn("amqps://localhost", connDialer(fakeDialer{}))
	require.NoError(t, err)
	require.NotNil(t, c)
	require.NotNil(t, c.tlsConfig)
	c, err = dialConn("amqp://localhost:12345", connDialer(fakeDialer{}))
	require.NoError(t, err)
	require.NotNil(t, c)
	c, err = dialConn("amqp://username:password@localhost", connDialer(fakeDialer{}))
	require.NoError(t, err)
	require.NotNil(t, c)
	if _, ok := c.saslHandlers[saslMechanismPLAIN]; !ok {
		t.Fatal("missing SASL plain handler")
	}
	c, err = dialConn("amqp://localhost", connDialer(fakeDialer{fail: true}))
	require.Error(t, err)
	require.Nil(t, c)
}

func TestStart(t *testing.T) {
	tests := []struct {
		label     string
		fails     bool
		responder func(frames.FrameBody) ([]byte, error)
	}{
		{
			label: "bad header",
			fails: true,
			responder: func(req frames.FrameBody) ([]byte, error) {
				switch req.(type) {
				case *mocks.AMQPProto:
					return []byte{'B', 'A', 'A', 'D', 0, 1, 0, 0}, nil
				default:
					return nil, fmt.Errorf("unhandled frame %T", req)
				}
			},
		},
		{
			label: "incorrect version",
			fails: true,
			responder: func(req frames.FrameBody) ([]byte, error) {
				switch req.(type) {
				case *mocks.AMQPProto:
					return []byte{'A', 'M', 'Q', 'P', 0, 2, 0, 0}, nil
				default:
					return nil, fmt.Errorf("unhandled frame %T", req)
				}
			},
		},
		{
			label: "failed PerformOpen",
			fails: true,
			responder: func(req frames.FrameBody) ([]byte, error) {
				switch req.(type) {
				case *mocks.AMQPProto:
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
			responder: func(req frames.FrameBody) ([]byte, error) {
				switch req.(type) {
				case *mocks.AMQPProto:
					return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
				case *frames.PerformOpen:
					return mocks.PerformBegin(1)
				default:
					return nil, fmt.Errorf("unhandled frame %T", req)
				}
			},
		},
		{
			label: "success",
			responder: func(req frames.FrameBody) ([]byte, error) {
				switch req.(type) {
				case *mocks.AMQPProto:
					return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
				case *frames.PerformOpen:
					return mocks.PerformOpen("container")
				default:
					return nil, fmt.Errorf("unhandled frame %T", req)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			netConn := mocks.NewNetConn(tt.responder)
			conn, err := newConn(netConn)
			require.NoError(t, err)
			err = conn.Start()
			if tt.fails && err == nil {
				t.Error("unexpected nil error")
			} else if !tt.fails && err != nil {
				t.Error(err)
			}
		})
	}
}

func TestClose(t *testing.T) {
	netConn := mocks.NewNetConn(senderFrameHandlerNoUnhandled(ModeUnsettled))
	conn, err := newConn(netConn)
	require.NoError(t, err)
	require.NoError(t, conn.Start())
	require.NoError(t, conn.Close())
	// with Close error
	netConn = mocks.NewNetConn(senderFrameHandlerNoUnhandled(ModeUnsettled))
	conn, err = newConn(netConn)
	require.NoError(t, err)
	require.NoError(t, conn.Start())
	netConn.OnClose = func() error {
		return errors.New("mock close failed")
	}
	// wait a bit for connReader to read from the mock
	time.Sleep(100 * time.Millisecond)
	require.Error(t, conn.Close())
}

func TestServerSideClose(t *testing.T) {
	netConn := mocks.NewNetConn(senderFrameHandlerNoUnhandled(ModeUnsettled))
	conn, err := newConn(netConn)
	require.NoError(t, err)
	require.NoError(t, conn.Start())
	fr, err := mocks.PerformClose(nil)
	require.NoError(t, err)
	netConn.SendFrame(fr)
	err = conn.Close()
	require.NoError(t, err)
	// with error
	netConn = mocks.NewNetConn(senderFrameHandlerNoUnhandled(ModeUnsettled))
	conn, err = newConn(netConn)
	require.NoError(t, err)
	require.NoError(t, conn.Start())
	fr, err = mocks.PerformClose(&Error{Condition: "Close", Description: "mock server error"})
	require.NoError(t, err)
	netConn.SendFrame(fr)
	// wait a bit for connReader to read from the mock
	time.Sleep(100 * time.Millisecond)
	err = conn.Close()
	var connErr *ConnectionError
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	require.Equal(t, "*Error{Condition: Close, Description: mock server error, Info: map[]}", connErr.Error())
}

func TestKeepAlives(t *testing.T) {
	// closing conn can race with keep-alive ticks, so sometimes we get
	// two in this test.  the test needs to receive at least one keep-alive,
	// so use a buffered channel to absorb any extras.
	keepAlives := make(chan struct{}, 3)
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			// specify small idle timeout so we receive a lot of keep-alives
			return mocks.EncodeFrame(mocks.FrameAMQP, 0, &frames.PerformOpen{ContainerID: "container", IdleTimeout: 100 * time.Millisecond})
		case *frames.PerformClose:
			return mocks.PerformClose(nil)
		case *mocks.KeepAlive:
			keepAlives <- struct{}{}
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}

	netConn := mocks.NewNetConn(responder)
	conn, err := newConn(netConn)
	require.NoError(t, err)
	require.NoError(t, conn.Start())
	// send keepalive
	netConn.SendKeepAlive()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-keepAlives:
		// got keep-alive
	case <-ctx.Done():
		t.Fatal("didn't receive any keepalive frames")
	}
	require.NoError(t, conn.Close())
}

func TestConnReaderError(t *testing.T) {
	netConn := mocks.NewNetConn(senderFrameHandlerNoUnhandled(ModeUnsettled))
	conn, err := newConn(netConn)
	require.NoError(t, err)
	require.NoError(t, conn.Start())
	// trigger some kind of error
	netConn.ReadErr <- errors.New("failed")
	// wait a bit for the connReader goroutine to read from the mock
	time.Sleep(100 * time.Millisecond)
	err = conn.Close()
	var connErr *ConnectionError
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
}

func TestConnWriterError(t *testing.T) {
	netConn := mocks.NewNetConn(senderFrameHandlerNoUnhandled(ModeUnsettled))
	conn, err := newConn(netConn)
	require.NoError(t, err)
	require.NoError(t, conn.Start())
	// send a frame that our responder doesn't handle to simulate a conn.connWriter error
	require.NoError(t, conn.SendFrame(frames.Frame{
		Type: frameTypeAMQP,
		Body: &frames.PerformFlow{},
	}))
	// wait a bit for connReader to read from the mock
	time.Sleep(100 * time.Millisecond)
	err = conn.Close()
	var connErr *ConnectionError
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
}
