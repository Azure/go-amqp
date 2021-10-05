package amqp

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/Azure/go-amqp/internal/mocks"
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

func TestDialConn(t *testing.T) {
	tcpDialer = func(network, address string) (net.Conn, error) {
		return nil, nil
	}
	tlsDialer = func(dialer *net.Dialer, network, addr string, config *tls.Config) (*tls.Conn, error) {
		return nil, nil
	}
	defer func() {
		// reset
		tcpDialer = nil
		tlsDialer = tls.DialWithDialer
	}()
	c, err := dialConn(":bad url/ value")
	if err == nil {
		t.Fatal("unexpected nil error")
	}
	if c != nil {
		t.Fatal("expected nil conn")
	}
	c, err = dialConn("http://localhost")
	if err == nil {
		t.Fatal("unexpected nil error")
	}
	if c != nil {
		t.Fatal("expected nil conn")
	}
	c, err = dialConn("amqp://localhost")
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("unexpected nil conn")
	}
	if c.tlsConfig != nil {
		t.Fatal("expected no TLS config")
	}
	c, err = dialConn("amqps://localhost")
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("unexpected nil conn")
	}
	if c.tlsConfig == nil {
		t.Fatal("unexpected nil TLS config")
	}
	c, err = dialConn("amqp://localhost:12345")
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("unexpected nil conn")
	}
	c, err = dialConn("amqp://username:password@localhost")
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("unexpected nil conn")
	}
	if _, ok := c.saslHandlers[saslMechanismPLAIN]; !ok {
		t.Fatal("missing SASL plain handler")
	}
	tcpDialer = func(network, address string) (net.Conn, error) {
		return nil, errors.New("failed")
	}
	c, err = dialConn("amqp://localhost")
	if err == nil {
		t.Fatal("unexpected nil error")
	}
	if c != nil {
		t.Fatal("expected nil conn")
	}
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
					return mocks.PerformOpen("container", 0)
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
			if err != nil {
				t.Fatal(err)
			}
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
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container", 0)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}

	netConn := mocks.NewNetConn(responder)
	conn, err := newConn(netConn)
	if err != nil {
		t.Fatal(err)
	}
	if err = conn.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err = conn.Close(); err != nil {
		t.Fatal(err)
	}
	// with Close error
	netConn = mocks.NewNetConn(responder)
	conn, err = newConn(netConn)
	if err != nil {
		t.Fatal(err)
	}
	if err = conn.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	netConn.OnClose = func() error {
		return errors.New("mock close failed")
	}
	if err = conn.Close(); err == nil {
		t.Fatal("unexpected nil error")
	}
}

func TestServerSideClose(t *testing.T) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			return mocks.PerformOpen("container", 0)
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}

	netConn := mocks.NewNetConn(responder)
	conn, err := newConn(netConn)
	if err != nil {
		t.Fatal(err)
	}
	err = conn.Start()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	fr, err := mocks.PerformClose(nil)
	if err != nil {
		t.Fatal(err)
	}
	netConn.SendFrame(fr)
	time.Sleep(100 * time.Millisecond)
	err = conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	// with error
	netConn = mocks.NewNetConn(responder)
	conn, err = newConn(netConn)
	if err != nil {
		t.Fatal(err)
	}
	err = conn.Start()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	fr, err = mocks.PerformClose(&Error{Condition: "Close", Description: "mock server error"})
	if err != nil {
		t.Fatal(err)
	}
	netConn.SendFrame(fr)
	time.Sleep(100 * time.Millisecond)
	err = conn.Close()
	var ee *Error
	if !errors.As(err, &ee) {
		t.Fatalf("unexpected error type %T", err)
	}
	if ee.Condition != "Close" {
		t.Fatalf("unexpected error value %+v", *ee)
	}
}

func TestKeepAlives(t *testing.T) {
	keepAlives := 0
	responder := func(req frames.FrameBody) ([]byte, error) {
		switch req.(type) {
		case *mocks.AMQPProto:
			return []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}, nil
		case *frames.PerformOpen:
			// specify small idle timeout so we receive a lot of keep-alives
			return mocks.PerformOpen("container", 1*time.Millisecond)
		case *mocks.KeepAlive:
			keepAlives++
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}

	netConn := mocks.NewNetConn(responder)
	conn, err := newConn(netConn)
	if err != nil {
		t.Fatal(err)
	}
	err = conn.Start()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	// send keepalive
	netConn.SendKeepAlive()
	time.Sleep(100 * time.Millisecond)
	err = conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	if keepAlives == 0 {
		t.Fatal("didn't receive any keepalive frames")
	}
}
