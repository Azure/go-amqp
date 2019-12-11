// +build local

package amqp_test

import (
	"net"
	"testing"

	"github.com/Azure/go-amqp"
)

// Tests that require a local broker running on the standard AMQP port.

func TestDial_IPV6(t *testing.T) {
	if c, err := amqp.Dial("amqp://localhost"); err != nil {
		t.Skip("can't connect to local AMQP server")
	} else {
		c.Close()
	}
	l, err := net.Listen("tcp6", "[::]:0")
	if err != nil {
		t.Skip("ipv6 not supported")
	}
	l.Close()

	for _, u := range []string{"amqp://[::]:5672", "amqp://[::]"} {
		u := u // Don't  use range variable in func literal.
		t.Run(u, func(t *testing.T) {
			c, err := amqp.Dial(u)
			if err != nil {
				t.Errorf("%q: %v", u, err)
			} else {
				c.Close()
			}
		})
	}
}
