package amqp_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"
)

func BenchmarkSimple(b *testing.B) {
	if localBrokerAddr == "" {
		b.Skip()
	}
	client, err := amqp.Dial(localBrokerAddr)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		b.Fatal(err)
	}

	// add a random suffix to the link name so the test broker always creates a new node
	targetName := fmt.Sprintf("BenchmarkSimple %d", rand.Uint64())

	sender, err := session.NewSender(
		amqp.LinkTargetAddress(targetName),
	)
	if err != nil {
		b.Fatal(err)
	}

	receiver, err := session.NewReceiver(
		amqp.LinkSourceAddress(targetName),
	)
	if err != nil {
		b.Fatal(err)
	}

	msg := amqp.NewMessage([]byte("test message"))
	for i := 0; i < b.N; i++ {
		// simple send and receive message, no concurrency
		for j := 0; j < 10000; j++ {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			if err := sender.Send(ctx, msg); err != nil {
				b.Fatal(err)
			}
			cancel()

			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			msg, err := receiver.Receive(ctx)
			cancel()
			if err != nil {
				b.Fatal(err)
			}
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			err = receiver.AcceptMessage(ctx, msg)
			cancel()
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
