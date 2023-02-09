package amqp_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
)

var localBrokerAddr string
var rng *rand.Rand

func init() {
	// rand used to generate queue names, non-determinism is fine for this use
	rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	localBrokerAddr = os.Getenv("AMQP_BROKER_ADDR")
}

type lockedError struct {
	mu  sync.RWMutex
	err error
}

func (l *lockedError) read() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.err
}

func (l *lockedError) write(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err == nil {
		l.err = err
	}
}

func TestIntegrationRoundTrip(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}
	tests := []struct {
		label    string
		sessions uint16
		data     []string
	}{
		{
			label:    "1 roundtrip, small payload",
			sessions: 1,
			data:     []string{"1Hello there!"},
		},
		{
			label:    "3 roundtrip, small payload",
			sessions: 1,
			data: []string{
				"2Hey there!",
				"2Hi there!",
				"2Ho there!",
			},
		},
		{
			label:    "1000 roundtrip, small payload",
			sessions: 1,
			data: repeatStrings(1000,
				"3Hey there!",
				"3Hi there!",
				"3Ho there!",
			),
		},
		{
			label:    "1 roundtrip, small payload, 10 sessions",
			sessions: 10,
			data:     []string{"1Hello there!"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

			// Create client
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			client, err := amqp.Dial(ctx, localBrokerAddr, &amqp.ConnOptions{
				MaxSessions: tt.sessions,
			})
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			for i := uint16(0); i < tt.sessions; i++ {
				// Open a session
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				session, err := client.NewSession(ctx, nil)
				cancel()
				if err != nil {
					t.Fatal(err)
				}

				// add a random suffix to the link name so the test broker always creates a new node
				targetName := fmt.Sprintf("%s %d", tt.label, rng.Uint64())

				// Create a sender
				ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
				sender, err := session.NewSender(ctx, targetName, nil)
				cancel()
				if err != nil {
					t.Fatal(err)
				}

				// Perform test concurrently for speed and to catch races
				wg := &sync.WaitGroup{}
				wg.Add(2)

				sendErr := lockedError{}
				go func() {
					defer wg.Done()
					maxSendSemaphore := make(chan struct{}, 20)
					for i, d := range tt.data {
						if sendErr.read() != nil {
							// don't send anymore messages if there was an error
							return
						}
						maxSendSemaphore <- struct{}{}
						go func(index int, data string) {
							defer func() { <-maxSendSemaphore }()
							msg := amqp.NewMessage([]byte(data))
							msg.ApplicationProperties = make(map[string]any)
							msg.ApplicationProperties["i"] = index
							ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
							err := sender.Send(ctx, msg, nil)
							cancel()
							if err != nil {
								sendErr.write(fmt.Errorf("error after %d sends: %+v", index, err))
							}
						}(i, d)
					}
				}()

				receiveErr := lockedError{}
				receiveCount := 0
				go func() {
					defer wg.Done()

					// Create a receiver
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					receiver, err := session.NewReceiver(ctx, targetName, &amqp.ReceiverOptions{
						MaxCredit: 10,
					})
					cancel()
					if err != nil {
						receiveErr.write(err)
						return
					}
					defer testClose(t, receiver.Close)

					for i := 0; i < len(tt.data); i++ {
						ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
						msg, err := receiver.Receive(ctx, nil)
						cancel()
						if err != nil {
							receiveErr.write(fmt.Errorf("error after %d receives: %+v", i, err))
							break
						}
						ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
						err = receiver.AcceptMessage(ctx, msg)
						cancel()
						if err != nil {
							receiveErr.write(fmt.Errorf("failed to accept message: %v", err))
							break
						}
						if msg.DeliveryTag == nil {
							receiveErr.write(fmt.Errorf("error after %d receives: nil deliverytag received", i))
							break
						}
						msgIndex, ok := msg.ApplicationProperties["i"].(int64)
						if !ok {
							receiveErr.write(fmt.Errorf("failed to parse i. %v", msg.ApplicationProperties["i"]))
							break
						}
						expectedData := tt.data[msgIndex]
						if !bytes.Equal([]byte(expectedData), msg.GetData()) {
							receiveErr.write(fmt.Errorf("expected received message %d to be %v, but it was %v", msgIndex, expectedData, string(msg.GetData())))
							break
						}
						receiveCount++
						if err != nil {
							receiveErr.write(fmt.Errorf("error after %d receives: %+v", i, err))
							break
						}
					}
				}()

				wg.Wait()
				testClose(t, sender.Close)
				if err = sendErr.read(); err != nil {
					t.Fatalf("send error: %v", err)
				}
				if err = receiveErr.read(); err != nil {
					t.Fatalf("receive error: %v", err)
				}
				if expected := len(tt.data); receiveCount != expected {
					t.Fatalf("expected %d got %d", expected, receiveCount)
				}
			}

			client.Close() // close before leak check
			checkLeaks()   // this is done here because queuesClient starts additional goroutines
		})
	}
}

func TestIntegrationRoundTrip_Buffered(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}
	tests := []struct {
		label string
		data  []string
	}{
		{
			label: "10 buffer, small payload",
			data: []string{
				"2Hey there!",
				"2Hi there!",
				"2Ho there!",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

			// Create client
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			client, err := amqp.Dial(ctx, localBrokerAddr, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			// Open a session
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			session, err := client.NewSession(ctx, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			// Create a sender
			// add a random suffix to the link name so the test broker always creates a new node
			targetName := fmt.Sprintf("%s %d", tt.label, rng.Uint64())
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			sender, err := session.NewSender(ctx, targetName, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			for i, data := range tt.data {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				err = sender.Send(ctx, amqp.NewMessage([]byte(data)), nil)
				cancel()
				if err != nil {
					t.Fatalf("Error after %d sends: %+v", i, err)
				}
			}
			testClose(t, sender.Close)

			// Create a receiver
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			receiver, err := session.NewReceiver(ctx, targetName, &amqp.ReceiverOptions{
				MaxCredit:                 uint32(len(tt.data)),
				RequestedSenderSettleMode: amqp.SenderSettleModeSettled.Ptr(),
			})
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			// read buffered messages
			for i, data := range tt.data {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				msg, err := receiver.Receive(ctx, nil)
				cancel()
				if err != nil {
					t.Fatalf("Error after %d receives: %+v", i, err)
				}
				if !bytes.Equal([]byte(data), msg.GetData()) {
					t.Fatalf("Expected received message %d to be %v, but it was %v", i+1, string(data), string(msg.GetData()))
				}
				ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
				err = receiver.AcceptMessage(ctx, msg)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
			}

			// close link
			testClose(t, receiver.Close)
			client.Close() // close before leak check
			checkLeaks()   // this is done here because queuesClient starts additional goroutines
		})
	}
}

func TestIntegrationReceiverModeSecond(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}
	tests := []struct {
		label    string
		sessions int
		data     []string
	}{
		{
			label:    "3 roundtrip, small payload",
			sessions: 1,
			data: []string{
				"2Hey there!",
				"2Hi there!",
				"2Ho there!",
			},
		},
		{
			label:    "1000 roundtrip, small payload",
			sessions: 1,
			data: repeatStrings(1000,
				"3Hey there!",
				"3Hi there!",
				"3Ho there!",
			),
		},
		{
			label:    "1 roundtrip, small payload, 10 sessions",
			sessions: 10,
			data:     []string{"1Hello there!"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

			// Create client
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			client, err := amqp.Dial(ctx, localBrokerAddr, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			for i := 0; i < tt.sessions; i++ {
				// Open a session
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				session, err := client.NewSession(ctx, nil)
				cancel()
				if err != nil {
					t.Fatal(err)
				}

				// add a random suffix to the link name so the test broker always creates a new node
				targetName := fmt.Sprintf("%s %d", tt.label, rng.Uint64())

				// Create a sender
				ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
				sender, err := session.NewSender(ctx, targetName, nil)
				cancel()
				if err != nil {
					t.Fatal(err)
				}

				// Perform test concurrently for speed and to catch races
				var wg sync.WaitGroup
				wg.Add(2)

				sendErr := lockedError{}
				go func() {
					defer wg.Done()
					defer testClose(t, sender.Close)

					for i, data := range tt.data {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						err = sender.Send(ctx, amqp.NewMessage([]byte(data)), nil)
						cancel()
						if err != nil {
							sendErr.write(fmt.Errorf("Error after %d sends: %+v", i, err))
							break
						}
					}
				}()

				receiveErr := lockedError{}
				go func() {
					defer wg.Done()

					// Create a receiver
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					receiver, err := session.NewReceiver(ctx, targetName, &amqp.ReceiverOptions{
						SettlementMode: amqp.ReceiverSettleModeSecond.Ptr(),
					})
					cancel()
					if err != nil {
						receiveErr.write(err)
						return
					}
					defer testClose(t, receiver.Close)

					for i, data := range tt.data {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						msg, err := receiver.Receive(ctx, nil)
						cancel()
						if err != nil {
							receiveErr.write(fmt.Errorf("Error after %d receives: %+v", i, err))
							break
						}
						if !bytes.Equal([]byte(data), msg.GetData()) {
							receiveErr.write(fmt.Errorf("Expected received message %d to be %v, but it was %v", i+1, string(data), string(msg.GetData())))
							break
						}
						ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
						err = receiver.AcceptMessage(ctx, msg)
						cancel()
						if err != nil {
							receiveErr.write(fmt.Errorf("Error accepting message: %+v", err))
							break
						}
					}
				}()

				wg.Wait()

				if err = sendErr.read(); err != nil {
					t.Fatalf("send error: %v", err)
				}
				if err = receiveErr.read(); err != nil {
					t.Fatalf("receive error: %v", err)
				}
			}

			client.Close() // close before leak check
			checkLeaks()   // this is done here because queuesClient starts additional goroutines
		})
	}
}

func TestIntegrationSessionHandleMax(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	tests := []struct {
		maxLinks uint32
		links    int
		close    int
		error    *regexp.Regexp
	}{
		{
			maxLinks: 4,
			links:    5,
			error:    regexp.MustCompile(`handle max \(4\)`),
		},
		{
			maxLinks: 5,
			links:    5,
		},
		{
			maxLinks: 4,
			links:    5,
			close:    1,
		},
		{
			maxLinks: 4,
			links:    8,
			close:    4,
		},
		{
			maxLinks: 62,
			links:    64,
			close:    2,
		},
		{
			maxLinks: 62,
			links:    64,
			close:    1,
			error:    regexp.MustCompile(`handle max \(62\)`),
		},
	}

	for _, tt := range tests {
		label := fmt.Sprintf("max %d, links %d, close %d", tt.maxLinks, tt.links, tt.close)
		t.Run(label, func(t *testing.T) {
			// checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

			// Create client
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			client, err := amqp.Dial(ctx, localBrokerAddr, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			// Open a session
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			session, err := client.NewSession(ctx, &amqp.SessionOptions{
				MaxLinks: tt.maxLinks,
			})
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			var matches int

			// Create a sender
			for i := 0; i < tt.links; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				sender, err := session.NewSender(ctx, fmt.Sprintf("TestIntegrationSessionHandleMax %d", rng.Uint64()), nil)
				cancel()
				switch {
				case err == nil:
				case tt.error == nil:
					t.Fatal(err)
				case !tt.error.MatchString(err.Error()):
					t.Errorf("expect error to match %q, but it was %q", tt.error, err)
				default:
					matches++
				}

				if tt.close > 0 {
					err = sender.Close(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					tt.close--
				}
			}

			if tt.error != nil && matches == 0 {
				t.Errorf("expect an error")
			}
			if tt.error != nil && matches > 1 {
				t.Errorf("expected 1 matching error, got %d", matches)
			}
		})
	}
}

func TestIntegrationLinkName(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	tests := []struct {
		name  string
		error string
	}{
		{
			name:  "linkA",
			error: "link with name 'linkA' already exists",
		},
	}

	for _, tt := range tests {
		label := fmt.Sprintf("name %v", tt.name)
		t.Run(label, func(t *testing.T) {
			// Create client
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			client, err := amqp.Dial(ctx, localBrokerAddr, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			// Open a session
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			session, err := client.NewSession(ctx, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			senderOrigin, err := session.NewSender(ctx, "TestIntegrationLinkName", &amqp.SenderOptions{
				Name: tt.name,
			})
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			defer testClose(t, senderOrigin.Close)

			// This one should fail
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			sender, err := session.NewSender(ctx, "TestIntegrationLinkName", &amqp.SenderOptions{
				Name: tt.name,
			})
			cancel()
			if err == nil {
				testClose(t, sender.Close)
			}

			switch {
			case err == nil && tt.error == "":
				// success
			case err == nil:
				t.Fatalf("expected error to contain %q, but it was nil", tt.error)
			case !strings.Contains(err.Error(), tt.error):
				t.Errorf("expected error to contain %q, but it was %q", tt.error, err)
			}
		})
	}
}

func TestIntegrationClose(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	label := "link"
	t.Run(label, func(t *testing.T) {
		checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

		// Create client
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		client, err := amqp.Dial(ctx, localBrokerAddr, nil)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		// Open a session
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		session, err := client.NewSession(ctx, nil)
		cancel()
		if err != nil {
			t.Fatal(err)
		}

		// Create a sender
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		receiver, err := session.NewReceiver(ctx, "TestIntegrationClose", nil)
		cancel()
		if err != nil {
			t.Fatal(err)
		}

		testClose(t, receiver.Close)

		_, err = receiver.Receive(context.Background(), nil)
		var linkErr *amqp.LinkError
		require.ErrorAs(t, err, &linkErr)

		err = client.Close() // close before leak check
		if err != nil {
			t.Fatal(err)
		}

		checkLeaks()
	})

	label = "session"
	t.Run(label, func(t *testing.T) {
		checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

		// Create client
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		client, err := amqp.Dial(ctx, localBrokerAddr, nil)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		// Open a session
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		session, err := client.NewSession(ctx, nil)
		cancel()
		if err != nil {
			t.Fatal(err)
		}

		// Create a sender
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		receiver, err := session.NewReceiver(ctx, "TestIntegrationClose", nil)
		cancel()
		if err != nil {
			t.Fatal(err)
		}

		testClose(t, session.Close)

		msg, err := receiver.Receive(context.Background(), nil)
		var sessionErr *amqp.SessionError
		require.ErrorAs(t, err, &sessionErr)
		if msg != nil {
			t.Fatal("expected nil message")
		}

		err = client.Close() // close before leak check
		if err != nil {
			t.Fatal(err)
		}

		checkLeaks()
	})

	label = "conn"
	t.Run(label, func(t *testing.T) {
		checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

		// Create client
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		client, err := amqp.Dial(ctx, localBrokerAddr, nil)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		// Open a session
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		session, err := client.NewSession(ctx, nil)
		cancel()
		if err != nil {
			t.Fatal(err)
		}

		// Create a sender
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		receiver, err := session.NewReceiver(ctx, "TestIntegrationClose", nil)
		cancel()
		if err != nil {
			t.Fatal(err)
		}

		err = client.Close()
		if err != nil {
			t.Fatalf("Expected nil error from client.Close(), got: %+v", err)
		}

		msg, err := receiver.Receive(context.Background(), nil)
		var connErr *amqp.ConnError
		if !errors.As(err, &connErr) {
			t.Fatalf("unexpected error type %T", err)
			return
		}
		if msg != nil {
			t.Fatal("expected nil message")
		}

		checkLeaks()
	})
}

func TestMultipleSessionsOpenClose(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	checkLeaks := leaktest.Check(t)

	// Create client
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := amqp.Dial(ctx, localBrokerAddr, nil)
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	sessions := [10]*amqp.Session{}
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			session, err := client.NewSession(ctx, nil)
			cancel()
			if err != nil {
				t.Fatalf("failed to create session: %v", err)
				return
			}
			sessions[j] = session
		}
		for _, session := range sessions {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = session.Close(ctx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	client.Close()
	checkLeaks()
}

func TestConcurrentSessionsOpenClose(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	checkLeaks := leaktest.Check(t)

	// Create client
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := amqp.Dial(ctx, localBrokerAddr, nil)
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			session, err := client.NewSession(ctx, nil)
			cancel()
			if err != nil {
				t.Errorf("failed to create session: %v", err)
				return
			}
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			err = session.Close(ctx)
			cancel()
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	client.Close()
	checkLeaks()
}

func TestReceiverModeFirst(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	checkLeaks := leaktest.Check(t)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := amqp.Dial(ctx, localBrokerAddr, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	sender, err := session.NewSender(ctx, "TestReceiverModeFirst", nil)
	cancel()
	require.NoError(t, err)

	const (
		linkCredit = 10
		msgCount   = 2 * linkCredit
	)

	// prime with a bunch of messages
	for i := 0; i < msgCount; i++ {
		err = sender.Send(context.Background(), amqp.NewMessage([]byte(fmt.Sprintf("test %d", i))), nil)
		require.NoError(t, err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	err = sender.Close(ctx)
	cancel()
	require.NoError(t, err)

	// create a new sender
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	sender, err = session.NewSender(ctx, "TestReceiverModeFirstOther", nil)
	cancel()
	require.NoError(t, err)

	// now drain the messages

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	recv, err := session.NewReceiver(ctx, "TestReceiverModeFirst", &amqp.ReceiverOptions{
		MaxCredit: linkCredit,
	})
	cancel()
	require.NoError(t, err)

	msgs := make(chan *amqp.Message, linkCredit)
	for i := 0; i < msgCount; i++ {
		// receive one message
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		msg, err := recv.Receive(ctx, nil)
		cancel()
		require.NoError(t, err)

		msgs <- msg

		// send to other sender
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		err = sender.Send(ctx, msg, nil)
		cancel()
		require.NoError(t, err)

		if (i+1)%linkCredit == 0 {
			for j := 0; j < linkCredit; j++ {
				msg = <-msgs
				ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
				err = recv.AcceptMessage(ctx, msg)
				cancel()
				require.NoError(t, err)
			}
		}
	}

	client.Close()
	checkLeaks()
}

func TestSenderExactlyOnce(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	checkLeaks := leaktest.Check(t)

	// Create client
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := amqp.Dial(ctx, localBrokerAddr, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	sender, err := session.NewSender(ctx, "TestSenderExactlyOnce", &amqp.SenderOptions{
		SettlementMode:              amqp.SenderSettleModeUnsettled.Ptr(),
		RequestedReceiverSettleMode: amqp.ReceiverSettleModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	err = sender.Send(ctx, amqp.NewMessage([]byte("hello!")), nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	receiver, err := session.NewReceiver(ctx, "TestSenderExactlyOnce", &amqp.ReceiverOptions{
		SettlementMode: amqp.ReceiverSettleModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	msg, err := receiver.Receive(ctx, nil)
	cancel()
	require.NoError(t, err)

	require.Equal(t, "hello!", string(msg.GetData()))
	client.Close()
	checkLeaks()
}

func repeatStrings(count int, strs ...string) []string {
	var out []string
	for i := 0; i < count; i += len(strs) {
		out = append(out, strs...)
	}
	return out[:count]
}

func testClose(t testing.TB, close func(context.Context) error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := close(ctx)
	if err != nil {
		t.Errorf("error closing: %+v\n", err)
	}
}
