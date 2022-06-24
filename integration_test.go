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
)

var localBrokerAddr string

func init() {
	// rand used to generate queue names, non-determinism is fine for this use
	rand.Seed(time.Now().UnixNano())
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
			client, err := amqp.Dial(localBrokerAddr, &amqp.ConnOptions{
				MaxSessions: tt.sessions,
			})
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
				targetName := fmt.Sprintf("%s %d", tt.label, rand.Uint64())

				// Create a sender
				ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
				sender, err := session.NewSender(
					ctx,
					amqp.LinkTargetAddress(targetName),
				)
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
							msg.ApplicationProperties = make(map[string]interface{})
							msg.ApplicationProperties["i"] = index
							ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
							err := sender.Send(ctx, msg)
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
					receiver, err := session.NewReceiver(
						ctx,
						amqp.LinkSourceAddress(targetName),
						amqp.LinkCredit(10),
					)
					cancel()
					if err != nil {
						receiveErr.write(err)
						return
					}
					defer testClose(t, receiver.Close)

					for i := 0; i < len(tt.data); i++ {
						ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
						msg, err := receiver.Receive(ctx)
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
			client, err := amqp.Dial(localBrokerAddr, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			// Open a session
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			session, err := client.NewSession(ctx, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			// Create a sender
			// add a random suffix to the link name so the test broker always creates a new node
			targetName := fmt.Sprintf("%s %d", tt.label, rand.Uint64())
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			sender, err := session.NewSender(
				ctx,
				amqp.LinkTargetAddress(targetName),
			)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			for i, data := range tt.data {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				err = sender.Send(ctx, amqp.NewMessage([]byte(data)))
				cancel()
				if err != nil {
					t.Fatalf("Error after %d sends: %+v", i, err)
				}
			}
			testClose(t, sender.Close)

			// Create a receiver
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			receiver, err := session.NewReceiver(
				ctx,
				amqp.LinkSourceAddress(targetName),
				amqp.LinkCredit(uint32(len(tt.data))),   // enough credit to buffer all messages
				amqp.LinkSenderSettle(amqp.ModeSettled), // don't require acknowledgment
			)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			// read buffered messages
			for i, data := range tt.data {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				msg, err := receiver.Receive(ctx)
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
			client, err := amqp.Dial(localBrokerAddr, nil)
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
				targetName := fmt.Sprintf("%s %d", tt.label, rand.Uint64())

				// Create a sender
				ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
				sender, err := session.NewSender(
					ctx,
					amqp.LinkTargetAddress(targetName),
				)
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
						err = sender.Send(ctx, amqp.NewMessage([]byte(data)))
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
					receiver, err := session.NewReceiver(
						ctx,
						amqp.LinkSourceAddress(targetName),
						amqp.LinkReceiverSettle(amqp.ModeSecond),
					)
					cancel()
					if err != nil {
						receiveErr.write(err)
						return
					}
					defer testClose(t, receiver.Close)

					for i, data := range tt.data {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						msg, err := receiver.Receive(ctx)
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
			client, err := amqp.Dial(localBrokerAddr, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			// Open a session
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
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
				sender, err := session.NewSender(
					ctx,
					amqp.LinkTargetAddress(fmt.Sprintf("TestIntegrationSessionHandleMax %d", rand.Uint64())),
				)
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
			client, err := amqp.Dial(localBrokerAddr, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			// Open a session
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			session, err := client.NewSession(ctx, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			senderOrigin, err := session.NewSender(
				ctx,
				amqp.LinkTargetAddress("TestIntegrationLinkName"),
				amqp.LinkName(tt.name),
			)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			defer testClose(t, senderOrigin.Close)

			// This one should fail
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			sender, err := session.NewSender(
				ctx,
				amqp.LinkTargetAddress("TestIntegrationLinkName"),
				amqp.LinkName(tt.name),
			)
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
		client, err := amqp.Dial(localBrokerAddr, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		// Open a session
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		session, err := client.NewSession(ctx, nil)
		cancel()
		if err != nil {
			t.Fatal(err)
		}

		// Create a sender
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		receiver, err := session.NewReceiver(
			ctx,
			amqp.LinkSourceAddress("TestIntegrationClose"),
		)
		cancel()
		if err != nil {
			t.Fatal(err)
		}

		testClose(t, receiver.Close)

		_, err = receiver.Receive(context.Background())
		if err != amqp.ErrLinkClosed {
			t.Fatalf("Expected ErrLinkClosed from receiver.Receiver, got: %+v", err)
			return
		}

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
		client, err := amqp.Dial(localBrokerAddr, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		// Open a session
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		session, err := client.NewSession(ctx, nil)
		cancel()
		if err != nil {
			t.Fatal(err)
		}

		// Create a sender
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		receiver, err := session.NewReceiver(
			ctx,
			amqp.LinkSourceAddress("TestIntegrationClose"),
		)
		cancel()
		if err != nil {
			t.Fatal(err)
		}

		testClose(t, session.Close)

		msg, err := receiver.Receive(context.Background())
		if !errors.Is(err, amqp.ErrSessionClosed) {
			t.Fatalf("Expected ErrSessionClosed from receiver.Receiver, got: %+v", err)
			return
		}
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
		client, err := amqp.Dial(localBrokerAddr, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		// Open a session
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		session, err := client.NewSession(ctx, nil)
		cancel()
		if err != nil {
			t.Fatal(err)
		}

		// Create a sender
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		receiver, err := session.NewReceiver(
			ctx,
			amqp.LinkSourceAddress("TestIntegrationClose"),
		)
		cancel()
		if err != nil {
			t.Fatal(err)
		}

		err = client.Close()
		if err != nil {
			t.Fatalf("Expected nil error from client.Close(), got: %+v", err)
		}

		msg, err := receiver.Receive(context.Background())
		var connErr *amqp.ConnectionError
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
	// TODO: connReader and connWriter goroutines will leak
	//checkLeaks := leaktest.Check(t)

	// Create client
	client, err := amqp.Dial(localBrokerAddr, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

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
	//checkLeaks()
}

func TestConcurrentSessionsOpenClose(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}
	// TODO: connReader and connWriter goroutines will leak
	//checkLeaks := leaktest.Check(t)

	// Create client
	client, err := amqp.Dial(localBrokerAddr, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
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
	//checkLeaks()
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
