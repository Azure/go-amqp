package amqp_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"
)

var liveTestCheck = func(t *testing.T) {}
var defaultTimeout = 5 * time.Minute

func init() {
	_, err := newTestVars()

	if err != nil {
		fmt.Printf("Errors loading test vars, live tests will be disabled: %s", err)

		liveTestCheck = func(t *testing.T) {
			t.Skipf("Live tests are disabled, environment not defined")
		}
		return
	}

	// purge the queue before all the tests start.
	testClients := mustCreateClients()

	defer func() {
		if err := testClients.Cleanup(); err != nil {
			panic(err)
		}
	}()

	fmt.Printf("Cleaning up queue for tests\n")
	purged := 0

	for { // purge all the old messages
		msgs, err := receiveMessages(testClients, &receiveMessagesOptions{
			MaxWaitForMessage: 10 * time.Second,
			Count:             1000,
		})

		if err != nil {
			panic(err)
		}

		purged += len(msgs)

		if len(msgs) == 0 {
			break
		}
	}

	if purged > 0 {
		fmt.Printf("(init) Purged %d messages\n", purged)
	}
}

func TestTransactionControllerDeclare(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	defer func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	transactionID, err := clients.TC.Declare(ctx, amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, transactionID)

	ctx, cancel = context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	err = clients.TC.Discharge(ctx, amqp.TransactionDischarge{
		TransactionID: transactionID,
	}, nil)
	require.NoError(t, err)
}

func TestTransactionControllerDeclareWithUnsupportedCapabilities(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	defer func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	}()

	tc, err := clients.Session.NewTransactionController(context.Background(), &amqp.TransactionControllerOptions{
		MultiSessionsPerTransaction:    true,
		MultipleTransactionsPerSession: true,
		LocalTransactions:              true,
		DistributedTransactions:        true,
		PromotableTransactions:         true,
	})
	require.Nil(t, tc)
	var amqpErr *amqp.Error
	require.ErrorAs(t, err, &amqpErr)
	require.Equal(t, amqp.ErrCondPreconditionFailed, amqpErr.Condition)
	require.Equal(t, "Transaction coordinator did not support all desired capabilities: amqp:distributed-transactions,amqp:promotable-transactions", amqpErr.Description)
}

func TestTransactionControllerDeclareAndDischargeNoMessages(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	defer func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	transactionID, err := clients.TC.Declare(ctx, amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, transactionID)

	ctx, cancel = context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	err = clients.TC.Discharge(ctx, amqp.TransactionDischarge{
		TransactionID: transactionID,
	}, nil)
	require.NoError(t, err)
}

func TestTransactionControllerDeclareAndDischarge(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	defer func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	}()

	now := time.Now().UnixNano()

	// use the same transaction controller for three transactions, serially.
	for i := 0; i < 3; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer cancel()

			transactionID, err := clients.TC.Declare(ctx, amqp.TransactionDeclare{}, nil)
			require.NoError(t, err)
			require.NotEmpty(t, transactionID)

			body := fmt.Sprintf(t.Name()+": %d, i:%d", now, i)

			err = sendTestMessage(t, clients, &amqp.Message{Value: body, TransactionID: transactionID})
			require.NoError(t, err)

			ctx, cancel = context.WithTimeout(context.Background(), defaultTimeout)
			defer cancel()

			err = clients.TC.Discharge(ctx, amqp.TransactionDischarge{
				TransactionID: transactionID,
			}, nil)
			require.NoError(t, err)

			t.Logf("Waiting to receive message we commited in the transaction")
			now := time.Now()
			messages, err := receiveMessages(clients, nil)
			require.NoError(t, err)
			require.Equal(t, body, messages[0].Value)
			require.Equal(t, 1, len(messages))
			t.Logf("Took %d seconds to receive messages committed through transaction", time.Since(now)/time.Second)
		})
	}

}

func TestTransactionControllerDischargingWithInvalidID(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	defer func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	}()

	// this is from an older transaction I ran. Won't be valid anymore.
	id := []byte("txn:1e99912c4f2849d6845ecb72e321795b__G65:3417454_G65")

	// Discharging twice:
	// *Error{Condition: amqp:transaction:unknown-id, Description: Transaction is not declared Reference:638f20d1-d269-4579-b819-45782f70347e,
	// TrackingId:68f7ab2d-700a-45d7-9873-51ed47cc55c7_G34, SystemTracker:gtm, Timestamp:2023-08-28T22:57:19, Info: map[]}
	err := clients.TC.Discharge(context.Background(), amqp.TransactionDischarge{
		TransactionID: id,
	}, nil)
	requireAMQPError(t, err, amqp.ErrCondTransactionUnknownID)
}

func TestTransactionControllerDeclareWithGlobalID(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	defer func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	}()

	transactionID, err := clients.TC.Declare(context.Background(), amqp.TransactionDeclare{
		// this was just ignored by Service Bus...
		GlobalID: []byte("some global ID"),
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, transactionID)
}

// TestTransactionControllerImplicitRollback checks that if you just close your link involved in the transaction
// that the default is to rollback.
func TestTransactionControllerImplicitRollback(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	defer func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	}()

	transactionID, err := clients.TC.Declare(context.Background(), amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)

	// send the message, associated with the transaction
	err = sendTestMessage(t, clients, &amqp.Message{Value: "hello", TransactionID: transactionID})
	require.NoError(t, err)

	// shut everything down instead
	err = clients.Cleanup()
	require.NoError(t, err)

	// check that it's clean.
	clients = mustCreateClients()

	messages, err := receiveMessages(clients, nil)
	require.NoError(t, err)
	require.Empty(t, messages)
}

func TestTransactionControllersFromSeparateConnectionsAreIndependent(t *testing.T) {
	liveTestCheck(t)

	clients1 := mustCreateClients()

	t.Cleanup(func() {
		err := clients1.Cleanup()
		require.NoError(t, err)
	})

	otherClients := mustCreateClients()

	t.Cleanup(func() {
		err := otherClients.Cleanup()
		require.NoError(t, err)
	})

	transID, err := clients1.TC.Declare(context.Background(), amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, transID)

	// attempt to Discharge a transaction on a separate connection than it was created on.
	err = otherClients.TC.Discharge(context.Background(), amqp.TransactionDischarge{
		TransactionID: transID,
	}, nil)
	requireAMQPError(t, err, amqp.ErrCondTransactionUnknownID, "transaction IDs aren't global across connections")

	// now let's try sending a message as part of a transaction from another connection
	err = sendTestMessage(t, otherClients, &amqp.Message{Value: "hello", TransactionID: transID})
	requireAMQPError(t, err, amqp.ErrCondTransactionUnknownID, "transaction IDs aren't global across connections")
}

func TestTransactionControllerMultipleActiveTransactions(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	t.Cleanup(func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	})

	now := time.Now().UnixNano()
	firstMsgValue := fmt.Sprintf("first transaction: %d", now)
	secondMsgValue := fmt.Sprintf("second transaction: %d", now)

	// multiple transactions can be active simultaneously.

	// this first transaction will be completed before the second transaction (which we're about to also
	// create).
	firstTransID, err := clients.TC.Declare(context.Background(), amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, firstTransID)

	secondTransID, err := clients.TC.Declare(context.Background(), amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, secondTransID)

	// send a message to each transaction - so now we have pending work in both, simultaneously.
	err = sendTestMessage(t, clients, &amqp.Message{Value: firstMsgValue, TransactionID: firstTransID})
	require.NoError(t, err)

	err = sendTestMessage(t, clients, &amqp.Message{Value: secondMsgValue, TransactionID: secondTransID})
	require.NoError(t, err)

	t.Logf("Committing first transaction")
	// now we'll commit the first transaction. The second transaction remains uncommitted.
	err = clients.TC.Discharge(context.Background(), amqp.TransactionDischarge{
		TransactionID: firstTransID,
	}, nil)
	require.NoError(t, err)

	// and now the message is available.
	messages, err := receiveMessages(clients, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(messages))

	// we get the message from our first transaction
	require.Equal(t, firstMsgValue, messages[0].Value)

	t.Logf("Committing second transaction")
	// now we'll commit the second transaction, and validate that it's event is still available.
	err = clients.TC.Discharge(context.Background(), amqp.TransactionDischarge{
		TransactionID: secondTransID,
	}, nil)
	require.NoError(t, err)

	messages, err = receiveMessages(clients, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(messages))

	require.Equal(t, secondMsgValue, messages[0].Value)
}

func TestTransactionControllerTimeout(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	defer func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	}()

	tc, err := clients.Session.NewTransactionController(context.Background(), nil)
	require.NoError(t, err)

	// transactions (with Service Bus) can only last for two minutes.
	// Hm...
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	transactionID, err := tc.Declare(ctx, amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, transactionID)

	err = sendTestMessage(t, clients, &amqp.Message{Value: "hello", TransactionID: transactionID})
	require.NoError(t, err)

	// > The transaction timer starts when the first operation in the transaction starts.
	start := time.Now()

	// We can't be totally idle or else they'll shut down the connection.
	// We want it to be alive, with the transaction just sitting there un-discharged.
	{
		// Let's just make sure the connection doesn't idle out and wait for the transaction to
		// timeout, as documented here:
		// https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-transactions#timeout
		//
		// > A transaction times out after 2 minutes
		ctx, cancel := context.WithDeadline(context.Background(), start.Add(3*time.Minute+30*time.Second))
		defer cancel()

		go func() {
			t.Logf("Waiting for the transaction to expire")

		Loop:
			for {
				select {
				case <-ctx.Done():
					break Loop
				default:
					t.Logf("Sending message to keep connection alive. %s seconds since transaction started.", time.Since(start)/time.Second)
					err = sendTestMessage(t, clients, &amqp.Message{Value: "hello"})
					require.NoError(t, err)

					messages, err := receiveMessages(clients, nil)
					require.NoError(t, err)
					require.Equal(t, 1, len(messages))

					t.Logf("Sleeping for a minute")

					select {
					case <-ctx.Done():
						break Loop
					case <-time.After(time.Minute):
					}
				}
			}
		}()

		<-ctx.Done()
	}

	ctx, cancel = context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	err = tc.Discharge(ctx, amqp.TransactionDischarge{
		TransactionID: transactionID,
	}, nil)
	requireAMQPError(t, err, amqp.ErrCondTransactionUnknownID)

	messages, err := receiveMessages(clients, &receiveMessagesOptions{
		MaxWaitForMessage: 30 * time.Second,
		Count:             1,
	})
	require.NoError(t, err)
	require.Empty(t, messages)
}

func TestTempTransactionControllerReceiveCompleteAndRollback(t *testing.T) {
	require.Fail(t, "Test not written yet")
}

type clients struct {
	Conn      *amqp.Conn
	Session   *amqp.Session
	TC        *amqp.TransactionController
	Cleanup   func() error
	QueueName string
}

type testVars struct {
	Endpoint  string
	User      string
	Password  string
	QueueName string
}

func newTestVars() (testVars, error) {
	if err := godotenv.Load(); err != nil {
		return testVars{}, err
	}

	var missing []string

	getEnv := func(ev string) string {
		v := os.Getenv(ev)
		if v == "" {
			missing = append(missing, ev)
		}
		return v
	}

	tv := testVars{
		QueueName: getEnv("QUEUE_NAME"), // ex: demo
	}

	// parse this out from the connection string in SERVICEBUS_CONNECTION_STRING
	// Ex: Endpoint=sb://<servicebus>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<key>
	cs := getEnv("SERVICEBUS_CONNECTION_STRING")

	for _, part := range strings.Split(cs, ";") {
		kvparts := strings.SplitN(part, "=", 2)

		switch kvparts[0] {
		case "Endpoint":
			tv.Endpoint = strings.Replace(kvparts[1], "sb://", "amqps://", 1)
		case "SharedAccessKeyName":
			tv.User = kvparts[1]
		case "SharedAccessKey":
			tv.Password = kvparts[1]
		}
	}

	if tv.Endpoint == "" || tv.User == "" || tv.Password == "" || tv.QueueName == "" {
		return testVars{}, fmt.Errorf("Missing environment variables: %s", strings.Join(missing, ","))
	}

	return tv, nil
}

func mustCreateClients() clients {
	testVars, err := newTestVars()

	if err != nil {
		panic(err)
	}

	// Optional: Useful if you want to view this encrypted traffic using wireshark.
	tlsKeyLogPath := os.Getenv("TLS_KEYLOGPATH")

	var tlsConfig *tls.Config

	if tlsKeyLogPath != "" {
		f, err := os.OpenFile(tlsKeyLogPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0777)

		if err != nil {
			panic(err)
		}

		tlsConfig = &tls.Config{
			KeyLogWriter: f,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	conn, err := amqp.Dial(ctx, testVars.Endpoint, &amqp.ConnOptions{
		SASLType:  amqp.SASLTypePlain(testVars.User, testVars.Password),
		TLSConfig: tlsConfig,
	})
	if err != nil {
		panic(err)
	}

	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		panic(err)
	}

	tc, err := sess.NewTransactionController(ctx, nil)
	if err != nil {
		panic(err)
	}

	cleanupFn := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()

		if err := tc.Close(ctx); err != nil {
			return err
		}

		if err := sess.Close(ctx); err != nil {
			return err
		}

		return conn.Close()
	}

	return clients{
		Cleanup:   cleanupFn,
		Conn:      conn,
		QueueName: testVars.QueueName,
		Session:   sess,
		TC:        tc,
	}
}

type receiveMessagesOptions struct {
	MaxWaitForMessage time.Duration
	Count             int
}

func receiveMessages(clients clients, tmpOpts *receiveMessagesOptions) ([]*amqp.Message, error) {
	var opts receiveMessagesOptions

	if tmpOpts != nil {
		opts.MaxWaitForMessage = tmpOpts.MaxWaitForMessage
		opts.Count = tmpOpts.Count
	}

	if opts.MaxWaitForMessage == 0 {
		opts.MaxWaitForMessage = defaultTimeout
	}

	if opts.Count == 0 {
		opts.Count = 1
	}

	session, err := clients.Conn.NewSession(context.Background(), nil)

	if err != nil {
		return nil, err
	}

	defer func() {
		err := session.Close(context.Background())

		if err != nil {
			panic(err)
		}
	}()

	receiver, err := session.NewReceiver(context.Background(), clients.QueueName, &amqp.ReceiverOptions{
		Credit:         -1,
		SettlementMode: amqp.ReceiverSettleModeSecond.Ptr(),
	})

	if err != nil {
		return nil, err
	}

	defer func() {
		err := receiver.Close(context.Background())

		if err != nil {
			panic(err)
		}
	}()

	if err := receiver.IssueCredit(uint32(opts.Count)); err != nil {
		return nil, err
	}

	var msgs []*amqp.Message

	fmt.Printf("Attempting to receive %d messages\n", opts.Count)
	now := time.Now()

	for i := 0; i < opts.Count; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), opts.MaxWaitForMessage)
		msg, err := receiver.Receive(ctx, nil)
		cancel()

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			break
		}

		if err := receiver.AcceptMessage(context.Background(), msg); err != nil {
			return nil, err
		}

		msgs = append(msgs, msg)
	}

	fmt.Printf("Received %d messages in %d seconds\n", opts.Count, time.Since(now)/time.Second)
	return msgs, nil
}

func requireAMQPError(t *testing.T, err error, expectedCond amqp.ErrCond, msgAndArgs ...interface{}) {
	var amqpErr *amqp.Error
	require.ErrorAs(t, err, &amqpErr, msgAndArgs...)
	require.Equal(t, expectedCond, amqpErr.Condition, msgAndArgs...)
}

func sendTestMessage(t *testing.T, clients clients, msg *amqp.Message) error {
	session, err := clients.Conn.NewSession(context.Background(), nil)
	require.NoError(t, err)
	defer testClose(t, session.Close)

	sender, err := session.NewSender(context.Background(), clients.QueueName, nil)
	require.NoError(t, err)

	return sender.Send(context.Background(), msg, nil)
}
