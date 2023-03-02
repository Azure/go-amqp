package amqp

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/fake"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/stretchr/testify/require"
)

func BenchmarkSenderSendSSMUnsettled(b *testing.B) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := senderFrameHandler(SenderSettleModeUnsettled)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch tt := req.(type) {
		case *frames.PerformFlow:
			return nil, nil
		case *frames.PerformDisposition:
			return nil, nil
		case *frames.PerformTransfer:
			return fake.PerformDisposition(encoding.RoleReceiver, 0, *tt.DeliveryID, nil, &encoding.StateAccepted{})
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(b, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(b, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	sndr, err := session.NewSender(ctx, "target", &SenderOptions{
		SettlementMode: SenderSettleModeUnsettled.Ptr(),
	})
	cancel()
	require.NoError(b, err)
	sendInitialFlowFrame(b, conn, 0, 1000000)
	b.ResetTimer()
	b.ReportAllocs()

	msg := NewMessage([]byte("test"))
	for i := 0; i < b.N; i++ {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		err = sndr.Send(ctx, msg, nil)
		cancel()
		require.NoError(b, err)
	}
}

func BenchmarkSenderSendSSMSettled(b *testing.B) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := senderFrameHandler(SenderSettleModeSettled)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch req.(type) {
		case *frames.PerformFlow:
			return nil, nil
		case *frames.PerformDisposition:
			return nil, nil
		case *frames.PerformTransfer:
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(b, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(b, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	sndr, err := session.NewSender(ctx, "target", &SenderOptions{
		SettlementMode: SenderSettleModeSettled.Ptr(),
	})
	cancel()
	require.NoError(b, err)
	sendInitialFlowFrame(b, conn, 0, 1000000)
	b.ResetTimer()
	b.ReportAllocs()

	msg := NewMessage([]byte("test"))
	for i := 0; i < b.N; i++ {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		err = sndr.Send(ctx, msg, nil)
		cancel()
		require.NoError(b, err)
	}
}

func BenchmarkReceiverReceiveRSMFirst(b *testing.B) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ReceiverSettleModeFirst)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch req.(type) {
		case *frames.PerformFlow:
			return nil, nil
		case *frames.PerformDisposition:
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(b, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(b, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	rcvr, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeFirst.Ptr(),
	})
	cancel()
	require.NoError(b, err)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		fr, err := fake.PerformTransfer(0, 0, uint32(i), []byte{})
		require.NoError(b, err)
		conn.SendFrame(fr)
		b.StartTimer()

		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		_, err = rcvr.Receive(ctx, nil)
		cancel()
		require.NoError(b, err)
	}
}

func BenchmarkReceiverReceiveRSMSecond(b *testing.B) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ReceiverSettleModeSecond)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch req.(type) {
		case *frames.PerformFlow:
			return nil, nil
		case *frames.PerformDisposition:
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(b, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(b, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	rcvr, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeSecond.Ptr(),
	})
	cancel()
	require.NoError(b, err)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		fr, err := fake.PerformTransfer(0, 0, uint32(i), []byte{})
		require.NoError(b, err)
		conn.SendFrame(fr)
		b.StartTimer()

		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		_, err = rcvr.Receive(ctx, nil)
		cancel()
		require.NoError(b, err)
	}
}

func BenchmarkReceiverSettleMessage(b *testing.B) {
	responder := func(req frames.FrameBody) ([]byte, error) {
		b, err := receiverFrameHandler(ReceiverSettleModeFirst)(req)
		if b != nil || err != nil {
			return b, err
		}
		switch req.(type) {
		case *frames.PerformFlow:
			return nil, nil
		case *frames.PerformDisposition:
			return nil, nil
		default:
			return nil, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(b, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(b, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	rcvr, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(b, err)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		require.NoError(b, rcvr.AcceptMessage(ctx, &Message{deliveryID: 0}))
		cancel()
	}
}
