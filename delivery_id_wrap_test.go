// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package amqp

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/fake"
	"github.com/stretchr/testify/require"
)

// Delivery IDs are RFC 1982 serial numbers and wrap from math.MaxUint32 to 0
// once a session has carried 2^32 transfers.

func TestReceiverAcceptLastDeliveryIDBeforeWrap(t *testing.T) {
	const linkHandle = uint32(0)
	conn := fake.NewNetConn(receiverOnDeliveryStateChangedResponder(t, ReceiverSettleModeFirst), fake.NetConnOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := NewConn(ctx, conn, nil)
	require.NoError(t, err)
	session, err := client.NewSession(ctx, nil)
	require.NoError(t, err)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeFirst.Ptr(),
		Credit:         2,
	})
	require.NoError(t, err)

	for _, id := range []uint32{math.MaxUint32, 0} {
		b, err := fake.PerformTransfer(0, linkHandle, id, []byte("hello"))
		require.NoError(t, err)
		conn.SendFrame(b)
		msg, err := r.Receive(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, r.AcceptMessage(ctx, msg), "accepting delivery %d", id)
	}
	require.NoError(t, client.Close())
}

func TestReceiverDispositionRangeAcrossWrap(t *testing.T) {
	const linkHandle = uint32(0)
	ids := []uint32{math.MaxUint32 - 1, math.MaxUint32, 0, 1}

	conn := fake.NewNetConn(receiverOnDeliveryStateChangedResponder(t, ReceiverSettleModeFirst), fake.NetConnOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := NewConn(ctx, conn, nil)
	require.NoError(t, err)
	session, err := client.NewSession(ctx, nil)
	require.NoError(t, err)

	states := make(chan DeliveryState, len(ids))
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeFirst.Ptr(),
		Credit:         int32(len(ids)),
		OnDeliveryStateChanged: func(msg *Message, state DeliveryState) {
			states <- state
		},
	})
	require.NoError(t, err)

	for _, id := range ids {
		b, err := fake.PerformTransfer(0, linkHandle, id, []byte("hello"))
		require.NoError(t, err)
		conn.SendFrame(b)
	}
	for range ids {
		_, err := r.Receive(ctx, nil)
		require.NoError(t, err)
	}

	// one disposition covering MaxUint32-1, MaxUint32, 0, 1
	last := ids[len(ids)-1]
	b, err := fake.PerformDisposition(encoding.RoleSender, 0, ids[0], &last, &encoding.StateModified{DeliveryFailed: true})
	require.NoError(t, err)
	conn.SendFrame(b)

	for i := range ids {
		select {
		case state := <-states:
			require.IsType(t, &StateModified{}, state)
		case <-time.After(2 * time.Second):
			t.Fatalf("got %d of %d OnDeliveryStateChanged callbacks", i, len(ids))
		}
	}
	require.NoError(t, client.Close())
}

func TestDeliveryIDCount(t *testing.T) {
	u32 := func(v uint32) *uint32 { return &v }
	tests := []struct {
		name  string
		first uint32
		last  *uint32
		want  uint32
	}{
		{"single, no last", 7, nil, 1},
		{"single, last equals first", 7, u32(7), 1},
		{"normal range", 3, u32(5), 3},
		{"last ID before wrap", math.MaxUint32, nil, 1},
		{"last ID before wrap, last set", math.MaxUint32, u32(math.MaxUint32), 1},
		{"range ending at MaxUint32", math.MaxUint32 - 2, u32(math.MaxUint32), 3},
		{"range across wrap", math.MaxUint32 - 1, u32(1), 4},
		{"last precedes first", 5, u32(3), 0},
		{"last precedes first across wrap", 1, u32(math.MaxUint32), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, deliveryIDCount(tt.first, tt.last))
		})
	}
}
