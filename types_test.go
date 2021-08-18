package amqp

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

const amqpArrayHeaderLength = 4

func TestMarshalArrayInt64AsLongArray(t *testing.T) {
	// 244 is larger than a int8 can contain. When it marshals it
	// it'll have to use the typeCodeLong (8 bytes, signed) vs the
	// typeCodeSmalllong (1 byte, signed).
	ai := arrayInt64([]int64{math.MaxInt8 + 1})

	buff := &buffer{}
	require.NoError(t, ai.marshal(buff))
	require.EqualValues(t, amqpArrayHeaderLength+8, buff.len(), "Expected an AMQP header (4 bytes) + 8 bytes for a long")

	unmarshalled := arrayInt64{}
	require.NoError(t, unmarshalled.unmarshal(buff))

	require.EqualValues(t, arrayInt64([]int64{math.MaxInt8 + 1}), unmarshalled)
}

func TestMarshalArrayInt64AsSmallLongArray(t *testing.T) {
	// If the values are small enough for a typeCodeSmalllong (1 byte, signed)
	// we can save some space.
	ai := arrayInt64([]int64{math.MaxInt8, math.MinInt8})

	buff := &buffer{}
	require.NoError(t, ai.marshal(buff))
	require.EqualValues(t, amqpArrayHeaderLength+1+1, buff.len(), "Expected an AMQP header (4 bytes) + 1 byte apiece for the two values")

	unmarshalled := arrayInt64{}
	require.NoError(t, unmarshalled.unmarshal(buff))

	require.EqualValues(t, arrayInt64([]int64{math.MaxInt8, math.MinInt8}), unmarshalled)
}

func TestMessageCallDoneMultipleTimes(t *testing.T) {
	tests := []struct {
		name       string
		message    *Message
		iterations int
	}{
		{
			name:       "Channel Not Initialized",
			message:    &Message{},
			iterations: 100,
		},
		{
			name: "Channel Initialized",
			message: &Message{
				doneSignal: make(chan struct{}, 1),
			},
			iterations: 100,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				g := sync.WaitGroup{}
				for i := 0; i < test.iterations; i++ {
					go func(waitID int) {
						g.Add(1)
						test.message.done()
						g.Done()
					}(i)
				}
				g.Wait()
			})
		})
	}
}
