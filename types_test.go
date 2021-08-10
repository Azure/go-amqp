package amqp

import (
	"math"
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
	messageNilDoneChannel := &Message{}
	require.NotPanics(t, func() {
		messageNilDoneChannel.done()
		messageNilDoneChannel.done()
	})

	messageWithDoneChannel := &Message{
		doneSignal: make(chan struct{}, 1),
	}
	require.NotPanics(t, func() {
		messageWithDoneChannel.done()
		messageWithDoneChannel.done()
	})
}
