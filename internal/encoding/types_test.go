package encoding

import (
	"math"
	"testing"

	"github.com/Azure/go-amqp/internal/buffer"
	"github.com/stretchr/testify/require"
)

const amqpArrayHeaderLength = 4

func TestMarshalArrayInt64AsLongArray(t *testing.T) {
	// 244 is larger than a int8 can contain. When it marshals it
	// it'll have to use the typeCodeLong (8 bytes, signed) vs the
	// typeCodeSmalllong (1 byte, signed).
	ai := arrayInt64([]int64{math.MaxInt8 + 1})

	buff := &buffer.Buffer{}
	require.NoError(t, ai.Marshal(buff))
	require.EqualValues(t, amqpArrayHeaderLength+8, buff.Len(), "Expected an AMQP header (4 bytes) + 8 bytes for a long")

	unmarshalled := arrayInt64{}
	require.NoError(t, unmarshalled.Unmarshal(buff))

	require.EqualValues(t, arrayInt64([]int64{math.MaxInt8 + 1}), unmarshalled)
}

func TestMarshalArrayInt64AsSmallLongArray(t *testing.T) {
	// If the values are small enough for a typeCodeSmalllong (1 byte, signed)
	// we can save some space.
	ai := arrayInt64([]int64{math.MaxInt8, math.MinInt8})

	buff := &buffer.Buffer{}
	require.NoError(t, ai.Marshal(buff))
	require.EqualValues(t, amqpArrayHeaderLength+1+1, buff.Len(), "Expected an AMQP header (4 bytes) + 1 byte apiece for the two values")

	unmarshalled := arrayInt64{}
	require.NoError(t, unmarshalled.Unmarshal(buff))

	require.EqualValues(t, arrayInt64([]int64{math.MaxInt8, math.MinInt8}), unmarshalled)
}

func TestDecodeSmallInts(t *testing.T) {
	t.Run("smallong", func(t *testing.T) {
		buff := &buffer.Buffer{}

		v := int8(-1)
		buff.AppendByte(byte(TypeCodeSmalllong))
		buff.AppendByte(byte(v))

		val, err := readLong(buff)
		require.NoError(t, err)
		require.Equal(t, int64(-1), val)
	})

	t.Run("smallint", func(t *testing.T) {
		buff := &buffer.Buffer{}

		v := int8(-1)
		buff.AppendByte(byte(TypeCodeSmallint))
		buff.AppendByte(byte(v))

		val, err := readInt32(buff)
		require.NoError(t, err)
		require.Equal(t, int32(-1), val)
	})
}
