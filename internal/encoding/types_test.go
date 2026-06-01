package encoding

import (
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/Azure/go-amqp/internal/buffer"
	"github.com/stretchr/testify/require"
)

const amqpArrayHeaderLength = 4

func appendUint32BE(b []byte, v uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	return append(b, buf[:]...)
}

func TestEncodeDecodeTimestamp(t *testing.T) {
	// this is DateTime.MaxValue from .NET
	dotnetMaxTime := time.UnixMilli(int64(253402300799999)).UTC()
	require.Equal(t, "9999-12-31T23:59:59Z", dotnetMaxTime.Format(time.RFC3339))

	// previously we were using the < go1.17 method that involved converting our milliseconds
	// into nanoseconds, but that'll overflow an int64 in valid cases, like the .NET DateTime.MaxValue,
	// which is often used as a sentinel value in brokers like Service Bus or Event Hubs.
	buff := buffer.New(nil)
	writeTimestamp(buff, dotnetMaxTime)

	decodedTimestamp, err := readTimestamp(buff)
	require.NoError(t, err)

	require.Equal(t, "9999-12-31T23:59:59Z", decodedTimestamp.UTC().Format(time.RFC3339))
}

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

func TestArray32CountExceedingMaxIsRejected(t *testing.T) {
	// Array32 with count 0x7FFFFFFF. Body padding makes the size check
	// pass, so the count cap is what must fire.
	const size = 9
	frame := make([]byte, 0, 1+4+size)
	frame = append(frame, byte(TypeCodeArray32))
	frame = appendUint32BE(frame, uint32(size))
	frame = appendUint32BE(frame, 0x7fffffff)        // pretend our zero-width array is HUGE
	frame = append(frame, byte(TypeCodeNull))        // use a zero-width 'constructor' for the array
	frame = append(frame, make([]byte, size-4-1)...) // note that the actual bytes sent is basically zero, for the body of the array. Totally legal, but would have caused us to allocate a 'count' sized array to represent it.

	buff := buffer.New(frame)
	_, err := readArrayHeader(buff)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds maximum")
}

func TestArray32CountExceedingBodyIsRejected(t *testing.T) {
	// count=100 is below maxCompoundCount but exceeds the body budget
	// after the count field. Zero-width Null element mirrors the
	// exploit shape the body-length check targets.
	const size = 10
	const count = 100
	frame := make([]byte, 0, 1+4+size)
	frame = append(frame, byte(TypeCodeArray32))
	frame = appendUint32BE(frame, uint32(size))
	frame = appendUint32BE(frame, uint32(count))
	frame = append(frame, byte(TypeCodeNull))
	frame = append(frame, make([]byte, size-4-1)...) // pad to declared size
	buff := buffer.New(frame)

	_, err := readArrayHeader(buff)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds body length")
}

func TestArray32AtMaxCountSucceeds(t *testing.T) {
	// Exactly maxCompoundCount elements, each one byte
	// (TypeCodeSmallUint). Header should decode cleanly.
	const count = 65536
	const size = 4 + 1 + count
	frame := make([]byte, 0, 1+4+size)
	frame = append(frame, byte(TypeCodeArray32))
	frame = appendUint32BE(frame, uint32(size))
	frame = appendUint32BE(frame, uint32(count))
	frame = append(frame, byte(TypeCodeSmallUint))
	frame = append(frame, make([]byte, count)...)
	buff := buffer.New(frame)

	n, err := readArrayHeader(buff)
	require.NoError(t, err)
	require.EqualValues(t, count, n)
}

func TestArray8CountChecksApplied(t *testing.T) {
	// Array8's max count is 255, so maxCompoundCount is a no-op here;
	// the body-length check is what must reject count=200 with only
	// 4 bytes of body remaining.
	frame := []byte{
		byte(TypeCodeArray8),
		0x05, // size: 1 (count) + 1 (constructor) + 3 (padding)
		200,  // count
		byte(TypeCodeNull),
		0x00, 0x00, 0x00, // padding to declared size
	}
	buff := buffer.New(frame)
	_, err := readArrayHeader(buff)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds body length")
}

func TestList32CountBoundsChecked(t *testing.T) {
	// count 0x7FFFFFFF on a List32 must be rejected by the cap.
	const size = 9
	frame := make([]byte, 0, 1+4+size)
	frame = append(frame, byte(TypeCodeList32))
	frame = appendUint32BE(frame, uint32(size))
	frame = appendUint32BE(frame, 0x7fffffff)
	frame = append(frame, make([]byte, size-4)...)

	buff := buffer.New(frame)
	_, err := readListHeader(buff)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds maximum")
}

func TestMap32CountBoundsChecked(t *testing.T) {
	// count 0x7FFFFFFF on a Map32 must be rejected by the cap.
	const size = 9
	frame := make([]byte, 0, 1+4+size)
	frame = append(frame, byte(TypeCodeMap32))
	frame = appendUint32BE(frame, uint32(size))
	frame = appendUint32BE(frame, 0x7fffffff)
	frame = append(frame, make([]byte, size-4)...)

	buff := buffer.New(frame)
	_, err := readMapHeader(buff)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds maximum")
}
