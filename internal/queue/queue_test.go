package queue

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueueBasic(t *testing.T) {
	q := New[string](5)
	require.NotNil(t, q)

	v := q.Dequeue()
	require.Nil(t, v)
	require.Zero(t, q.head)
	require.Zero(t, q.tail)

	const one = "one"
	q.Enqueue(one)
	require.EqualValues(t, 1, q.tail)
	require.EqualValues(t, 1, q.Len())
	v = q.Dequeue()
	require.NotNil(t, v)
	require.Zero(t, q.Len())
	require.Zero(t, q.tail)
	require.EqualValues(t, one, *v)

	v = q.Dequeue()
	require.Empty(t, v)
	require.Nil(t, v)

	const two = "two"
	q.Enqueue(one)
	q.Enqueue(two)
	require.EqualValues(t, 2, q.Len())
	require.EqualValues(t, 2, q.tail)

	v = q.Dequeue()
	require.NotNil(t, v)
	require.EqualValues(t, 1, q.head)
	require.EqualValues(t, 2, q.tail)
	require.EqualValues(t, one, *v)

	v = q.Dequeue()
	require.NotNil(t, v)
	require.Zero(t, q.head)
	require.Zero(t, q.tail)
	require.EqualValues(t, two, *v)
}

func TestQueueNewSeg(t *testing.T) {
	const size = 5
	q := New[int](size)
	require.NotNil(t, q)

	for i := 0; i < size; i++ {
		q.Enqueue(i + 1)
	}

	// fill up first segment, verify there's no new segment
	require.Zero(t, q.head)
	require.Equal(t, size, q.tail)
	require.Nil(t, q.next)
	require.EqualValues(t, size, q.Len())

	// with first segment full, a new one is created
	q.Enqueue(6)
	require.NotNil(t, q.next)
	require.EqualValues(t, 6, q.Len())

	// dequeue the first three items
	for i := 0; i < 3; i++ {
		val := q.Dequeue()
		require.NotNil(t, val)
		require.EqualValues(t, i+1, *val)
	}
	// should be two items left
	require.EqualValues(t, size-2, q.head)
	require.EqualValues(t, 3, q.Len())

	// enqueue another item, first segment is undisturbed
	q.Enqueue(7)
	// should be two items left
	require.EqualValues(t, size-2, q.head)
	require.EqualValues(t, 4, q.Len())

	// now dequeue remaining items in first segment
	for i := 0; i < 2; i++ {
		val := q.Dequeue()
		require.NotNil(t, val)
		require.EqualValues(t, i+4, *val)
	}

	// first segement is now empty
	require.Zero(t, q.head)
	require.Zero(t, q.tail)
	require.EqualValues(t, 2, q.Len())

	// enqueueing another value should not touch first segment
	q.Enqueue(8)
	require.Zero(t, q.head)
	require.Zero(t, q.tail)
	require.EqualValues(t, 3, q.Len())

	// dequeue items from second segment
	for i := 0; i < 2; i++ {
		val := q.Dequeue()
		require.NotNil(t, val)
		require.EqualValues(t, 6+i, *val)
	}
	require.EqualValues(t, 2, q.next.head)
	require.EqualValues(t, 3, q.next.tail)
	require.EqualValues(t, 1, q.Len())

	// dequeue last item
	val := q.Dequeue()
	require.NotNil(t, val)
	require.EqualValues(t, 8, *val)

	// both segments are empty
	require.Zero(t, q.head)
	require.Zero(t, q.tail)
	require.Zero(t, q.next.head)
	require.Zero(t, q.next.tail)
	require.Zero(t, q.Len())
}
