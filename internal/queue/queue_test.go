package queue

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueueBasic(t *testing.T) {
	q := New[string](5)
	require.NotNil(t, q)
	require.Zero(t, q.Len())
	require.Same(t, q.head, q.tail)
	require.EqualValues(t, 1, q.head.Len())

	v := q.Dequeue()
	require.Nil(t, v)
	require.Zero(t, q.Len())
	require.Same(t, q.head, q.tail)
	require.EqualValues(t, 1, q.head.Len())

	const one = "one"
	q.Enqueue(one)
	require.EqualValues(t, 1, q.Len())
	require.Same(t, q.head, q.tail)
	require.EqualValues(t, 1, q.head.Len())
	seg, ok := q.head.Value.(*segment[string])
	require.True(t, ok)
	require.EqualValues(t, 0, seg.head)
	require.EqualValues(t, 1, seg.tail)

	v = q.Dequeue()
	require.NotNil(t, v)
	require.EqualValues(t, one, *v)
	require.Zero(t, q.Len())
	require.Same(t, q.head, q.tail)
	require.EqualValues(t, 1, q.head.Len())
	seg, ok = q.head.Value.(*segment[string])
	require.True(t, ok)
	require.EqualValues(t, 0, seg.head)
	require.EqualValues(t, 0, seg.tail)

	v = q.Dequeue()
	require.Nil(t, v)
	require.EqualValues(t, 1, q.head.Len())

	const two = "two"
	q.Enqueue(one)
	q.Enqueue(two)
	require.EqualValues(t, 2, q.Len())
	require.Same(t, q.head, q.tail)
	require.EqualValues(t, 1, q.head.Len())
	seg, ok = q.head.Value.(*segment[string])
	require.True(t, ok)
	require.EqualValues(t, 0, seg.head)
	require.EqualValues(t, 2, seg.tail)

	v = q.Dequeue()
	require.NotNil(t, v)
	require.EqualValues(t, one, *v)
	require.EqualValues(t, 1, q.head.Len())
	seg, ok = q.head.Value.(*segment[string])
	require.True(t, ok)
	require.EqualValues(t, 1, seg.head)
	require.EqualValues(t, 2, seg.tail)

	v = q.Dequeue()
	require.NotNil(t, v)
	require.EqualValues(t, two, *v)
	require.EqualValues(t, 1, q.head.Len())
	seg, ok = q.head.Value.(*segment[string])
	require.True(t, ok)
	require.EqualValues(t, 0, seg.head)
	require.EqualValues(t, 0, seg.tail)
}

func TestQueueNewSeg(t *testing.T) {
	const size = 5
	q := New[int](size)
	require.NotNil(t, q)

	// fill up first segment
	for i := 0; i < size; i++ {
		q.Enqueue(i + 1)
	}

	// verify there's no new segment
	require.EqualValues(t, size, q.Len())
	require.Same(t, q.head, q.tail)
	seg, ok := q.head.Value.(*segment[int])
	require.True(t, ok)
	require.EqualValues(t, 0, seg.head)
	require.EqualValues(t, size, seg.tail)

	// with first segment full, a new one is created
	q.Enqueue(6)
	require.EqualValues(t, size+1, q.Len())
	require.NotSame(t, q.head, q.tail)
	require.EqualValues(t, 2, q.head.Len())

	// first segment undisturbed
	seg, ok = q.head.Value.(*segment[int])
	require.True(t, ok)
	require.EqualValues(t, 0, seg.head)
	require.EqualValues(t, size, seg.tail)

	// second segment contains one item
	seg, ok = q.tail.Value.(*segment[int])
	require.True(t, ok)
	require.EqualValues(t, 0, seg.head)
	require.EqualValues(t, 1, seg.tail)

	// new segment properly linked
	require.EqualValues(t, q.tail, q.head.Next())
	require.EqualValues(t, q.head, q.tail.Next())

	// dequeue the first three items
	for i := 0; i < 3; i++ {
		val := q.Dequeue()
		require.NotNil(t, val)
		require.EqualValues(t, i+1, *val)
	}
	require.EqualValues(t, 3, q.Len())

	// should be two items left
	seg, ok = q.head.Value.(*segment[int])
	require.True(t, ok)
	require.EqualValues(t, size-2, seg.head)
	require.EqualValues(t, size, seg.tail)

	// enqueue another item
	q.Enqueue(7)
	require.EqualValues(t, 4, q.Len())

	// first segment is undisturbed (still two items)
	seg, ok = q.head.Value.(*segment[int])
	require.True(t, ok)
	require.EqualValues(t, size-2, seg.head)
	require.EqualValues(t, size, seg.tail)

	// now there are two items in the second segment
	seg, ok = q.tail.Value.(*segment[int])
	require.True(t, ok)
	require.EqualValues(t, 0, seg.head)
	require.EqualValues(t, 2, seg.tail)

	// now dequeue remaining items in first segment
	for i := 0; i < 2; i++ {
		val := q.Dequeue()
		require.NotNil(t, val)
		require.EqualValues(t, i+4, *val)
	}
	require.EqualValues(t, 2, q.Len())
	require.EqualValues(t, 2, q.head.Len())

	// first segement is now empty and q.head has advanced
	require.Same(t, q.head, q.tail)
	seg, ok = q.tail.Prev().Value.(*segment[int])
	require.True(t, ok)
	require.EqualValues(t, 0, seg.head)
	require.EqualValues(t, 0, seg.tail)

	// enqueueing another value
	q.Enqueue(8)
	require.EqualValues(t, 3, q.Len())
	require.Same(t, q.head, q.tail)
	seg, ok = q.head.Value.(*segment[int])
	require.True(t, ok)
	require.EqualValues(t, 0, seg.head)
	require.EqualValues(t, 3, seg.tail)

	// should not touch first (empty) segment
	seg, ok = q.head.Prev().Value.(*segment[int])
	require.True(t, ok)
	require.EqualValues(t, 0, seg.head)
	require.EqualValues(t, 0, seg.tail)

	// dequeue items from second segment (one left)
	for i := 0; i < 2; i++ {
		val := q.Dequeue()
		require.NotNil(t, val)
		require.EqualValues(t, 6+i, *val)
	}
	require.EqualValues(t, 1, q.Len())

	seg, ok = q.head.Value.(*segment[int])
	require.True(t, ok)
	require.EqualValues(t, 2, seg.head)
	require.EqualValues(t, 3, seg.tail)

	// dequeue last item
	val := q.Dequeue()
	require.NotNil(t, val)
	require.EqualValues(t, 8, *val)
	require.Zero(t, q.Len())
	require.Same(t, q.head, q.tail)

	// both segments are empty
	seg, ok = q.head.Value.(*segment[int])
	require.True(t, ok)
	require.EqualValues(t, 0, seg.head)
	require.EqualValues(t, 0, seg.tail)
	seg, ok = q.head.Next().Value.(*segment[int])
	require.True(t, ok)
	require.EqualValues(t, 0, seg.head)
	require.EqualValues(t, 0, seg.tail)
	require.EqualValues(t, 2, q.head.Len())
}

func TestManyItemsStableRingCount(t *testing.T) {
	const (
		items   = 5000
		factor  = 10
		segSize = items / factor
	)

	q := New[int](segSize)
	for n := 0; n < 100; n++ {
		for i := 0; i < items; i++ {
			q.Enqueue(i)

			if n == 0 && i > 0 && i%segSize == 0 {
				// on the first iteration, for every seg-size inserts, we should see a new segment
				require.EqualValues(t, (i/segSize)+1, q.head.Len())
			}
		}
		require.Equal(t, items, q.Len())
		require.Equal(t, factor, q.head.Len())

		for i := 0; i < items; i++ {
			v := q.Dequeue()
			require.NotNil(t, v)
			require.EqualValues(t, i, *v)
		}
		require.Zero(t, q.Len())
		require.Equal(t, factor, q.head.Len())

		require.Same(t, q.head, q.tail)
	}
}
