package queue

import (
	"container/ring"
)

// Queue[T] is a segmented FIFO queue of Ts.
type Queue[T any] struct {
	head *ring.Ring
	tail *ring.Ring
	size int
}

// New creates a new instance of Queue[T].
//   - size is the size of each Queue segment
func New[T any](size int) *Queue[T] {
	r := &ring.Ring{
		Value: &segment[T]{
			items: make([]*T, size),
		},
	}
	return &Queue[T]{
		head: r,
		tail: r,
	}
}

// Enqueue adds the specified item to the end of the queue.
// If the current segment is full, a new segment is created.
func (q *Queue[T]) Enqueue(item T) {
	for {
		r := q.tail
		seg := r.Value.(*segment[T])

		if seg.tail < len(seg.items) {
			seg.items[seg.tail] = &item
			seg.tail++
			q.size++
			return
		}

		// segment is full, can we advance?
		if next := r.Next(); next != q.head {
			q.tail = next
			continue
		}

		// no, add a new ring
		r.Link(&ring.Ring{
			Value: &segment[T]{
				items: make([]*T, len(seg.items)),
			},
		})

		q.tail = r.Next()
	}
}

// Dequeue removes and returns the item from the front of the queue.
func (q *Queue[T]) Dequeue() *T {
	r := q.head
	seg := r.Value.(*segment[T])

	if seg.tail == 0 {
		// queue is empty
		return nil
	}

	// remove first item
	item := seg.items[seg.head]
	seg.items[seg.head] = nil
	seg.head++
	q.size--

	if seg.head == seg.tail {
		// segment is now empty, reset indices
		seg.head, seg.tail = 0, 0

		// if we're not at the last ring, advance head to the next one
		if next := r.Next(); next != q.head {
			q.head = next
		} else {
			// this is the last segment in the ring and it's empty, so the
			// entire queue is empty. reset head and tail to this segment.
			q.head, q.tail = r, r
		}
	}

	return item
}

// Len returns the total count of enqueued items.
func (q *Queue[T]) Len() int {
	return q.size
}

type segment[T any] struct {
	items []*T
	head  int
	tail  int
}
