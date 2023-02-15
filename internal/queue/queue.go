package queue

// Queue[T] is a segmented FIFO queue of Ts.
type Queue[T any] struct {
	next  *Queue[T]
	items []*T
	head  int
	tail  int
}

// New creates a new instance of Queue[T].
//   - size is the size of each Queue segment
func New[T any](size int) *Queue[T] {
	return &Queue[T]{
		items: make([]*T, size),
	}
}

// Enqueue adds the specified item to the end of the queue.
// If the current segment is full, a new segment is created.
func (q *Queue[T]) Enqueue(item T) {
	cur := q
	for {
		// always enqueue to the last segment
		if cur.next != nil {
			cur = cur.next
			continue
		}

		if cur.tail < len(cur.items) {
			cur.items[cur.tail] = &item
			cur.tail++
			return
		}

		// no free segment, allocate and enqueue
		break
	}

	cur.next = &Queue[T]{
		items: make([]*T, len(cur.items)),
	}
	cur.next.Enqueue(item)
}

// Dequeue removes and returns the item from the front of the queue.
func (q *Queue[T]) Dequeue() *T {
	if q.head == q.tail {
		if q.next != nil {
			// try next segment
			return q.next.Dequeue()
		}

		// empty
		return nil
	}

	item := q.items[q.head]
	q.head++
	if q.head == q.tail {
		// segment is now empty, reset indices
		q.head, q.tail = 0, 0
	}

	return item
}

// Len returns the total count of enqueued items.
func (q *Queue[T]) Len() int {
	var size int
	for cur := q; cur != nil; cur = cur.next {
		size += cur.tail - cur.head
	}
	return size
}
