package continuesqueue

import (
	"github.com/enriquebris/goconcurrentqueue"
	"go.uber.org/atomic"
)

type sourceFn func() []interface{}

// Queue continues queue
type Queue struct {
	buckets      []goconcurrentqueue.Queue
	totalBuckets int32
	pt           *atomic.Int32
	source       sourceFn
}

// NewQueue returns a Queue pointer
func NewQueue(buckets int, cap int, source sourceFn) *Queue {
	queue := &Queue{
		pt:           atomic.NewInt32(0),
		buckets:      make([]goconcurrentqueue.Queue, buckets),
		source:       source,
		totalBuckets: int32(buckets),
	}
	for i := 0; i < buckets; i++ {
		bucket := goconcurrentqueue.NewFixedFIFO(cap)
		queue.buckets[i] = bucket
	}
	return queue
}

func (q *Queue) Initiate() {
	for idx := range q.buckets {
		q.fill(idx)
	}
}

func (q *Queue) swap() {
	newVal := q.pt.Inc()
	if newVal >= q.totalBuckets {
		q.pt.Store(0)
	}
}

// Dequeue dequeue element
// retry if pool locked
func (q *Queue) Dequeue() interface{} {
	pt := q.pt.Load()
	if pt >= q.totalBuckets {
		q.swap()
		return q.Dequeue()
	}
	bs, err := q.buckets[pt].Dequeue()
	if err != nil {
		q.swap()
		go q.fill(int(pt))
		return q.Dequeue()
	}
	return bs
}

func (q *Queue) fill(pt int) {
	items := q.source()
	for _, v := range items {
		q.buckets[pt].Enqueue(v)
	}
}

func (q *Queue) Iter() <-chan interface{} {
	ch := make(chan interface{})
	go func() {
		for _, bucket := range q.buckets {
			for {
				if val, err := bucket.Dequeue(); err != nil {
					break
				} else {
					ch <- val
				}
			}
		}
		close(ch)
	}()
	return ch
}
