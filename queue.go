package continuesqueue

import (
	"errors"

	"github.com/enriquebris/goconcurrentqueue"
	"go.uber.org/atomic"
)

type generatorFn func(n int) <-chan interface{}

// Queue continues queue
type Queue struct {
	buckets      []goconcurrentqueue.Queue
	totalBuckets int32
	pt           *atomic.Int32
	enqueueLock  *atomic.Bool
	cap          int
	generator    generatorFn
}

// NewQueue returns a Queue pointer
func NewQueue(buckets int, cap int, generator generatorFn) *Queue {
	queue := &Queue{
		pt:           atomic.NewInt32(0),
		buckets:      make([]goconcurrentqueue.Queue, buckets),
		totalBuckets: int32(buckets),
		cap:          cap,
		enqueueLock:  atomic.NewBool(false),
		generator:    generator,
	}
	for i := 0; i < buckets; i++ {
		bucket := goconcurrentqueue.NewFixedFIFO(cap)
		queue.buckets[i] = bucket
	}
	return queue
}

func (q *Queue) EnqueueEqually() {
	if !q.enqueueLock.CAS(false, true) {
		return
	}
	defer q.enqueueLock.Store(false)
	total := int(q.totalBuckets)
	skips := make(map[int]struct{}, total)
	for i := 0; i < q.cap; i++ {
		var idx int
		n := total - len(skips)
		if n <= 0 {
			return
		}
		for v := range q.generator(n) {
			for {
				if _, found := skips[idx]; found {
					idx++
				} else {
					break
				}
			}
			if idx >= total {
				continue
			}
			if err := q.buckets[idx].Enqueue(v); err != nil {
				skips[idx] = struct{}{}
			}
		}
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
		go q.EnqueueEqually()
		q.swap()
		return q.Dequeue()
	}
	return bs
}

// DequeueRetry dequeue element
// retry if pool locked
func (q *Queue) DequeueRetry(retries int) (interface{}, error) {
	if retries <= 0 {
		return nil, errors.New("exceed max retries")
	}
	pt := q.pt.Load()
	if pt >= q.totalBuckets {
		q.swap()
		return q.DequeueRetry(retries)
	}
	bs, err := q.buckets[pt].Dequeue()
	if err != nil {
		go q.EnqueueEqually()
		q.swap()
		return q.DequeueRetry(retries - 1)
	}
	return bs, nil
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
