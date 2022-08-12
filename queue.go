package continuesqueue

import (
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
	enqueueCh    chan struct{}
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
		enqueueCh:    make(chan struct{}),
	}
	for i := 0; i < buckets; i++ {
		bucket := goconcurrentqueue.NewFixedFIFO(cap)
		queue.buckets[i] = bucket
	}
	go queue.start()
	return queue
}

func (q *Queue) start() {
	for range q.enqueueCh {
		if q.enqueueLock.CAS(false, true) {
			q.EnqueueEqually()
			q.enqueueLock.Store(false)
		}
	}
}

func (q *Queue) Close() {
	close(q.enqueueCh)
}

func (q *Queue) Fill(list []interface{}) {
	if !q.enqueueLock.CAS(false, true) {
		return
	}
	defer q.enqueueLock.Store(false)
	for _, v := range list {
		for _, bucket := range q.buckets {
			bucket.Enqueue(v)
		}
	}
}

func (q *Queue) EnqueueEqually() {
	total := int(q.totalBuckets)
	skips := make(map[int]struct{}, total)
	for idx, b := range q.buckets {
		if b.GetLen() > 0 {
			skips[idx] = struct{}{}
		}
	}
	if total-len(skips) <= 0 {
		return
	}
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
			idx++
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
	q.swap()
	pt := q.pt.Load()
	if pt >= q.totalBuckets {
		return q.Dequeue()
	}
	bs, err := q.buckets[pt].Dequeue()
	if err != nil {
		q.enqueueCh <- struct{}{}
		return q.Dequeue()
	}
	return bs
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
