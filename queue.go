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
		generator:    generator,
	}
	for i := 0; i < buckets; i++ {
		bucket := goconcurrentqueue.NewFixedFIFO(cap)
		queue.buckets[i] = bucket
	}
	return queue
}

func (q *Queue) Initiate(vals []interface{}, fill bool) {
	var (
		idx int
		l   = len(vals)
	)
	for bucketID, bucket := range q.buckets {
		var bucketFull bool
		for idx < l {
			if err := bucket.Enqueue(vals[idx]); err != nil {
				bucketFull = true
				break
			}
			idx++
		}
		if bucketFull {
			continue
		} else if idx-bucketID*q.cap < q.cap && fill {
			q.fill(bucketID)
		}
	}
}

func (q *Queue) EnqueueEqually(n int) {
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
		q.swap()
		go q.fill(int(pt))
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
		q.swap()
		go q.fill(int(pt))
		return q.DequeueRetry(retries - 1)
	}
	return bs, nil
}

func (q *Queue) fill(pt int) {
	bucket := q.buckets[pt]
	n := bucket.GetCap() - bucket.GetLen()
	for v := range q.generator(n) {
		bucket.Enqueue(v)
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
