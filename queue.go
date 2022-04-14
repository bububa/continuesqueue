package continuesqueue

import (
	"github.com/enriquebris/goconcurrentqueue"
	"go.uber.org/atomic"
)

type generatorFn func() <-chan interface{}

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
	for v := range q.generator() {
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
