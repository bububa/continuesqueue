package continuesqueue

import (
	"errors"
	"time"

	"github.com/enriquebris/goconcurrentqueue"
	"go.uber.org/atomic"
)

type generatorFn func(n int) <-chan interface{}

// Queue continues queue
type Queue struct {
	cap           int
	fillThreshold int
	totalBuckets  int32
	pt            *atomic.Int32
	enqueueLock   *atomic.Bool
	generator     generatorFn
	buckets       []goconcurrentqueue.Queue
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

func (q *Queue) SetFillThreshold(n int) {
	if n >= q.cap {
		n = q.cap - 1
	} else if n < 0 {
		n = 0
	}
	q.fillThreshold = n
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
	defer q.enqueueLock.Store(false)
	total := int(q.totalBuckets)
	skips := make(map[int]struct{}, total)
	// for idx, b := range q.buckets {
	// 	if b.GetLen() > q.fillThreshold {
	// 		skips[idx] = struct{}{}
	// 	}
	// }
	// if total-len(skips) <= 0 {
	// 	return
	// }
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
	var ts time.Time
	ret, _ := q.timeoutDequeue(ts, 0)
	return ret
}

// DequeueWithTimeout .
func (q *Queue) DequeueWithTimeout(timeout time.Duration) (interface{}, error) {
	return q.timeoutDequeue(time.Now(), timeout)
}

func (q *Queue) timeoutDequeue(startTime time.Time, timeout time.Duration) (interface{}, error) {
	if timeout > 0 && time.Since(startTime) > timeout {
		return nil, errors.New("timeout")
	}
	pt := q.pt.Load()
	if pt >= q.totalBuckets {
		return q.timeoutDequeue(startTime, timeout)
	}
	bs, err := q.buckets[pt].Dequeue()
	if err != nil {
		if q.enqueueLock.CAS(false, true) {
			go q.EnqueueEqually()
		}
		q.swap()
		return q.timeoutDequeue(startTime, timeout)
	}
	return bs, nil
}

// Iter iterate elements in the queue
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
