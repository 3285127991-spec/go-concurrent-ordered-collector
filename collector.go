// Package orderedcollector provides a concurrency-safe collector that
// accepts out-of-order submissions and delivers them strictly in sequence.
package orderedcollector

import (
	"context"
	"errors"
	"sort"
	"sync"
)

var (
	// ErrClosed is returned by Submit and Next after Close, and by Next once
	// no further in-order value can ever be delivered.
	ErrClosed = errors.New("orderedcollector: closed")
	// ErrDuplicate is returned when a sequence number is submitted twice.
	ErrDuplicate = errors.New("orderedcollector: duplicate sequence")
	// ErrStale is returned when a sequence number was already consumed.
	ErrStale = errors.New("orderedcollector: sequence already consumed")
)

// Snapshot is a point-in-time view of a Collector.
type Snapshot struct {
	NextSeq  int   // next sequence number Next will deliver
	Buffered []int // buffered (not yet consumed) sequence numbers, ascending
	Waiting  int   // number of Next calls currently blocked
	Closed   bool  // whether Close has been called
}

// Collector buffers values submitted with a sequence number and hands them
// out strictly in order, starting from the sequence given to New.
type Collector struct {
	mu      sync.Mutex
	nextSeq int
	buf     map[int]any
	waiting int
	closed  bool
	changed chan struct{} // closed and replaced on every state change
}

// New returns a Collector whose first delivered sequence number is start.
func New(start int) *Collector {
	return &Collector{
		nextSeq: start,
		buf:     make(map[int]any),
		changed: make(chan struct{}),
	}
}

// broadcastLocked wakes every blocked Next. Callers must hold c.mu.
func (c *Collector) broadcastLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

// Submit stores value under seq. Values may arrive in any order; each seq
// may be submitted at most once and never after it was consumed.
func (c *Collector) Submit(seq int, value any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if seq < c.nextSeq {
		return ErrStale
	}
	if _, ok := c.buf[seq]; ok {
		return ErrDuplicate
	}
	c.buf[seq] = value
	c.broadcastLocked()
	return nil
}

// Next returns the value for the current expected sequence, blocking until
// it is submitted, ctx is cancelled, or the collector is closed and no
// in-order value remains. Each value is delivered to exactly one caller.
func (c *Collector) Next(ctx context.Context) (any, error) {
	for {
		c.mu.Lock()
		if v, ok := c.buf[c.nextSeq]; ok {
			delete(c.buf, c.nextSeq)
			c.nextSeq++
			c.broadcastLocked()
			c.mu.Unlock()
			return v, nil
		}
		if c.closed {
			c.mu.Unlock()
			return nil, ErrClosed
		}
		ch := c.changed
		c.waiting++
		c.mu.Unlock()

		select {
		case <-ch:
			c.mu.Lock()
			c.waiting--
			c.mu.Unlock()
		case <-ctx.Done():
			c.mu.Lock()
			c.waiting--
			c.mu.Unlock()
			return nil, ctx.Err()
		}
	}
}

// Snapshot returns a consistent point-in-time view of the collector.
func (c *Collector) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := Snapshot{
		NextSeq: c.nextSeq,
		Waiting: c.waiting,
		Closed:  c.closed,
	}
	if len(c.buf) > 0 {
		s.Buffered = make([]int, 0, len(c.buf))
		for seq := range c.buf {
			s.Buffered = append(s.Buffered, seq)
		}
		sort.Ints(s.Buffered)
	}
	return s
}

// Len reports the number of buffered values not yet consumed.
func (c *Collector) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.buf)
}

// Close rejects future Submit calls. Buffered values that remain
// deliverable in order can still be obtained via Next; once the next
// expected sequence can never arrive, Next returns ErrClosed.
func (c *Collector) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	c.broadcastLocked()
}
