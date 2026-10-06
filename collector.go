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
	// ErrClosed is returned by Submit after Close, and by Next when the
	// collector is closed and no further in-order value can be delivered.
	ErrClosed = errors.New("orderedcollector: closed")
	// ErrDuplicate is returned when seq is already buffered.
	ErrDuplicate = errors.New("orderedcollector: duplicate sequence")
	// ErrExpired is returned when seq has already been consumed.
	ErrExpired = errors.New("orderedcollector: sequence already consumed")
)

// Snapshot is a point-in-time copy of the collector state.
type Snapshot struct {
	NextSeq  int   // next sequence number Next will deliver
	Buffered []int // buffered (not yet consumed) sequences, ascending
	Waiting  int   // number of Next calls currently blocked
	Closed   bool
}

// Collector buffers values submitted with a sequence number and delivers
// them strictly in order: start, start+1, start+2, ...
// The zero value is not usable; create one with New.
type Collector[T any] struct {
	mu      sync.Mutex
	nextSeq int
	buf     map[int]T
	closed  bool
	waiting int
	changed chan struct{} // closed and replaced on every state change
}

// New returns a Collector whose first delivered sequence is start.
func New[T any](start int) *Collector[T] {
	return &Collector[T]{
		nextSeq: start,
		buf:     make(map[int]T),
		changed: make(chan struct{}),
	}
}

// Submit buffers value under seq. Values may arrive out of order.
// It fails with ErrClosed after Close, ErrExpired if seq was already
// consumed, or ErrDuplicate if seq is already buffered.
func (c *Collector[T]) Submit(seq int, value T) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if seq < c.nextSeq {
		return ErrExpired
	}
	if _, ok := c.buf[seq]; ok {
		return ErrDuplicate
	}
	c.buf[seq] = value
	c.broadcastLocked()
	return nil
}

// Next returns the value for the next expected sequence, blocking until it
// is submitted, ctx is cancelled, or the collector is closed and drained.
// Each value is delivered to exactly one Next call.
func (c *Collector[T]) Next(ctx context.Context) (T, error) {
	c.mu.Lock()
	for {
		if value, ok := c.buf[c.nextSeq]; ok {
			delete(c.buf, c.nextSeq)
			c.nextSeq++
			c.mu.Unlock()
			return value, nil
		}
		if c.closed {
			c.mu.Unlock()
			var zero T
			return zero, ErrClosed
		}
		changed := c.changed
		c.waiting++
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			// Only this call is removed. If the value became available
			// concurrently, either outcome is allowed; we report cancel
			// and leave the value for another caller.
			c.mu.Lock()
			c.waiting--
			c.mu.Unlock()
			var zero T
			return zero, ctx.Err()
		case <-changed:
			c.mu.Lock()
			c.waiting--
		}
	}
}

// Snapshot returns a consistent copy of the current state.
func (c *Collector[T]) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	buffered := make([]int, 0, len(c.buf))
	for seq := range c.buf {
		buffered = append(buffered, seq)
	}
	sort.Ints(buffered)
	return Snapshot{
		NextSeq:  c.nextSeq,
		Buffered: buffered,
		Waiting:  c.waiting,
		Closed:   c.closed,
	}
}

// Len reports the number of buffered, not yet consumed values.
func (c *Collector[T]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.buf)
}

// Close rejects further Submit calls. Buffered values remain drainable in
// order; once the next expected sequence cannot be delivered, Next returns
// ErrClosed. Close is idempotent.
func (c *Collector[T]) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	c.broadcastLocked()
}

// broadcastLocked wakes all blocked Next calls. c.mu must be held.
func (c *Collector[T]) broadcastLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}
