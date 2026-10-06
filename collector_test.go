package orderedcollector_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"testing"

	"orderedcollector"
)

type nextResult struct {
	v   any
	err error
}

// waitFor spins until cond holds, without sleeping.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 100000; i++ {
		if cond() {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestOutOfOrderSubmitInOrderConsume(t *testing.T) {
	c := orderedcollector.New(1)
	defer c.Close()
	for _, seq := range []int{3, 1, 2} {
		if err := c.Submit(seq, fmt.Sprintf("v%d", seq)); err != nil {
			t.Fatalf("Submit(%d): %v", seq, err)
		}
	}
	for _, want := range []string{"v1", "v2", "v3"} {
		got, err := c.Next(context.Background())
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if got != want {
			t.Fatalf("Next = %v, want %v", got, want)
		}
	}
}

func TestDuplicateAndStaleSeq(t *testing.T) {
	c := orderedcollector.New(5)
	defer c.Close()
	if err := c.Submit(4, "v4"); !errors.Is(err, orderedcollector.ErrStale) {
		t.Fatalf("Submit(4) = %v, want ErrStale", err)
	}
	if err := c.Submit(5, "v5"); err != nil {
		t.Fatalf("Submit(5): %v", err)
	}
	if err := c.Submit(5, "again"); !errors.Is(err, orderedcollector.ErrDuplicate) {
		t.Fatalf("re-Submit(5) = %v, want ErrDuplicate", err)
	}
	if _, err := c.Next(context.Background()); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if err := c.Submit(5, "again"); !errors.Is(err, orderedcollector.ErrStale) {
		t.Fatalf("Submit(5) after consume = %v, want ErrStale", err)
	}
}

func TestContiguousBufferedAdvance(t *testing.T) {
	c := orderedcollector.New(1)
	defer c.Close()
	for _, seq := range []int{5, 4, 3, 2, 1} {
		if err := c.Submit(seq, fmt.Sprintf("v%d", seq)); err != nil {
			t.Fatalf("Submit(%d): %v", seq, err)
		}
	}
	// An already-cancelled context proves each Next is served from the
	// buffer immediately, without ever blocking.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, want := range []string{"v1", "v2", "v3", "v4", "v5"} {
		got, err := c.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if got != want {
			t.Fatalf("Next = %v, want %v", got, want)
		}
	}
}

func TestBlockingNext(t *testing.T) {
	c := orderedcollector.New(1)
	defer c.Close()
	res := make(chan nextResult, 1)
	go func() {
		v, err := c.Next(context.Background())
		res <- nextResult{v, err}
	}()
	waitFor(t, "waiter", func() bool { return c.Snapshot().Waiting == 1 })
	if err := c.Submit(1, "v1"); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	r := <-res
	if r.err != nil || r.v != "v1" {
		t.Fatalf("Next = %v, %v; want v1, nil", r.v, r.err)
	}
	if got := c.Snapshot().Waiting; got != 0 {
		t.Fatalf("Waiting = %d, want 0", got)
	}
}

func TestMultipleNextNoDuplicateDelivery(t *testing.T) {
	c := orderedcollector.New(1)
	defer c.Close()
	res := make(chan nextResult, 3)
	for range 3 {
		go func() {
			v, err := c.Next(context.Background())
			res <- nextResult{v, err}
		}()
	}
	waitFor(t, "3 waiters", func() bool { return c.Snapshot().Waiting == 3 })
	for _, seq := range []int{2, 3, 1} {
		if err := c.Submit(seq, fmt.Sprintf("v%d", seq)); err != nil {
			t.Fatalf("Submit(%d): %v", seq, err)
		}
	}
	got := map[any]int{}
	for range 3 {
		r := <-res
		if r.err != nil {
			t.Fatalf("Next: %v", r.err)
		}
		got[r.v]++
	}
	want := map[any]int{"v1": 1, "v2": 1, "v3": 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("delivered %v, want %v", got, want)
	}
}

func TestCancelWaitingNext(t *testing.T) {
	c := orderedcollector.New(1)
	defer c.Close()
	ctx1, cancel1 := context.WithCancel(context.Background())
	res1 := make(chan nextResult, 1)
	res2 := make(chan nextResult, 1)
	go func() {
		v, err := c.Next(ctx1)
		res1 <- nextResult{v, err}
	}()
	go func() {
		v, err := c.Next(context.Background())
		res2 <- nextResult{v, err}
	}()
	waitFor(t, "2 waiters", func() bool { return c.Snapshot().Waiting == 2 })
	cancel1()
	r1 := <-res1
	if !errors.Is(r1.err, context.Canceled) {
		t.Fatalf("cancelled Next = %v, %v; want context.Canceled", r1.v, r1.err)
	}
	waitFor(t, "1 waiter left", func() bool { return c.Snapshot().Waiting == 1 })
	// The surviving waiter still receives the next value.
	if err := c.Submit(1, "v1"); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	r2 := <-res2
	if r2.err != nil || r2.v != "v1" {
		t.Fatalf("Next = %v, %v; want v1, nil", r2.v, r2.err)
	}
}

func TestCloseDrainSemantics(t *testing.T) {
	c := orderedcollector.New(10)
	for _, seq := range []int{11, 10, 13} {
		if err := c.Submit(seq, fmt.Sprintf("v%d", seq)); err != nil {
			t.Fatalf("Submit(%d): %v", seq, err)
		}
	}
	c.Close()
	c.Close() // idempotent
	if err := c.Submit(12, "v12"); !errors.Is(err, orderedcollector.ErrClosed) {
		t.Fatalf("Submit after Close = %v, want ErrClosed", err)
	}
	for _, want := range []string{"v10", "v11"} {
		got, err := c.Next(context.Background())
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if got != want {
			t.Fatalf("Next = %v, want %v", got, want)
		}
	}
	// 12 can never arrive, so draining stops even though 13 is buffered.
	if _, err := c.Next(context.Background()); !errors.Is(err, orderedcollector.ErrClosed) {
		t.Fatalf("Next at gap = %v, want ErrClosed", err)
	}
	if got := c.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1 (seq 13 stuck behind gap)", got)
	}
}

func TestSnapshotAndLen(t *testing.T) {
	c := orderedcollector.New(1)
	for _, seq := range []int{3, 1, 2} {
		if err := c.Submit(seq, seq*10); err != nil {
			t.Fatalf("Submit(%d): %v", seq, err)
		}
	}
	snap := c.Snapshot()
	if snap.NextSeq != 1 || snap.Waiting != 0 || snap.Closed {
		t.Fatalf("Snapshot = %+v", snap)
	}
	if !reflect.DeepEqual(snap.Buffered, []int{1, 2, 3}) {
		t.Fatalf("Buffered = %v, want [1 2 3]", snap.Buffered)
	}
	snap.Buffered[0] = 99 // the returned slice must be a copy
	if got := c.Snapshot().Buffered[0]; got != 1 {
		t.Fatalf("Buffered changed after mutation: %v", got)
	}
	if got := c.Len(); got != 3 {
		t.Fatalf("Len = %d, want 3", got)
	}
	for range 3 {
		if _, err := c.Next(context.Background()); err != nil {
			t.Fatalf("Next: %v", err)
		}
	}
	snap = c.Snapshot()
	if snap.NextSeq != 4 || len(snap.Buffered) != 0 || c.Len() != 0 {
		t.Fatalf("after drain: Snapshot = %+v, Len = %d", snap, c.Len())
	}
	// A blocked Next is woken by Close with ErrClosed.
	res := make(chan nextResult, 1)
	go func() {
		v, err := c.Next(context.Background())
		res <- nextResult{v, err}
	}()
	waitFor(t, "waiter", func() bool { return c.Snapshot().Waiting == 1 })
	c.Close()
	r := <-res
	if !errors.Is(r.err, orderedcollector.ErrClosed) {
		t.Fatalf("Next after Close = %v, %v; want ErrClosed", r.v, r.err)
	}
	if snap := c.Snapshot(); !snap.Closed {
		t.Fatal("Closed = false, want true")
	}
}
