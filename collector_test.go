package orderedcollector

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
)

// waitFor polls cond without sleeping; it fails after a generous deadline.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within deadline")
		}
		runtime.Gosched()
	}
}

// mustNext calls Next with a timeout guard so a broken implementation fails
// fast instead of hanging the test run.
func mustNext[T any](t *testing.T, c *Collector[T]) T {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	value, err := c.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	return value
}

func TestOutOfOrderSubmitDeliveredInOrder(t *testing.T) {
	c := New[string](5)
	for _, item := range []struct {
		seq   int
		value string
	}{{7, "seven"}, {5, "five"}, {6, "six"}} {
		if err := c.Submit(item.seq, item.value); err != nil {
			t.Fatalf("Submit(%d): %v", item.seq, err)
		}
	}
	if got := c.Len(); got != 3 {
		t.Fatalf("Len = %d, want 3", got)
	}
	for _, want := range []string{"five", "six", "seven"} {
		if got := mustNext(t, c); got != want {
			t.Fatalf("Next = %q, want %q", got, want)
		}
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0", got)
	}
	if got := c.Snapshot().NextSeq; got != 8 {
		t.Fatalf("NextSeq = %d, want 8", got)
	}
}

func TestSubmitDuplicateAndExpired(t *testing.T) {
	c := New[int](0)
	if err := c.Submit(0, 10); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := c.Submit(0, 11); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate Submit err = %v, want ErrDuplicate", err)
	}
	if got := mustNext(t, c); got != 10 {
		t.Fatalf("Next = %d, want 10", got)
	}
	if err := c.Submit(0, 12); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired Submit err = %v, want ErrExpired", err)
	}
	if err := c.Submit(1, 13); err != nil {
		t.Fatalf("Submit(1): %v", err)
	}
	if err := c.Submit(1, 14); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate Submit err = %v, want ErrDuplicate", err)
	}
}

func TestContiguousBufferedAdvance(t *testing.T) {
	c := New[int](0)
	for _, seq := range []int{2, 4, 1, 3, 0} {
		if err := c.Submit(seq, seq*10); err != nil {
			t.Fatalf("Submit(%d): %v", seq, err)
		}
	}
	// Every value is already buffered, so each Next returns immediately
	// after the previous one advances the expected sequence.
	for want := 0; want < 5; want++ {
		if got := mustNext(t, c); got != want*10 {
			t.Fatalf("Next = %d, want %d", got, want*10)
		}
	}
}

func TestBlockingNext(t *testing.T) {
	c := New[string](0)
	done := make(chan string, 1)
	go func() {
		value, err := c.Next(context.Background())
		if err == nil {
			done <- value
		}
	}()
	waitFor(t, func() bool { return c.Snapshot().Waiting == 1 })
	select {
	case v := <-done:
		t.Fatalf("Next returned %q before its value was submitted", v)
	default:
	}
	if err := c.Submit(0, "hello"); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	select {
	case got := <-done:
		if got != "hello" {
			t.Fatalf("Next = %q, want %q", got, "hello")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Next did not deliver after Submit")
	}
}

func TestConcurrentNextDistinct(t *testing.T) {
	const n = 8
	c := New[int](0)
	results := make(chan int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			value, err := c.Next(ctx)
			if err != nil {
				t.Errorf("Next: %v", err)
				return
			}
			results <- value
		}()
	}
	waitFor(t, func() bool { return c.Snapshot().Waiting == n })
	for _, seq := range []int{3, 0, 7, 2, 5, 1, 6, 4} {
		if err := c.Submit(seq, seq*100); err != nil {
			t.Fatalf("Submit(%d): %v", seq, err)
		}
	}
	wg.Wait()
	close(results)
	seen := make(map[int]bool)
	for value := range results {
		if seen[value] {
			t.Fatalf("value %d delivered more than once", value)
		}
		seen[value] = true
	}
	for seq := 0; seq < n; seq++ {
		if !seen[seq*100] {
			t.Fatalf("value for seq %d was never delivered", seq)
		}
	}
}

func TestCancel(t *testing.T) {
	c := New[int](0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := c.Next(ctx)
		done <- err
	}()
	waitFor(t, func() bool { return c.Snapshot().Waiting == 1 })
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Next err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled Next did not return")
	}
	waitFor(t, func() bool { return c.Snapshot().Waiting == 0 })
	// The cancelled call consumed nothing: a later submit is delivered
	// to a fresh Next.
	if err := c.Submit(0, 42); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if got := mustNext(t, c); got != 42 {
		t.Fatalf("Next = %d, want 42", got)
	}
}

func TestCloseDrainSemantics(t *testing.T) {
	c := New[string](0)
	if err := c.Submit(1, "b"); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := c.Submit(0, "a"); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	c.Close()
	c.Close() // idempotent
	if err := c.Submit(2, "c"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Submit after Close err = %v, want ErrClosed", err)
	}
	// Buffered in-order values remain drainable after Close.
	if got := mustNext(t, c); got != "a" {
		t.Fatalf("Next = %q, want %q", got, "a")
	}
	if got := mustNext(t, c); got != "b" {
		t.Fatalf("Next = %q, want %q", got, "b")
	}
	// The next expected sequence can never arrive: report closed
	// immediately instead of blocking.
	if _, err := c.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next err = %v, want ErrClosed", err)
	}
}

func TestSnapshot(t *testing.T) {
	c := New[int](10)
	for _, seq := range []int{12, 10, 15} {
		if err := c.Submit(seq, seq); err != nil {
			t.Fatalf("Submit(%d): %v", seq, err)
		}
	}
	snap := c.Snapshot()
	if snap.NextSeq != 10 {
		t.Fatalf("NextSeq = %d, want 10", snap.NextSeq)
	}
	if !reflect.DeepEqual(snap.Buffered, []int{10, 12, 15}) {
		t.Fatalf("Buffered = %v, want [10 12 15]", snap.Buffered)
	}
	if snap.Waiting != 0 || snap.Closed {
		t.Fatalf("Waiting=%d Closed=%v, want 0 false", snap.Waiting, snap.Closed)
	}
	// Buffered is a copy: mutating it must not affect the collector.
	snap.Buffered[0] = 999
	if got := c.Snapshot().Buffered; !reflect.DeepEqual(got, []int{10, 12, 15}) {
		t.Fatalf("Buffered after mutation = %v, want [10 12 15]", got)
	}

	// Waiting and Closed are exercised on an empty collector so the
	// blocked Next cannot be satisfied by buffered values.
	d := New[int](0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _ = d.Next(ctx)
		close(done)
	}()
	waitFor(t, func() bool { return d.Snapshot().Waiting == 1 })
	cancel()
	<-done
	waitFor(t, func() bool { return d.Snapshot().Waiting == 0 })

	d.Close()
	if snap := d.Snapshot(); !snap.Closed {
		t.Fatal("Closed = false, want true")
	}
}
