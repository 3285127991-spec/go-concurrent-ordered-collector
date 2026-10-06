// Command demo shows out-of-order submit with in-order delivery, then the
// drain semantics of Close.
package main

import (
	"context"
	"fmt"

	"orderedcollector"
)

func main() {
	ctx := context.Background()

	fmt.Println("== out-of-order submit, in-order delivery ==")
	c := orderedcollector.New(1)
	for _, seq := range []int{3, 1, 5, 2, 4} {
		if err := c.Submit(seq, fmt.Sprintf("value-%d", seq)); err != nil {
			fmt.Println("submit error:", err)
		}
		fmt.Printf("submitted seq=%d, buffered=%v\n", seq, c.Snapshot().Buffered)
	}
	for range 5 {
		v, err := c.Next(ctx)
		if err != nil {
			fmt.Println("next error:", err)
			break
		}
		fmt.Println("next ->", v)
	}

	fmt.Println("== close: drain what is deliverable, then ErrClosed ==")
	d := orderedcollector.New(10)
	_ = d.Submit(11, "eleven")
	_ = d.Submit(10, "ten")
	_ = d.Submit(13, "thirteen") // gap at 12: never deliverable after Close
	d.Close()
	fmt.Println("submit after close:", d.Submit(12, "twelve"))
	for {
		v, err := d.Next(ctx)
		if err != nil {
			fmt.Println("next ->", err)
			break
		}
		fmt.Println("next ->", v)
	}
	fmt.Println("final snapshot:", d.Snapshot())
}
