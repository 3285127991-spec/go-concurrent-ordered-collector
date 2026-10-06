// Command collector-demo shows out-of-order submission with in-order
// delivery, then Close drain semantics.
package main

import (
	"context"
	"fmt"

	"orderedcollector"
)

func main() {
	ctx := context.Background()

	fmt.Println("== out-of-order submit, in-order delivery ==")
	c := orderedcollector.New[string](1)
	for _, item := range []struct {
		seq   int
		value string
	}{{3, "three"}, {1, "one"}, {2, "two"}} {
		if err := c.Submit(item.seq, item.value); err != nil {
			fmt.Println("submit:", err)
		}
	}
	for c.Len() > 0 {
		value, err := c.Next(ctx)
		if err != nil {
			fmt.Println("next:", err)
			break
		}
		fmt.Println("next:", value)
	}

	fmt.Println("== close drains the buffer, then reports closed ==")
	d := orderedcollector.New[int](0)
	_ = d.Submit(1, 100)
	_ = d.Submit(0, 0)
	d.Close()
	fmt.Println("submit after close:", d.Submit(2, 200))
	for {
		value, err := d.Next(ctx)
		if err != nil {
			fmt.Println("next:", err)
			break
		}
		fmt.Println("next:", value)
	}
}
