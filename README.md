# orderedcollector

A small, dependency-free Go library: values are submitted out of order with
a sequence number and delivered strictly in order — `start, start+1, ...`.

## Usage

```go
c := orderedcollector.New(1) // first delivered seq is 1

_ = c.Submit(2, "b") // buffered
_ = c.Submit(1, "a")

v, _ := c.Next(ctx) // "a"
v, _ = c.Next(ctx)  // "b" (already buffered, returns immediately)
```

## API

- `New(start)` — create a collector whose first sequence is `start`.
- `Submit(seq, value)` — buffer a value; errors: `ErrDuplicate` (same seq
  twice), `ErrStale` (seq already consumed), `ErrClosed` (after `Close`).
- `Next(ctx)` — return the value for the current expected seq, blocking
  until it arrives, `ctx` is cancelled, or the collector is closed and no
  in-order value remains. Each value is delivered to exactly one caller.
- `Snapshot()` — `{NextSeq, Buffered (ascending copy), Waiting, Closed}`.
- `Len()` — number of buffered, not-yet-consumed values.
- `Close()` — reject new submits; buffered values still drain in order,
  then `Next` returns `ErrClosed`.

## Demo & tests

```sh
go run ./cmd/demo   # out-of-order submit / in-order delivery + Close demo
go test -race ./...
```
