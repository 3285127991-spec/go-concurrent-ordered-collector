# orderedcollector

并发有序的 Go 收集器：允许乱序提交，严格按序号顺序交付。零第三方依赖。

## 语义

- `New[T](start)` 创建收集器，首个交付序号为 `start`。
- `Submit(seq, value)` 乱序提交，未轮到的值暂存内存；重复序号返回
  `ErrDuplicate`，已消费序号返回 `ErrExpired`，关闭后返回 `ErrClosed`。
- `Next(ctx)` 严格按 `start, start+1, ...` 交付；期望值未到达时阻塞。
  每个值只交付一次，多个并发 `Next` 不会取到同一个值。`ctx` 取消只
  移除当前调用；取消与值可用并发发生时，只产生取消或交付其中一种结果。
- `Close()` 后拒绝新的 `Submit`；已缓冲且能按序接续的值仍可取出，
  无法继续时 `Next` 返回 `ErrClosed`。`Close` 幂等。
- `Snapshot()` 返回 `NextSeq`、升序的已缓冲序号副本、`Waiting`、
  `Closed`；`Len()` 只统计尚未消费的缓冲项。

## 示例

```go
c := orderedcollector.New[string](0)
_ = c.Submit(1, "b")
_ = c.Submit(0, "a")
v, _ := c.Next(context.Background()) // "a"
v, _ = c.Next(context.Background())  // "b"
```

## 演示

```
go run ./cmd/collector-demo
```

## 测试

```
go test -race ./...
```

