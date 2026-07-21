# Benchmarks & methodology

Reproducible benchmarks for the two numbers that define `coherent`: how fast a **local hit** is, and
how fast an **invalidation** is applied.

## Run them

```sh
go test -run='^$' -bench=. -benchmem ./bench/
```

`-run='^$'` skips unit tests so only benchmarks run. Add `-benchtime=2s` or `-count=6` for tighter
estimates, and `-cpu=1,10` to see single- vs multi-core behaviour.

## What each benchmark measures

| Benchmark | Measures |
|---|---|
| `BenchmarkCacheHit` | The local read path — a `Get` hit that never leaves the process. The headline "microsecond read". |
| `BenchmarkCacheHitParallel` | The hit path under multi-core contention. `MemCache` is single-mutex; this is where a [`contrib/otter`](../contrib/otter) backend scales better, with the invalidation layer unchanged. |
| `BenchmarkCacheSet` | The write path. |
| `BenchmarkInvalidationLatency` | End-to-end **in-process** time from `Publish` to the entry being evicted by the `Handler` (publish → channel → Handler goroutine → `cache.Delete`), reported as `ns/invalidation`. It isolates the library's apply latency; a real transport (gRPC, a bus) adds its own network RTT on top. |

## Reference results

Apple M2 Pro (10 cores), Go 1.26, `darwin/arm64`, `MemCache` backend. Numbers are indicative — run on
your own target.

| Benchmark | Time/op | Allocs/op |
|---|---:|---:|
| `CacheHit` (single goroutine) | ~17.5 ns | 0 |
| `CacheSet` | ~19 ns | 0 |
| `CacheHit` (parallel, 10 cores) | ~154 ns | 0 |
| `InvalidationLatency` (in-process apply) | ~4.9 µs | 2 |

### Reading the numbers

- **Local hits are tens of nanoseconds, zero-allocation** — comfortably inside the "microsecond read"
  design goal. The read never touches the network.
- **Parallel hits cost more** because `MemCache` serializes on one mutex. That is the deliberate
  tradeoff for a zero-dependency default; swap in the Otter backend for lock-free scaling without
  touching the invalidation layer.
- **Invalidation apply is single-digit microseconds in-process** — dominated by goroutine scheduling
  and the channel hand-off, not the cache operation. Over a network transport, end-to-end freshness is
  this apply time plus the transport RTT (typically tens of milliseconds), which is the figure that
  matters versus a TTL measured in minutes.
