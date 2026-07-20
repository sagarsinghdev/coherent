# coherent

**A local, in-process cache for Go that stays coherent across a fleet of processes.**

[![Go Reference](https://pkg.go.dev/badge/github.com/OWNER/coherent.svg)](https://pkg.go.dev/github.com/OWNER/coherent)
[![CI](https://github.com/OWNER/coherent/actions/workflows/ci.yml/badge.svg)](https://github.com/OWNER/coherent/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/OWNER/coherent)](https://goreportcard.com/report/github.com/OWNER/coherent)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> Go has great **local** caches (Otter, Ristretto) and great **distributed** caches (Redis).
> `coherent` fills the gap between them: a local cache whose entries are **invalidated in
> near‑real‑time** when the source of truth changes — so every process serves microsecond‑fast reads
> that are also *fresh*.

---

## Why

A service that reads slowly-changing data (config, entitlements, reference data) from a central owner
has two bad options: call the owner on every read (milliseconds of latency, a hard dependency, and
read load that scales with your traffic), or cache locally with a TTL (stale for up to the TTL after
every change).

`coherent` gives you the third option — a local cache plus a **pluggable real-time invalidation
channel**:

- **Microsecond reads.** Hits are served from local memory. *(See benchmarks below.)*
- **Fresh.** When the owner changes a value, every process evicts its copy within milliseconds.
- **Correct under churn.** Two simple rules (below) keep caches correct across reconnects and faults.
- **Degrades gracefully.** If the invalidation channel drops, it falls back to TTL — never hard-fails.

## Install

```sh
go get github.com/OWNER/coherent
```

*(Replace `OWNER` with the published module path.)*

## Quickstart

```go
cache := coherent.NewMemCache[string, string](coherent.Options[string, string]{
    MaxEntries: 10_000,
    TTL:        5 * time.Minute, // fallback consistency; invalidation is primary
})

// A source pushes invalidation events. Use MemorySource in-process, or a
// gRPC-streaming source across the network (see examples/grpc).
src := coherent.NewMemorySource[string](64)

handler := coherent.NewHandler[string, string](cache, src, nil)
go handler.Run(ctx) // applies invalidations to the cache

// Read path — a hit never leaves the process:
if v, ok := cache.Get("user:42"); ok {
    use(v)
}
```

For read-through loading with cache-stampede protection:

```go
lc := coherent.NewLoadingCache(cache, func(ctx context.Context, id string) (string, error) {
    return ownerClient.FetchUser(ctx, id) // called once per key even under concurrent misses
})
v, err := lc.GetOrLoad(ctx, "user:42")
```

## Architecture

```mermaid
flowchart LR
  subgraph Owner service
    WR[write/mutation] --> PUB[(Redis Pub/Sub fan-out)]
    PUB --> CM[server.ConnectionManager]
    CM --> GS[gRPC stream]
  end
  subgraph Consumer process (coherent)
    GS --> SRC[InvalidationSource]
    SRC --> H[Handler: EvictKey / Clear]
    H --> LC[(Cache)]
    APP[app.Get] -->|hit| LC
    LC -.->|miss → Loader → Set| OWN[fetch from owner]
  end
```

Three small interfaces:

| Interface | Role | Provided |
|---|---|---|
| `Cache[K,V]` | storage | `MemCache` (bundled, zero-dep); adapt Otter etc. |
| `InvalidationSource[K]` | push channel of events | `MemorySource` (in-proc); gRPC/bus (see examples) |
| `Handler[K,V]` | applies events to the cache | bundled |

The `server` subpackage provides the owner-side primitives — `ConnectionManager` (non-blocking
broadcast) and `ReplayService` (watermark replay) — for building the service that emits events.

## Correctness — the two rules

1. **Clear on reconnect.** A reconnecting consumer can't know what changed while it was disconnected,
   so on every (re)connection the source emits a *cache-clear* event and the `Handler` flushes the
   cache; reads lazily re-fill. Brief re-warm, never a stale read across a gap.
2. **Idempotent, key-precise eviction.** Each event evicts exactly one key; deleting an absent key is
   a no-op, so duplicate/overlapping events (e.g. from replay) are harmless.

On the owner side, a subscribe handler must **register the consumer before starting replay**, then
drain events buffered during replay, then stream live — so no event is lost in the reconnect gap.

## Backends

`MemCache` is the bundled default: a thread-safe LRU with optional TTL and Caffeine-style
`RemovalListener` (`RemovalCause`: explicit / replaced / expired / size). It is correct and
convenient. For high-concurrency production workloads, adapt a specialized cache (e.g.
[Otter](https://github.com/maypok86/otter)'s adaptive W-TinyLFU) behind the `Cache` interface — see
[`contrib/otter`](contrib/otter).

## Transports

- **In-process:** `MemorySource` (bundled) — tests, local dev, single-binary usage.
- **gRPC streaming:** the recommended cross-process transport — see [`examples/grpc`](examples/grpc)
  for the `.proto`, the source adapter, and the owner-side wiring using the `server` primitives.
- **Message bus:** consume a topic directly with per-pod consumer groups — pattern documented in
  [`examples/grpc`](examples/grpc).

## Benchmarks

`MemCache`, Apple M-series, `go test -bench`. Numbers are indicative; run `make bench` on your target.

| Benchmark | Time/op | Allocs/op |
|---|---:|---:|
| `Get` hit (single goroutine) | ~11 ns | 0 |
| `Set` | ~14 ns | 0 |
| `Get` hit (high parallel contention) | ~105 ns | 0 |

The bundled `MemCache` uses a single lock, so heavy multi-core read contention costs more than a
lock-free cache would; that's the tradeoff for a zero-dependency default. Adapt Otter (see
`contrib/otter`) when you need lock-free scaling — `coherent`'s invalidation layer is unchanged.

## Status

`v0.x` — the API may change before `v1.0.0`. Semantic versioning; changes tracked in
[CHANGELOG.md](CHANGELOG.md).

**Roadmap:** sharded `MemCache` backend, an official Otter adapter module, a ready-to-import gRPC
source package, and OpenTelemetry metrics hooks.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Issues and PRs welcome.

## License

[Apache-2.0](LICENSE).
