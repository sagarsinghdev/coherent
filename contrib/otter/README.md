# Otter backend adapter

An [Otter v2](https://github.com/maypok86/otter) implementation of `coherent.Cache[K, V]`. The bundled
`MemCache` uses a single lock — fine for moderate read concurrency, but it costs more under heavy
multi-core contention. This adapter swaps in Otter's lock-free, adaptive W-TinyLFU storage while
keeping the invalidation `Handler`, sources, and server primitives unchanged.

It is a **separate module** so the core library stays dependency-free — only consumers that want Otter
pull in the Otter dependency.

## Install

```sh
go get github.com/sagarsinghdev/coherent/contrib/otter
```

## Use

```go
import (
    "time"

    "github.com/sagarsinghdev/coherent"
    otteradapter "github.com/sagarsinghdev/coherent/contrib/otter"
)

// maxEntries <= 0 is unbounded; ttl > 0 expires each entry that long after its
// last write (matching MemCache's TTL semantics).
cache, err := otteradapter.New[string, string](100_000, 5*time.Minute)
if err != nil {
    // handle
}

handler := coherent.NewHandler(cache, src, nil)
go handler.Run(ctx)
```

Use it exactly like `MemCache` — everything above the storage layer is identical. For full control
over Otter (weighers, stats, custom expiry), build the `*otter.Cache` yourself and pass it to
`otteradapter.Wrap`.

## API

| Method | Backed by |
|---|---|
| `Get(key)` | `otter.Cache.GetIfPresent` |
| `Set(key, value)` | `otter.Cache.Set` |
| `Delete(key)` | `otter.Cache.Invalidate` (no-op on a missing key → idempotent invalidation) |
| `Clear()` | `otter.Cache.InvalidateAll` |
| `Len()` | `otter.Cache.EstimatedSize` (an estimate — Otter maintains size asynchronously) |
