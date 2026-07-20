# Otter backend adapter

The bundled `MemCache` uses a single lock, which is fine for moderate read concurrency but costs more
under heavy multi-core contention. For lock-free scaling and adaptive W-TinyLFU admission, adapt
[Otter v2](https://github.com/maypok86/otter) behind `coherent.Cache[K, V]`.

The adapter is intentionally *not* part of the core module (it would force the Otter dependency on
everyone). Drop this into your own module, or a `contrib/otter` submodule with its own `go.mod`, and
`go get github.com/maypok86/otter/v2`.

> Verify signatures against the Otter version you pin — the sketch below targets the Otter v2 API and
> may need small adjustments as Otter evolves.

```go
package otteradapter

import (
    "time"

    "github.com/maypok86/otter/v2"
    "github.com/OWNER/coherent"
)

// Cache adapts an Otter cache to coherent.Cache[K, V].
type Cache[K comparable, V any] struct {
    c *otter.Cache[K, V]
}

func New[K comparable, V any](maxEntries int, ttl time.Duration) *Cache[K, V] {
    b := otter.Must(&otter.Options[K, V]{
        MaximumSize:      maxEntries,
        ExpiryCalculator: otter.ExpiryWriting[K, V](ttl), // TTL from last write; omit for none
    })
    return &Cache[K, V]{c: b}
}

func (a *Cache[K, V]) Get(key K) (V, bool) { return a.c.GetIfPresent(key) }
func (a *Cache[K, V]) Set(key K, val V)    { a.c.Set(key, val) }
func (a *Cache[K, V]) Delete(key K)        { a.c.Invalidate(key) }
func (a *Cache[K, V]) Clear()              { a.c.InvalidateAll() }
func (a *Cache[K, V]) Len() int            { return a.c.EstimatedSize() }
```

Use it exactly like `MemCache`:

```go
cache := otteradapter.New[string, string](100_000, 5*time.Minute)
handler := coherent.NewHandler(cache, src, logger)
go handler.Run(ctx)
```

Everything else — the invalidation `Handler`, sources, and server primitives — is unchanged. Only the
storage layer swaps.
