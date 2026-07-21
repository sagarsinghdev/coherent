// Package otteradapter adapts an Otter v2 cache to coherent.Cache[K, V], so a
// coherent consumer can use Otter's lock-free, adaptive W-TinyLFU storage instead
// of the bundled MemCache while keeping the same invalidation Handler, sources,
// and server primitives.
//
// It lives in its own module so the core coherent library stays dependency-free:
// only consumers that want Otter pull in the Otter dependency.
//
//	cache, err := otteradapter.New[string, string](100_000, 5*time.Minute)
//	if err != nil {
//		// handle
//	}
//	handler := coherent.NewHandler[string, string](cache, src, nil)
//	go handler.Run(ctx)
//
// Everything above the storage layer — the Handler, InvalidationSource, and
// server primitives — is unchanged; only the Cache implementation swaps.
package otteradapter

import (
	"time"

	"github.com/maypok86/otter/v2"
	"github.com/sagarsinghdev/coherent"
)

// Cache adapts an *otter.Cache to coherent.Cache[K, V]. Construct it with New or
// Wrap. It is safe for concurrent use.
type Cache[K comparable, V any] struct {
	c *otter.Cache[K, V]
}

var _ coherent.Cache[string, string] = (*Cache[string, string])(nil)

// New returns an Otter-backed Cache. maxEntries of 0 or less leaves the cache
// unbounded; a positive ttl expires each entry that long after its last write
// (Otter's ExpiryWriting), matching MemCache's TTL semantics. It returns an error
// only if Otter rejects the derived options.
//
// For full control over Otter (weighers, stats, custom expiry), build the
// *otter.Cache yourself and pass it to Wrap.
func New[K comparable, V any](maxEntries int, ttl time.Duration) (*Cache[K, V], error) {
	opts := &otter.Options[K, V]{}
	if maxEntries > 0 {
		opts.MaximumSize = maxEntries
	}
	if ttl > 0 {
		opts.ExpiryCalculator = otter.ExpiryWriting[K, V](ttl)
	}
	c, err := otter.New(opts)
	if err != nil {
		return nil, err
	}
	return &Cache[K, V]{c: c}, nil
}

// Wrap adapts an already-configured *otter.Cache to coherent.Cache[K, V].
func Wrap[K comparable, V any](c *otter.Cache[K, V]) *Cache[K, V] {
	return &Cache[K, V]{c: c}
}

// Get returns the value for key and whether it was present.
func (a *Cache[K, V]) Get(key K) (V, bool) { return a.c.GetIfPresent(key) }

// Set stores value under key, overwriting any existing entry.
func (a *Cache[K, V]) Set(key K, value V) { a.c.Set(key, value) }

// Delete removes key. Deleting an absent key is a no-op — the property that makes
// coherent's key-precise invalidation idempotent.
func (a *Cache[K, V]) Delete(key K) { a.c.Invalidate(key) }

// Clear removes all entries. The Handler calls this on a source cache-clear
// signal (reconnect / retention gap).
func (a *Cache[K, V]) Clear() { a.c.InvalidateAll() }

// Len returns Otter's estimated entry count. Because Otter maintains size
// asynchronously, this is an estimate, not an exact count.
func (a *Cache[K, V]) Len() int { return a.c.EstimatedSize() }
