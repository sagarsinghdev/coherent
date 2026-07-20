package coherent

import "context"

// Loader fetches the authoritative value for a key on a cache miss (typically an
// RPC or HTTP call to the owner service).
type Loader[K comparable, V any] func(ctx context.Context, key K) (V, error)

// LoadingCache wraps a Cache with read-through loading and stampede protection:
// concurrent misses for the same key collapse into a single Loader call.
//
// It embeds the underlying Cache, so Get/Set/Delete/Clear/Len remain available;
// use GetOrLoad for the read-through path.
type LoadingCache[K comparable, V any] struct {
	Cache[K, V]
	loader Loader[K, V]
	group  *flightGroup[K, V]
}

// NewLoadingCache returns a LoadingCache backed by cache, filling misses via
// loader.
func NewLoadingCache[K comparable, V any](cache Cache[K, V], loader Loader[K, V]) *LoadingCache[K, V] {
	return &LoadingCache[K, V]{
		Cache:  cache,
		loader: loader,
		group:  newFlightGroup[K, V](),
	}
}

// GetOrLoad returns the cached value for key, or loads it via the Loader on a
// miss and caches the result. Concurrent misses for the same key share one load.
// A load error is returned and the value is not cached.
func (lc *LoadingCache[K, V]) GetOrLoad(ctx context.Context, key K) (V, error) {
	if v, ok := lc.Get(key); ok {
		return v, nil
	}
	return lc.group.Do(key, func() (V, error) {
		// Re-check under the flight: another caller may have populated it.
		if v, ok := lc.Get(key); ok {
			return v, nil
		}
		v, err := lc.loader(ctx, key)
		if err != nil {
			return v, err
		}
		lc.Set(key, v)
		return v, nil
	})
}
