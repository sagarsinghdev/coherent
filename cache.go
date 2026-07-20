package coherent

// Cache is the storage layer coherent operates on. It is intentionally small so
// that any backend — the bundled MemCache, an Otter-backed adapter, or your own —
// can satisfy it.
//
// Implementations must be safe for concurrent use by multiple goroutines.
type Cache[K comparable, V any] interface {
	// Get returns the value for key and whether it was present.
	Get(key K) (V, bool)
	// Set stores value under key, overwriting any existing entry.
	Set(key K, value V)
	// Delete removes key. Deleting an absent key is a no-op.
	Delete(key K)
	// Clear removes all entries.
	Clear()
	// Len returns the current number of entries.
	Len() int
}
