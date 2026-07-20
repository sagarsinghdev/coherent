package coherent

import (
	"container/list"
	"sync"
	"time"
)

// RemovalCause explains why an entry left the cache. It mirrors the causes used
// by Caffeine and is reported to a RemovalListener.
type RemovalCause int

const (
	// CauseExplicit means the entry was removed by Delete or Clear (including
	// invalidation events).
	CauseExplicit RemovalCause = iota
	// CauseReplaced means the entry's value was overwritten by Set.
	CauseReplaced
	// CauseExpired means the entry's TTL elapsed.
	CauseExpired
	// CauseSize means the entry was evicted to keep the cache within MaxEntries.
	CauseSize
)

// String returns a human-readable name for the cause.
func (c RemovalCause) String() string {
	switch c {
	case CauseExplicit:
		return "explicit"
	case CauseReplaced:
		return "replaced"
	case CauseExpired:
		return "expired"
	case CauseSize:
		return "size"
	default:
		return "unknown"
	}
}

// RemovalListener is invoked after an entry is removed from the cache. It is
// called without the cache lock held, but it MUST NOT block for long and MUST
// NOT call back into the same cache instance.
type RemovalListener[K comparable, V any] func(key K, value V, cause RemovalCause)

// Options configures a MemCache.
type Options[K comparable, V any] struct {
	// MaxEntries is the maximum number of entries retained; 0 means unlimited.
	// When exceeded, the least-recently-used entry is evicted (CauseSize).
	MaxEntries int
	// TTL is the time-to-live for each entry, measured from its last write.
	// 0 disables expiry. Expiry is applied lazily on access.
	TTL time.Duration
	// OnRemoval, if set, is notified of every removal with its cause.
	OnRemoval RemovalListener[K, V]

	// clock is an injectable time source used only in tests. When nil,
	// time.Now is used.
	clock func() time.Time
}

type entry[K comparable, V any] struct {
	key      K
	value    V
	expireAt time.Time // zero value means "never expires"
}

// MemCache is the bundled, zero-dependency Cache implementation: a thread-safe
// LRU cache with optional TTL and removal notifications. It is correct and
// convenient for moderate throughput. For high-throughput production workloads,
// adapt a specialized cache (for example Otter's adaptive W-TinyLFU) behind the
// Cache interface — see contrib/otter.
//
// MemCache satisfies Cache[K, V].
type MemCache[K comparable, V any] struct {
	mu         sync.Mutex
	ll         *list.List // front = most-recently-used
	items      map[K]*list.Element
	maxEntries int
	ttl        time.Duration
	onRemoval  RemovalListener[K, V]
	now        func() time.Time
}

var _ Cache[string, int] = (*MemCache[string, int])(nil)

// NewMemCache returns a MemCache configured by opts.
func NewMemCache[K comparable, V any](opts Options[K, V]) *MemCache[K, V] {
	now := opts.clock
	if now == nil {
		now = time.Now
	}
	return &MemCache[K, V]{
		ll:         list.New(),
		items:      make(map[K]*list.Element),
		maxEntries: opts.MaxEntries,
		ttl:        opts.TTL,
		onRemoval:  opts.OnRemoval,
		now:        now,
	}
}

// removal is a pending listener notification, fired after the lock is released.
type removal[K comparable, V any] struct {
	key   K
	value V
	cause RemovalCause
}

func (c *MemCache[K, V]) fire(rs []removal[K, V]) {
	if c.onRemoval == nil {
		return
	}
	for _, r := range rs {
		c.onRemoval(r.key, r.value, r.cause)
	}
}

// dropElement unlinks el and returns its removal record. Caller holds the lock.
func (c *MemCache[K, V]) dropElement(el *list.Element, cause RemovalCause) removal[K, V] {
	e := el.Value.(*entry[K, V])
	c.ll.Remove(el)
	delete(c.items, e.key)
	return removal[K, V]{key: e.key, value: e.value, cause: cause}
}

// Get returns the value for key. Expired entries are removed and reported as a
// miss.
func (c *MemCache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	el, ok := c.items[key]
	if !ok {
		c.mu.Unlock()
		var zero V
		return zero, false
	}
	e := el.Value.(*entry[K, V])
	if !e.expireAt.IsZero() && c.now().After(e.expireAt) {
		r := c.dropElement(el, CauseExpired)
		c.mu.Unlock()
		c.fire([]removal[K, V]{r})
		var zero V
		return zero, false
	}
	c.ll.MoveToFront(el)
	v := e.value
	c.mu.Unlock()
	return v, true
}

// Set stores value under key. An overwrite reports the previous value as
// CauseReplaced; a size eviction reports the victim as CauseSize.
func (c *MemCache[K, V]) Set(key K, value V) {
	var exp time.Time
	if c.ttl > 0 {
		exp = c.now().Add(c.ttl)
	}
	var pending []removal[K, V]

	c.mu.Lock()
	if el, ok := c.items[key]; ok {
		e := el.Value.(*entry[K, V])
		old := e.value
		e.value = value
		e.expireAt = exp
		c.ll.MoveToFront(el)
		if c.onRemoval != nil {
			pending = append(pending, removal[K, V]{key: key, value: old, cause: CauseReplaced})
		}
	} else {
		el := c.ll.PushFront(&entry[K, V]{key: key, value: value, expireAt: exp})
		c.items[key] = el
		if c.maxEntries > 0 && c.ll.Len() > c.maxEntries {
			if victim := c.ll.Back(); victim != nil {
				r := c.dropElement(victim, CauseSize)
				if c.onRemoval != nil {
					pending = append(pending, r)
				}
			}
		}
	}
	c.mu.Unlock()
	c.fire(pending)
}

// Delete removes key if present, reporting it as CauseExplicit.
func (c *MemCache[K, V]) Delete(key K) {
	c.mu.Lock()
	el, ok := c.items[key]
	if !ok {
		c.mu.Unlock()
		return
	}
	r := c.dropElement(el, CauseExplicit)
	c.mu.Unlock()
	if c.onRemoval != nil {
		c.fire([]removal[K, V]{r})
	}
}

// Clear removes all entries, reporting each as CauseExplicit.
func (c *MemCache[K, V]) Clear() {
	c.mu.Lock()
	var pending []removal[K, V]
	if c.onRemoval != nil {
		pending = make([]removal[K, V], 0, len(c.items))
		for _, el := range c.items {
			e := el.Value.(*entry[K, V])
			pending = append(pending, removal[K, V]{key: e.key, value: e.value, cause: CauseExplicit})
		}
	}
	c.ll.Init()
	c.items = make(map[K]*list.Element)
	c.mu.Unlock()
	c.fire(pending)
}

// Len returns the number of entries currently held (including any that are
// expired but not yet lazily evicted).
func (c *MemCache[K, V]) Len() int {
	c.mu.Lock()
	n := c.ll.Len()
	c.mu.Unlock()
	return n
}
