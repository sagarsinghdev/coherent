package coherent

import (
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type capturedRemoval struct {
	key   string
	value int
	cause RemovalCause
}

type recorder struct {
	mu   sync.Mutex
	seen []capturedRemoval
}

func (r *recorder) listener(k string, v int, c RemovalCause) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, capturedRemoval{k, v, c})
}

func (r *recorder) snapshot() []capturedRemoval {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]capturedRemoval, len(r.seen))
	copy(out, r.seen)
	return out
}

func TestMemCacheBasic(t *testing.T) {
	c := NewMemCache[string, int](Options[string, int]{})
	if _, ok := c.Get("missing"); ok {
		t.Fatal("expected miss on empty cache")
	}
	c.Set("a", 1)
	c.Set("b", 2)
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a) = %v,%v; want 1,true", v, ok)
	}
	if c.Len() != 2 {
		t.Fatalf("Len = %d; want 2", c.Len())
	}
	c.Delete("a")
	if _, ok := c.Get("a"); ok {
		t.Fatal("expected miss after Delete")
	}
	c.Delete("a") // idempotent: must not panic
	c.Clear()
	if c.Len() != 0 {
		t.Fatalf("Len after Clear = %d; want 0", c.Len())
	}
}

func TestMemCacheTTLExpiry(t *testing.T) {
	clk := newFakeClock()
	rec := &recorder{}
	c := NewMemCache[string, int](Options[string, int]{
		TTL:       time.Minute,
		OnRemoval: rec.listener,
		clock:     clk.Now,
	})
	c.Set("a", 1)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("expected hit before expiry")
	}
	clk.Advance(2 * time.Minute)
	if _, ok := c.Get("a"); ok {
		t.Fatal("expected miss after TTL elapsed")
	}
	got := rec.snapshot()
	if len(got) != 1 || got[0].cause != CauseExpired || got[0].key != "a" {
		t.Fatalf("removals = %+v; want one CauseExpired for a", got)
	}
}

func TestMemCacheLRUEviction(t *testing.T) {
	rec := &recorder{}
	c := NewMemCache[string, int](Options[string, int]{MaxEntries: 2, OnRemoval: rec.listener})
	c.Set("a", 1)
	c.Set("b", 2)
	c.Get("a")    // touch a -> b becomes LRU
	c.Set("c", 3) // should evict b
	if c.Len() != 2 {
		t.Fatalf("Len = %d; want 2", c.Len())
	}
	if _, ok := c.Get("b"); ok {
		t.Fatal("expected b to be evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("expected a to survive (recently used)")
	}
	got := rec.snapshot()
	if len(got) != 1 || got[0].cause != CauseSize || got[0].key != "b" {
		t.Fatalf("removals = %+v; want one CauseSize for b", got)
	}
}

func TestMemCacheReplaced(t *testing.T) {
	rec := &recorder{}
	c := NewMemCache[string, int](Options[string, int]{OnRemoval: rec.listener})
	c.Set("a", 1)
	c.Set("a", 2)
	if v, _ := c.Get("a"); v != 2 {
		t.Fatalf("Get(a) = %d; want 2", v)
	}
	got := rec.snapshot()
	if len(got) != 1 || got[0].cause != CauseReplaced || got[0].value != 1 {
		t.Fatalf("removals = %+v; want one CauseReplaced with old value 1", got)
	}
}
