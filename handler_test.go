package coherent

import (
	"context"
	"errors"
	"testing"
	"time"
)

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestHandlerEvictsKey(t *testing.T) {
	cache := NewMemCache[string, int](Options[string, int]{})
	cache.Set("a", 1)
	cache.Set("b", 2)

	src := NewMemorySource[string](8)
	h := NewHandler[string, int](cache, src, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = h.Run(ctx) }()

	src.Publish(Event[string]{Key: "a", EventType: "updated", TimestampMs: 10})
	eventually(t, func() bool {
		_, ok := cache.Get("a")
		return !ok
	})
	if _, ok := cache.Get("b"); !ok {
		t.Fatal("b should remain cached (key-precise eviction)")
	}
}

func TestHandlerClearsOnCacheClear(t *testing.T) {
	cache := NewMemCache[string, int](Options[string, int]{})
	cache.Set("a", 1)
	cache.Set("b", 2)

	src := NewMemorySource[string](8)
	h := NewHandler[string, int](cache, src, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = h.Run(ctx) }()

	src.Publish(Event[string]{IsCacheClear: true, TimestampMs: 20})
	eventually(t, func() bool { return cache.Len() == 0 })
}

func TestHandlerStopsOnContextCancel(t *testing.T) {
	cache := NewMemCache[string, int](Options[string, int]{})
	src := NewMemorySource[string](1)
	h := NewHandler[string, int](cache, src, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v; want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

// TestGracefulDegradationSourceDown asserts the fallback guarantee: if the
// invalidation source dies, the Handler stops but the cache keeps serving its
// entries (bounded by TTL) rather than hard-failing reads.
func TestGracefulDegradationSourceDown(t *testing.T) {
	cache := NewMemCache[string, int](Options[string, int]{})
	cache.Set("a", 1)
	cache.Set("b", 2)

	src := NewMemorySource[string](1)
	h := NewHandler[string, int](cache, src, nil)

	done := make(chan error, 1)
	go func() { done <- h.Run(context.Background()) }()

	// The source goes away (e.g. the owner/transport dropped).
	src.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v; want nil on stream close", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after source closed")
	}

	// Reads are still served from the local cache — degrade, don't fail.
	if v, ok := cache.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a) = %v,%v; want 1,true (reads must survive source loss)", v, ok)
	}
	if v, ok := cache.Get("b"); !ok || v != 2 {
		t.Fatalf("Get(b) = %v,%v; want 2,true", v, ok)
	}
}

func TestHandlerStopsOnStreamClose(t *testing.T) {
	cache := NewMemCache[string, int](Options[string, int]{})
	src := NewMemorySource[string](1)
	h := NewHandler[string, int](cache, src, nil)

	done := make(chan error, 1)
	go func() { done <- h.Run(context.Background()) }()
	src.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v; want nil on stream close", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after stream close")
	}
}
