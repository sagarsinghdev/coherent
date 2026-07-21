package otteradapter_test

import (
	"context"
	"testing"
	"time"

	"github.com/sagarsinghdev/coherent"
	otteradapter "github.com/sagarsinghdev/coherent/contrib/otter"
)

// The adapter must satisfy coherent.Cache.
var _ coherent.Cache[string, string] = (*otteradapter.Cache[string, string])(nil)

func TestGetSetDelete(t *testing.T) {
	c, err := otteradapter.New[string, int](1000, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Get("missing"); ok {
		t.Fatal("expected miss on empty cache")
	}
	c.Set("a", 1)
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a) = %v,%v; want 1,true", v, ok)
	}
	c.Delete("a")
	if _, ok := c.Get("a"); ok {
		t.Fatal("expected miss after Delete")
	}
	c.Delete("a") // idempotent: must not panic
}

func TestClear(t *testing.T) {
	c, err := otteradapter.New[string, int](1000, 0)
	if err != nil {
		t.Fatal(err)
	}
	c.Set("a", 1)
	c.Set("b", 2)
	c.Clear()
	if _, ok := c.Get("a"); ok {
		t.Fatal("expected a gone after Clear")
	}
	if _, ok := c.Get("b"); ok {
		t.Fatal("expected b gone after Clear")
	}
}

// TestHandlerIntegration proves the adapter works behind the invalidation
// Handler exactly like MemCache: a key-level event evicts one key.
func TestHandlerIntegration(t *testing.T) {
	cache, err := otteradapter.New[string, int](1000, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	cache.Set("a", 1)
	cache.Set("b", 2)

	src := coherent.NewMemorySource[string](8)
	h := coherent.NewHandler[string, int](cache, src, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = h.Run(ctx) }()

	src.Publish(coherent.Event[string]{Key: "a", EventType: "updated", TimestampMs: 1})

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := cache.Get("a"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a was not evicted by the handler within timeout")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if _, ok := cache.Get("b"); !ok {
		t.Fatal("b should remain (key-precise eviction)")
	}
}
