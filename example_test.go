package coherent_test

import (
	"context"
	"fmt"
	"time"

	"github.com/OWNER/coherent"
)

// Example shows a consumer wiring: a local cache kept coherent by a Handler that
// applies invalidation events from a source. Here the source is in-process
// (MemorySource); in production it would be a gRPC-streaming source (see
// examples/grpc).
func Example() {
	cache := coherent.NewMemCache[string, string](coherent.Options[string, string]{
		MaxEntries: 10_000,
		TTL:        5 * time.Minute, // fallback consistency; invalidation is primary
	})

	src := coherent.NewMemorySource[string](16)
	handler := coherent.NewHandler[string, string](cache, src, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go handler.Run(ctx)

	cache.Set("user:42", "Ada")
	fmt.Println(get(cache, "user:42"))

	// The owner service changed user:42; it publishes an invalidation.
	src.Publish(coherent.Event[string]{Key: "user:42", EventType: "updated", TimestampMs: 1})

	// Wait for the asynchronous eviction to be applied.
	for {
		if _, ok := cache.Get("user:42"); !ok {
			break
		}
		time.Sleep(time.Millisecond)
	}
	fmt.Println(get(cache, "user:42"))

	// Output:
	// Ada
	// <miss>
}

func get(c *coherent.MemCache[string, string], k string) string {
	if v, ok := c.Get(k); ok {
		return v
	}
	return "<miss>"
}
