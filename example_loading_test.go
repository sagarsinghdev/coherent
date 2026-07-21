package coherent_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/sagarsinghdev/coherent"
)

// ExampleLoadingCache demonstrates read-through loading with cache-stampede
// protection: concurrent misses for the same key collapse into a single Loader
// call.
func ExampleLoadingCache() {
	var loads atomic.Int64
	lc := coherent.NewLoadingCache(
		coherent.NewMemCache[string, int](coherent.Options[string, int]{}),
		func(_ context.Context, key string) (int, error) {
			loads.Add(1)
			return len(key), nil // stand-in for an RPC to the owner service
		},
	)

	ctx := context.Background()
	v, _ := lc.GetOrLoad(ctx, "user:42") // miss -> loads
	fmt.Println("value:", v)
	_, _ = lc.GetOrLoad(ctx, "user:42") // hit -> no load
	fmt.Println("loads:", loads.Load())

	// Output:
	// value: 7
	// loads: 1
}

// ExampleMemCache_removalListener shows Caffeine-style removal notifications: a
// RemovalListener is told why each entry left the cache.
func ExampleMemCache_removalListener() {
	cache := coherent.NewMemCache[string, int](coherent.Options[string, int]{
		MaxEntries: 2,
		TTL:        time.Minute,
		OnRemoval: func(key string, _ int, cause coherent.RemovalCause) {
			fmt.Printf("removed %s: %s\n", key, cause)
		},
	})

	cache.Set("a", 1)
	cache.Set("a", 2) // overwrite -> CauseReplaced
	cache.Set("b", 1)
	cache.Set("c", 1) // exceeds MaxEntries -> LRU victim evicted (CauseSize)
	cache.Delete("c") // explicit removal -> CauseExplicit

	// Output:
	// removed a: replaced
	// removed a: size
	// removed c: explicit
}
