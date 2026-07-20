package coherent

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGetOrLoadCachesResult(t *testing.T) {
	var calls atomic.Int64
	lc := NewLoadingCache[string, int](
		NewMemCache[string, int](Options[string, int]{}),
		func(_ context.Context, key string) (int, error) {
			calls.Add(1)
			return len(key), nil
		},
	)

	v, err := lc.GetOrLoad(context.Background(), "abc")
	if err != nil || v != 3 {
		t.Fatalf("GetOrLoad = %d,%v; want 3,nil", v, err)
	}
	// Second call should hit the cache, not the loader.
	if _, err := lc.GetOrLoad(context.Background(), "abc"); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader called %d times; want 1", got)
	}
}

func TestGetOrLoadSingleflight(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	lc := NewLoadingCache[string, int](
		NewMemCache[string, int](Options[string, int]{}),
		func(_ context.Context, key string) (int, error) {
			calls.Add(1)
			<-release // hold the load open so concurrent callers coalesce
			return 42, nil
		},
	)

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			v, err := lc.GetOrLoad(context.Background(), "k")
			if err != nil || v != 42 {
				t.Errorf("GetOrLoad = %d,%v; want 42,nil", v, err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond) // let all goroutines reach the in-flight load
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("loader called %d times; want 1 (singleflight)", got)
	}
}
