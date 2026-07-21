package bench_test

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/sagarsinghdev/coherent"
)

// seedKeys returns n distinct keys and a cache preloaded with them.
func seedKeys(c coherent.Cache[string, int], n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
		c.Set(keys[i], i)
	}
	return keys
}

// BenchmarkCacheHit measures the local read path — a cache hit that never leaves
// the process. This is the headline "microsecond read" figure.
func BenchmarkCacheHit(b *testing.B) {
	c := coherent.NewMemCache[string, int](coherent.Options[string, int]{MaxEntries: 100_000})
	keys := seedKeys(c, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(keys[i%len(keys)])
	}
}

// BenchmarkCacheHitParallel measures the hit path under multi-core contention.
// MemCache uses a single mutex, so this is where an Otter-backed backend
// (contrib/otter) scales better; the invalidation layer is identical for both.
func BenchmarkCacheHitParallel(b *testing.B) {
	c := coherent.NewMemCache[string, int](coherent.Options[string, int]{MaxEntries: 100_000})
	keys := seedKeys(c, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = c.Get(keys[i%len(keys)])
			i++
		}
	})
}

// BenchmarkCacheSet measures the write path.
func BenchmarkCacheSet(b *testing.B) {
	c := coherent.NewMemCache[string, int](coherent.Options[string, int]{MaxEntries: 100_000})
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(keys[i%len(keys)], i)
	}
}

// BenchmarkInvalidationLatency measures the end-to-end, in-process time from
// publishing an invalidation to the entry actually being evicted by the Handler
// (publish -> channel -> Handler goroutine -> cache.Delete). It isolates the
// library's apply latency; a real transport (gRPC, a bus) adds its own network
// RTT on top. The result is reported as the custom metric "ns/invalidation".
func BenchmarkInvalidationLatency(b *testing.B) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	cache := coherent.NewMemCache[string, int](coherent.Options[string, int]{MaxEntries: 100_000})
	src := coherent.NewMemorySource[string](1024)
	h := coherent.NewHandler[string, int](cache, src, quiet)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = h.Run(ctx) }()

	const key = "hot"
	var total time.Duration
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set(key, i)
		start := time.Now()
		src.Publish(coherent.Event[string]{Key: key, TimestampMs: int64(i + 1)})
		for {
			if _, ok := cache.Get(key); !ok {
				break
			}
		}
		total += time.Since(start)
	}
	b.StopTimer()
	if b.N > 0 {
		b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "ns/invalidation")
	}
}
