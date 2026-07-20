package coherent

import (
	"strconv"
	"testing"
)

func BenchmarkMemCacheGetHit(b *testing.B) {
	c := NewMemCache[string, int](Options[string, int]{MaxEntries: 100_000})
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
		c.Set(keys[i], i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(keys[i%len(keys)])
	}
}

func BenchmarkMemCacheGetHitParallel(b *testing.B) {
	c := NewMemCache[string, int](Options[string, int]{MaxEntries: 100_000})
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
		c.Set(keys[i], i)
	}
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

func BenchmarkMemCacheSet(b *testing.B) {
	c := NewMemCache[string, int](Options[string, int]{MaxEntries: 100_000})
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
