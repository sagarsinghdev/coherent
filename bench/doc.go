// Package bench holds reproducible benchmarks for coherent: the local cache-hit
// cost and the end-to-end invalidation apply latency. It has no non-test code; it
// exists so the benchmarks and their methodology (see README.md) live in one
// place and stay dependency-free (they import only the core module).
//
// Run them with:
//
//	go test -bench=. -benchmem ./bench/
package bench
