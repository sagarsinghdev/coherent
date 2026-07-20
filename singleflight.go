package coherent

import "sync"

// flightGroup deduplicates concurrent work by key: if a call for a key is already
// in flight, later callers wait for and share its result. It is a minimal,
// dependency-free equivalent of golang.org/x/sync/singleflight for a single
// value type.
type flightGroup[K comparable, V any] struct {
	mu    sync.Mutex
	calls map[K]*flightCall[V]
}

type flightCall[V any] struct {
	wg  sync.WaitGroup
	val V
	err error
}

func newFlightGroup[K comparable, V any]() *flightGroup[K, V] {
	return &flightGroup[K, V]{calls: make(map[K]*flightCall[V])}
}

// Do runs fn for key, ensuring only one execution is in flight for a given key
// at a time. Duplicate callers receive the result of the in-flight call.
func (g *flightGroup[K, V]) Do(key K, fn func() (V, error)) (V, error) {
	g.mu.Lock()
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := new(flightCall[V])
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()

	return c.val, c.err
}
