package coherent

import (
	"context"
	"sync"
	"sync/atomic"
)

// MemorySource is an in-process InvalidationSource. It is intended for tests,
// local development, and single-process usage where invalidations are produced
// in the same binary that consumes them. For cross-process invalidation, use a
// transport-backed source (see examples/grpc).
//
// MemorySource satisfies InvalidationSource[K].
type MemorySource[K comparable] struct {
	mu        sync.Mutex
	ch        chan Event[K]
	closed    bool
	watermark atomic.Int64
}

var _ InvalidationSource[string] = (*MemorySource[string])(nil)

// NewMemorySource returns a MemorySource with the given channel buffer size.
func NewMemorySource[K comparable](buffer int) *MemorySource[K] {
	if buffer < 0 {
		buffer = 0
	}
	return &MemorySource[K]{ch: make(chan Event[K], buffer)}
}

// Events returns the event stream. The stream closes when Close is called.
func (s *MemorySource[K]) Events(ctx context.Context) <-chan Event[K] {
	return s.ch
}

// Watermark returns the highest TimestampMs published so far.
func (s *MemorySource[K]) Watermark() int64 { return s.watermark.Load() }

// Publish sends an event to consumers and advances the watermark. It blocks if
// the buffer is full. Publishing after Close panics, matching channel semantics.
func (s *MemorySource[K]) Publish(ev Event[K]) {
	for {
		cur := s.watermark.Load()
		if ev.TimestampMs <= cur || s.watermark.CompareAndSwap(cur, ev.TimestampMs) {
			break
		}
	}
	s.ch <- ev
}

// Close closes the event stream. It is safe to call once; further calls are
// no-ops.
func (s *MemorySource[K]) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}
