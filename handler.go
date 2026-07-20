package coherent

import (
	"context"
	"log/slog"
)

// Handler binds an InvalidationSource to a Cache and applies incoming events.
// It is the correctness core of the library (see the package doc for the two
// rules it enforces).
type Handler[K comparable, V any] struct {
	cache  Cache[K, V]
	source InvalidationSource[K]
	log    *slog.Logger
}

// NewHandler returns a Handler that applies events from source to cache.
// If logger is nil, slog.Default is used.
func NewHandler[K comparable, V any](cache Cache[K, V], source InvalidationSource[K], logger *slog.Logger) *Handler[K, V] {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler[K, V]{cache: cache, source: source, log: logger}
}

// Run consumes the source's event stream until ctx is cancelled or the stream
// closes. It applies each event to the cache:
//
//   - IsCacheClear -> cache.Clear()  (flush on (re)connect / retention gap)
//   - otherwise    -> cache.Delete(Key)  (idempotent, key-precise eviction)
//
// Run blocks; call it from its own goroutine. It returns ctx.Err() on
// cancellation, or nil when the source stream closes cleanly.
func (h *Handler[K, V]) Run(ctx context.Context) error {
	events := h.source.Events(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			if ev.IsCacheClear {
				h.cache.Clear()
				h.log.LogAttrs(ctx, slog.LevelInfo, "coherent: cache cleared on invalidation-source signal")
				continue
			}
			h.cache.Delete(ev.Key)
		}
	}
}
