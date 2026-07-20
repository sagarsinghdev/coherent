package coherent

import "context"

// Event is a single cache-invalidation signal delivered by an InvalidationSource.
//
// An Event either targets one key (the common case) or requests a full flush via
// IsCacheClear. When IsCacheClear is true, Key is ignored.
type Event[K comparable] struct {
	// Key identifies the entry to evict. Ignored when IsCacheClear is true.
	Key K
	// EventType is an optional, informational label such as "created",
	// "updated", or "deleted". coherent does not interpret it.
	EventType string
	// TimestampMs is the source-assigned event time in Unix milliseconds. It is
	// used as a watermark for replay on reconnect.
	TimestampMs int64
	// IsCacheClear requests that the entire cache be flushed. Sources emit this
	// on (re)connection and when a retention gap means missed events cannot be
	// replayed.
	IsCacheClear bool
}

// InvalidationSource is a pluggable transport that pushes invalidation events to
// a consumer. Implementations own their own connection lifecycle and MUST emit an
// Event with IsCacheClear set to true on every fresh (re)connection, before
// resuming key-level events.
//
// The returned channel is closed when the source is permanently done (for
// example, when the provided context is cancelled).
type InvalidationSource[K comparable] interface {
	// Events returns the stream of invalidation events. It should be called
	// once; calling it again may return the same channel or panic, depending on
	// the implementation.
	Events(ctx context.Context) <-chan Event[K]
	// Watermark returns the highest TimestampMs observed so far, or 0 before any
	// event is received. Consumers pass this as the resume point on reconnect so
	// the source can replay only what was missed.
	Watermark() int64
}
