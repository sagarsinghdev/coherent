// Package coherent provides a local, in-process cache for Go that stays
// coherent across a fleet of processes via real-time invalidation.
//
// Go has excellent local caches (for example Otter and Ristretto) and excellent
// distributed caches (for example Redis). coherent fills the gap between them:
// a local cache whose entries are invalidated in near-real-time when the source
// of truth changes, so every process serves microsecond-fast reads that are also
// fresh.
//
// # Design
//
// coherent separates three concerns behind small interfaces:
//
//   - Cache[K, V]            the storage layer (a zero-dependency default is
//     provided; adapt Otter or any other cache behind it).
//   - InvalidationSource[K]  a pluggable push channel of invalidation events
//     (gRPC streaming, a message bus, or in-memory).
//   - Handler[K, V]          binds a source to a cache and applies events.
//
// # Correctness
//
// Two rules make the design correct under process churn and network faults:
//
//  1. Clear on reconnect. A reconnecting consumer cannot know what it missed
//     while disconnected, so on any (re)connection the source emits a
//     cache-clear event and the Handler flushes the cache. Subsequent reads
//     lazily re-fill. This trades a brief re-warm for never serving a value
//     that changed during the gap.
//
//  2. Idempotent eviction. Deleting a key that is absent is a no-op, so
//     duplicate events (for example from replay overlap) are harmless.
//
// The server subpackage provides the owner-side primitives (a broadcast
// ConnectionManager and a watermark-based ReplayService) for building the
// service that pushes invalidation events.
package coherent
