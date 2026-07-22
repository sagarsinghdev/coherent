// Package server provides the owner-side primitives for building a service that
// pushes cache-invalidation events to coherent consumers.
//
// It contains two building blocks, both transport-agnostic:
//
//   - ConnectionManager broadcasts events to all currently-connected consumers
//     with non-blocking sends, so one slow consumer cannot stall the others.
//
//   - ReplayService replays events a reconnecting consumer missed, based on a
//     timestamp watermark, over any RecordReader.
//
//   - MemLog is a bundled, zero-dependency RecordReader source: a bounded,
//     in-memory event log that makes watermark replay (and retention-gap
//     detection) work out of the box. Swap it for a RecordReader over a durable
//     log (for example Kafka) when you need cross-restart retention — RecordReader
//     is the only seam that changes.
//
// A correct subscribe handler must REGISTER the consumer with the
// ConnectionManager BEFORE it starts replay, then drain buffered live events,
// then stream live. Registering first guarantees no event is lost in the gap
// between reconnect and the end of replay. See the examples/grpc refserver.
package server
