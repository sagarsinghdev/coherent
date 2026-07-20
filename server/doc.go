// Package server provides the owner-side primitives for building a service that
// pushes cache-invalidation events to coherent consumers.
//
// It contains two building blocks, both transport-agnostic:
//
//   - ConnectionManager broadcasts events to all currently-connected consumers
//     with non-blocking sends, so one slow consumer cannot stall the others.
//
//   - ReplayService replays events a reconnecting consumer missed, based on a
//     timestamp watermark, over any RecordReader (Kafka, a log, etc.).
//
// A correct subscribe handler must REGISTER the consumer with the
// ConnectionManager BEFORE it starts replay, then drain buffered live events,
// then stream live. Registering first guarantees no event is lost in the gap
// between reconnect and the end of replay. See the package example.
package server
