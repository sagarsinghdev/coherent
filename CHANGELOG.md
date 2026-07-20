# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Core interfaces: `Cache[K,V]`, `InvalidationSource[K]`, and `Handler[K,V]`.
- `MemCache`: bundled zero-dependency LRU cache with optional TTL and a Caffeine-style
  `RemovalListener` (`RemovalCause`: explicit / replaced / expired / size).
- `LoadingCache` with read-through loading and singleflight cache-stampede protection.
- `MemorySource`: in-process invalidation source for tests and single-binary usage.
- `server.ConnectionManager`: non-blocking broadcast registry for owner-side fan-out.
- `server.ReplayService`: watermark-based replay with retention-gap detection.
- Documentation for gRPC-streaming and message-bus transports (`examples/grpc`) and an
  Otter backend adapter (`contrib/otter`).

### Notes
- Pre-1.0: the public API may change. Breaking changes will be called out here.
