# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0]

First public release.

### Added
- Core interfaces: `Cache[K,V]`, `InvalidationSource[K]`, and `Handler[K,V]`.
- `MemCache`: bundled zero-dependency LRU cache with optional TTL and a Caffeine-style
  `RemovalListener` (`RemovalCause`: explicit / replaced / expired / size).
- `LoadingCache` with read-through loading and singleflight cache-stampede protection.
- `MemorySource`: in-process invalidation source for tests and single-binary usage.
- `server.ConnectionManager`: non-blocking broadcast registry for owner-side fan-out.
- `server.ReplayService`: watermark-based replay with retention-gap detection.
- **gRPC streaming transport** (`examples/grpc`, separate module): a reusable `grpcsource.Source`
  implementing `InvalidationSource` (reconnect with backoff, watermark resume, clear-on-reconnect,
  generic `KeyDecoder`), generated protobuf/gRPC code, a `refserver` reference owner server, and an
  end-to-end example over an in-process connection.
- **Otter backend adapter** (`contrib/otter`, separate module): `otteradapter.Cache`, an Otter v2
  implementation of `Cache[K,V]` for lock-free, high-concurrency storage.
- Reproducible benchmarks and methodology in [`bench/`](bench): local hit cost and end-to-end
  invalidation apply latency.
- Runnable `Example` tests for the package wiring, `LoadingCache`, and `MemCache` removal listeners.
- GitHub Actions CI: build + `go vet` + `-race` tests + `golangci-lint`, matrixed across the core and
  optional modules and two Go versions.

### Notes
- The core module (`github.com/sagarsinghdev/coherent`) is standard-library only. Optional transports
  and backends live in separate modules so they never add weight to the core.
- Pre-1.0: the public API may change. Breaking changes will be called out here.
