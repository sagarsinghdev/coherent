# gRPC streaming transport

The recommended cross-process transport for `coherent`, implemented as a **separate Go module** so the
core library stays dependency-free. The owner service exposes the `InvalidationService` defined in
[`proto/coherent/v1/invalidation.proto`](../../proto/coherent/v1/invalidation.proto); each consumer
opens a `Subscribe` stream and feeds events into a `coherent.Handler`.

This directory is a working, tested reference — not just a sketch:

| Package | What it is |
|---|---|
| [`gen/coherentv1`](gen/coherentv1) | Generated protobuf + gRPC code. |
| [`grpcsource`](grpcsource) | A reusable `coherent.InvalidationSource` over the gRPC client: reconnect with backoff, watermark-based resume, clear-on-reconnect, a generic `KeyDecoder`. |
| [`refserver`](refserver) | A minimal in-memory owner server wiring `server.ConnectionManager` behind the generated service. |
| [`grpcsource/example_test.go`](grpcsource/example_test.go) | An end-to-end round trip over an in-process `bufconn`: publish an invalidation, watch the consumer evict its copy. |

```sh
go get github.com/sagarsinghdev/coherent/examples/grpc/grpcsource
```

## Consumer side

```go
conn, _ := grpc.NewClient(ownerAddr, grpc.WithTransportCredentials(creds))
src, _ := grpcsource.New(grpcsource.Options[string]{
    Client:       coherentv1.NewInvalidationServiceClient(conn),
    SubscriberID: "billing-" + podName,
    KeyDecoder:   func(k string) (string, error) { return k, nil }, // or NewString(...)
})

cache := coherent.NewMemCache[string, string](coherent.Options[string, string]{TTL: 5 * time.Minute})
handler := coherent.NewHandler(cache, src, nil)
go handler.Run(ctx) // applies invalidations; clears on every (re)connect
```

The `Source` owns its connection lifecycle: it reconnects with exponential backoff (1s → 30s), passes
its `Watermark()` as `resume_after_ms` on each reconnect so the server can replay what was missed, and
emits a cache-clear on every fresh connection so the `Handler` flushes and lazily re-fills. You get
correctness across reconnects for free.

## Owner side

```go
owner := refserver.New(256, 4096) // per-consumer buffer, replay-log retention
owner.Register(grpcServer)

// After committing a mutation to the source of truth:
owner.Publish(key, "updated") // append to replay log + broadcast to consumers
```

`refserver` does the full correctness sequence out of the box: it appends every event to an in-memory
`server.MemLog` and, in `Subscribe`, **registers the live channel → replays everything after the
consumer's watermark (or sends one cache-clear on a retention gap) → drains buffered → streams live**.
So a consumer that reconnects with a watermark gets exactly what it missed — covered end-to-end by
[`refserver/replay_test.go`](refserver/replay_test.go).

`MemLog` retains a bounded number of recent events (fine for single-writer owners, tests, small
fleets). For **durable, cross-restart** retention, replace the `MemLog`-backed `RecordReader` with one
over your log — e.g. Kafka: `offsetsForTimes` to seek by watermark, and report a retention gap when the
earliest available offset is newer than the requested watermark. Nothing else changes, because
`RecordReader` is the only seam. See the [`server`](../../server) package docs.

## Regenerating the protobuf code

```sh
# from repo root; requires protoc, protoc-gen-go, protoc-gen-go-grpc
protoc -I proto \
  --go_out=examples/grpc/gen --go_opt=module=github.com/sagarsinghdev/coherent/examples/grpc/gen \
  --go-grpc_out=examples/grpc/gen --go-grpc_opt=module=github.com/sagarsinghdev/coherent/examples/grpc/gen \
  proto/coherent/v1/invalidation.proto
```

## Multi-replica fan-out

When the owner runs multiple replicas, each holds only its locally-connected consumers. Publish every
mutation to a shared **Redis Pub/Sub** channel; each replica subscribes at startup and calls
`Broadcast` for its local consumers. Consumers never touch Redis — they only speak gRPC to the owner.

## Message-bus alternative

If a consumer already has broker access and wants no relay dependency, consume the change topic
directly with a **per-pod consumer group** (`<name>-<service>-<POD_NAME>`, offset=latest) and emit
`coherent.Event` values into a channel-backed source. This mode has no server-side replay; it relies
on clear-on-(re)connect plus TTL to self-heal. Prefer the gRPC transport when you want replay.
