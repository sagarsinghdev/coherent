# gRPC streaming transport

The recommended cross-process transport for `coherent`. The owner service exposes the
`InvalidationService` defined in [`proto/coherent/v1/invalidation.proto`](../../proto/coherent/v1/invalidation.proto);
each consumer opens a `Subscribe` stream and feeds events into a `coherent.Handler`.

This directory is a **separate Go module** so the core library stays dependency-free. Generate the
protobuf code and wire it as shown below.

## 1. Generate code from the proto

```sh
# from repo root
protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/coherent/v1/invalidation.proto
```

(Requires `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc`; or use `buf generate`.)

## 2. Consumer side — a gRPC `InvalidationSource`

Implement `coherent.InvalidationSource[string]` over the generated client. Sketch:

```go
type grpcSource struct {
    client       coherentv1.InvalidationServiceClient
    subscriberID string
    watermark    atomic.Int64
    out          chan coherent.Event[string]
}

func (s *grpcSource) Events(ctx context.Context) <-chan coherent.Event[string] {
    go func() {
        defer close(s.out)
        backoff := time.Second
        for ctx.Err() == nil {
            stream, err := s.client.Subscribe(ctx, &coherentv1.SubscribeRequest{
                SubscriberId:  s.subscriberID,
                ResumeAfterMs: s.watermark.Load(), // 0 on first connect
            })
            if err != nil {
                sleep(ctx, &backoff) // exp backoff, cap 30s
                continue
            }
            backoff = time.Second
            for {
                ev, err := stream.Recv()
                if err != nil {
                    break // reconnect; server replays from watermark
                }
                if ev.TimestampMs > s.watermark.Load() {
                    s.watermark.Store(ev.TimestampMs)
                }
                s.out <- coherent.Event[string]{
                    Key:          ev.Key,
                    EventType:    ev.EventType,
                    TimestampMs:  ev.TimestampMs,
                    IsCacheClear: ev.IsCacheClear,
                }
            }
        }
    }()
    return s.out
}

func (s *grpcSource) Watermark() int64 { return s.watermark.Load() }
```

Then: `handler := coherent.NewHandler(cache, src, logger); go handler.Run(ctx)`.

> The server sends `is_cache_clear=true` on connect and on retention gaps, so the `Handler`
> automatically flushes and re-fills — you get correctness for free.

## 3. Owner side — using the `server` primitives

```go
mgr    := server.NewConnectionManager[*coherentv1.InvalidationEvent](256)
replay := server.NewReplayService(newKafkaRecordReader) // your RecordReader impl

// On every mutation, after writing the source of truth:
mgr.Broadcast(&coherentv1.InvalidationEvent{Key: key, EventType: "updated", TimestampMs: nowMs()})

// Subscribe RPC handler — REGISTER BEFORE REPLAY:
func (h *Handler) Subscribe(req *coherentv1.SubscribeRequest, stream coherentv1.InvalidationService_SubscribeServer) error {
    ch := mgr.Register(req.SubscriberId)          // 1. register live channel first
    defer mgr.Deregister(req.SubscriberId)

    if req.ResumeAfterMs > 0 {                     // 2. replay missed events
        err := replay.Replay(stream.Context(), req.ResumeAfterMs,
            func(raw []byte) error { return stream.Send(decode(raw)) },
            func() error { return stream.Send(&coherentv1.InvalidationEvent{IsCacheClear: true}) },
        )
        if err != nil {
            return err
        }
    }
    for ev := range ch {                           // 3. drain buffered + stream live
        if err := stream.Send(ev); err != nil {
            return err
        }
    }
    return nil
}
```

## 4. Multi-replica fan-out

When the owner runs multiple replicas, each holds only its locally-connected consumers. Publish every
mutation to a shared **Redis Pub/Sub** channel; each replica subscribes at startup and calls
`mgr.Broadcast` for its local consumers. Consumers never touch Redis — they only speak gRPC to the
owner.

## Message-bus alternative

If a consumer already has broker access and wants no relay dependency, consume the change topic
directly with a **per-pod consumer group** (`<name>-<service>-<POD_NAME>`, offset=latest) and emit
`coherent.Event` values into a channel-backed source. This mode has no server-side replay; it relies
on clear-on-(re)connect plus TTL to self-heal. Prefer the gRPC transport when you want replay.
