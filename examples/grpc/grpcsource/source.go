// Package grpcsource implements coherent.InvalidationSource over the gRPC
// server-streaming InvalidationService defined in
// proto/coherent/v1/invalidation.proto.
//
// A Source opens a Subscribe stream, forwards each InvalidationEvent to the
// consumer as a coherent.Event, and owns its own connection lifecycle: it
// reconnects with exponential backoff and, on every fresh (re)connection, passes
// the highest event timestamp it has seen as resume_after_ms so the server can
// replay what was missed.
//
// # Correctness
//
// On every successful (re)connection the Source emits a cache-clear event before
// forwarding any key-level event. This satisfies the coherent.InvalidationSource
// contract regardless of whether the server also sends one: a reconnecting
// consumer flushes and lazily re-fills rather than risk serving a value that
// changed during the gap. Duplicate clears are harmless.
//
// # Keys
//
// The wire contract carries string keys. KeyDecoder maps each wire key to the
// consumer's key type K. For string-keyed caches, use New with an identity
// decoder or the NewString helper.
package grpcsource

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagarsinghdev/coherent"
	coherentv1 "github.com/sagarsinghdev/coherent/examples/grpc/gen/coherentv1"
)

// Default backoff bounds and channel buffer.
const (
	defaultMinBackoff = time.Second
	defaultMaxBackoff = 30 * time.Second
	defaultBuffer     = 64
)

// Options configures a Source.
type Options[K comparable] struct {
	// Client is the generated InvalidationService client. Required.
	Client coherentv1.InvalidationServiceClient
	// SubscriberID is an opaque, stable identifier for this consumer connection,
	// used by the server for registry tracking. Recommended: "<service>-<pod>".
	SubscriberID string
	// Namespace optionally scopes the subscription to one logical dataset when the
	// owner serves several. Empty subscribes to all.
	Namespace string
	// KeyDecoder maps a wire event's string key to the cache key type K. Required.
	// A decode error causes the event to be skipped (logged), not fatal.
	KeyDecoder func(wireKey string) (K, error)
	// Buffer is the event-channel buffer size. Values below 1 use the default (64).
	Buffer int
	// MinBackoff and MaxBackoff bound the reconnect backoff. Non-positive values
	// use the defaults (1s and 30s).
	MinBackoff, MaxBackoff time.Duration
	// Logger receives connection-lifecycle logs. Nil uses slog.Default.
	Logger *slog.Logger
}

// Source is a gRPC-streaming coherent.InvalidationSource. Construct it with New.
type Source[K comparable] struct {
	opts       Options[K]
	minBackoff time.Duration
	maxBackoff time.Duration
	log        *slog.Logger

	out       chan coherent.Event[K]
	watermark atomic.Int64
	once      sync.Once
}

var _ coherent.InvalidationSource[string] = (*Source[string])(nil)

// New returns a Source configured by opts. It returns an error if Client or
// KeyDecoder is nil.
func New[K comparable](opts Options[K]) (*Source[K], error) {
	if opts.Client == nil {
		return nil, errors.New("grpcsource: Options.Client is required")
	}
	if opts.KeyDecoder == nil {
		return nil, errors.New("grpcsource: Options.KeyDecoder is required")
	}
	buffer := opts.Buffer
	if buffer < 1 {
		buffer = defaultBuffer
	}
	minB := opts.MinBackoff
	if minB <= 0 {
		minB = defaultMinBackoff
	}
	maxB := opts.MaxBackoff
	if maxB <= 0 {
		maxB = defaultMaxBackoff
	}
	if maxB < minB {
		maxB = minB
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Source[K]{
		opts:       opts,
		minBackoff: minB,
		maxBackoff: maxB,
		log:        log,
		out:        make(chan coherent.Event[K], buffer),
	}, nil
}

// NewString returns a Source for a string-keyed cache, using the wire key
// verbatim. It is a convenience wrapper over New.
func NewString(client coherentv1.InvalidationServiceClient, subscriberID string) (*Source[string], error) {
	return New(Options[string]{
		Client:       client,
		SubscriberID: subscriberID,
		KeyDecoder:   func(k string) (string, error) { return k, nil },
	})
}

// Events starts the connection loop (once) and returns the event stream. The
// stream is closed when ctx is cancelled. Calling Events more than once returns
// the same channel without starting a second loop.
func (s *Source[K]) Events(ctx context.Context) <-chan coherent.Event[K] {
	s.once.Do(func() { go s.run(ctx) })
	return s.out
}

// Watermark returns the highest event timestamp (Unix ms) observed so far, or 0
// before any event. It is passed as resume_after_ms on the next reconnect.
func (s *Source[K]) Watermark() int64 { return s.watermark.Load() }

// run drives the connect/consume/reconnect loop until ctx is cancelled.
func (s *Source[K]) run(ctx context.Context) {
	defer close(s.out)
	backoff := s.minBackoff
	for ctx.Err() == nil {
		stream, err := s.opts.Client.Subscribe(ctx, &coherentv1.SubscribeRequest{
			SubscriberId:  s.opts.SubscriberID,
			ResumeAfterMs: s.watermark.Load(),
			Namespace:     s.opts.Namespace,
		})
		if err != nil {
			s.log.LogAttrs(ctx, slog.LevelWarn, "coherent/grpcsource: subscribe failed; retrying",
				slog.String("error", err.Error()), slog.Duration("backoff", backoff))
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, s.maxBackoff)
			continue
		}
		backoff = s.minBackoff // reset on a successful connection

		// Fresh (re)connection: flush before resuming key-level events.
		if !s.emit(ctx, coherent.Event[K]{IsCacheClear: true}) {
			return
		}
		if !s.consume(ctx, stream) {
			return // ctx cancelled
		}
		// stream ended (server closed / transient error): reconnect from watermark.
	}
}

// consume forwards events from a live stream. It returns false only when ctx is
// cancelled (caller should stop); a stream error returns true so run reconnects.
func (s *Source[K]) consume(ctx context.Context, stream coherentv1.InvalidationService_SubscribeClient) bool {
	for {
		ev, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			s.log.LogAttrs(ctx, slog.LevelInfo, "coherent/grpcsource: stream ended; reconnecting",
				slog.String("error", err.Error()))
			return true
		}
		s.advanceWatermark(ev.GetTimestampMs())
		if ev.GetIsCacheClear() {
			if !s.emit(ctx, coherent.Event[K]{IsCacheClear: true, TimestampMs: ev.GetTimestampMs()}) {
				return false
			}
			continue
		}
		key, derr := s.opts.KeyDecoder(ev.GetKey())
		if derr != nil {
			s.log.LogAttrs(ctx, slog.LevelWarn, "coherent/grpcsource: key decode failed; skipping event",
				slog.String("wire_key", ev.GetKey()), slog.String("error", derr.Error()))
			continue
		}
		if !s.emit(ctx, coherent.Event[K]{
			Key:         key,
			EventType:   ev.GetEventType(),
			TimestampMs: ev.GetTimestampMs(),
		}) {
			return false
		}
	}
}

// emit sends ev to the consumer, honouring ctx cancellation. It returns false if
// ctx is done.
func (s *Source[K]) emit(ctx context.Context, ev coherent.Event[K]) bool {
	select {
	case <-ctx.Done():
		return false
	case s.out <- ev:
		return true
	}
}

// advanceWatermark raises the watermark to ts if ts is higher.
func (s *Source[K]) advanceWatermark(ts int64) {
	for {
		cur := s.watermark.Load()
		if ts <= cur || s.watermark.CompareAndSwap(cur, ts) {
			return
		}
	}
}

// nextBackoff doubles cur, capped at limit.
func nextBackoff(cur, limit time.Duration) time.Duration {
	next := cur * 2
	if next > limit {
		return limit
	}
	return next
}

// sleepCtx sleeps for d or until ctx is done. It returns false if ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
