// Package refserver is a minimal, in-memory reference implementation of the
// InvalidationService owner side. It shows how to wire coherent's server
// primitives (server.ConnectionManager) behind the generated gRPC service so a
// fleet of consumers stays coherent.
//
// It is intentionally small: it fans a published invalidation out to every
// connected consumer with non-blocking sends. It has no durable log and therefore
// no watermark replay — reconnecting consumers rely on the client-side
// clear-on-reconnect (see the grpcsource package) plus TTL to self-heal. For
// production replay, back a server.ReplayService with a RecordReader over your
// durable log (Kafka, etc.) and call Replay inside Subscribe before going live;
// see the server package docs.
package refserver

import (
	"sync/atomic"
	"time"

	"github.com/sagarsinghdev/coherent/examples/grpc/gen/coherentv1"
	"github.com/sagarsinghdev/coherent/server"
	"google.golang.org/grpc"
)

// Server is an in-memory InvalidationService. The zero value is not usable; call
// New.
type Server struct {
	coherentv1.UnimplementedInvalidationServiceServer
	mgr    *server.ConnectionManager[*coherentv1.InvalidationEvent]
	lastTS atomic.Int64
}

// New returns a Server whose per-consumer send buffers hold bufSize events.
func New(bufSize int) *Server {
	return &Server{mgr: server.NewConnectionManager[*coherentv1.InvalidationEvent](bufSize)}
}

// Register installs s on gs. Call before gs.Serve.
func (s *Server) Register(gs *grpc.Server) {
	coherentv1.RegisterInvalidationServiceServer(gs, s)
}

// Subscribe streams invalidation events to one consumer. It registers the
// consumer's live channel, then streams until the client disconnects or the
// consumer is replaced. This reference server has no replay, so ResumeAfterMs is
// accepted but not acted on; correctness across the reconnect gap is provided by
// the client's clear-on-reconnect.
func (s *Server) Subscribe(req *coherentv1.SubscribeRequest, stream coherentv1.InvalidationService_SubscribeServer) error {
	id := req.GetSubscriberId()
	ch := s.mgr.Register(id)
	defer s.mgr.Deregister(id)

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-ch:
			if !ok {
				return nil // deregistered or replaced by a newer connection
			}
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
	}
}

// Publish broadcasts a key-level invalidation to all connected consumers and
// returns the strictly-increasing timestamp (Unix ms) assigned to it. Call it
// after committing a mutation to the source of truth.
func (s *Server) Publish(key, eventType string) int64 {
	ts := s.nextTS()
	s.mgr.Broadcast(&coherentv1.InvalidationEvent{
		Key:         key,
		EventType:   eventType,
		TimestampMs: ts,
	})
	return ts
}

// PublishClear broadcasts a cache-clear signal to all connected consumers.
func (s *Server) PublishClear() int64 {
	ts := s.nextTS()
	s.mgr.Broadcast(&coherentv1.InvalidationEvent{IsCacheClear: true, TimestampMs: ts})
	return ts
}

// Active returns the number of connected consumers.
func (s *Server) Active() int { return s.mgr.Active() }

// Sent returns the total number of successful broadcast sends.
func (s *Server) Sent() int64 { return s.mgr.Sent() }

// Dropped returns the number of broadcast sends dropped due to full consumer
// buffers.
func (s *Server) Dropped() int64 { return s.mgr.Dropped() }

// nextTS returns a strictly-increasing Unix-millisecond timestamp, so watermarks
// order correctly even for events published within the same millisecond.
func (s *Server) nextTS() int64 {
	now := time.Now().UnixMilli()
	for {
		last := s.lastTS.Load()
		next := now
		if next <= last {
			next = last + 1
		}
		if s.lastTS.CompareAndSwap(last, next) {
			return next
		}
	}
}
