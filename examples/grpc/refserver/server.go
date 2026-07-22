// Package refserver is a small, in-memory reference implementation of the
// InvalidationService owner side. It shows how to wire coherent's server
// primitives — ConnectionManager for live fan-out and ReplayService over a MemLog
// for watermark replay — behind the generated gRPC service so a fleet of
// consumers stays coherent, with correct recovery across reconnects.
//
// The Subscribe handler follows the ordering invariant: it registers the
// consumer's live channel first, replays everything after the consumer's
// watermark (or sends one cache-clear on a retention gap), then streams live, so
// no event is lost in the reconnect gap.
//
// Replay here is backed by an in-memory MemLog (bounded retention), which fits
// single-writer owners, tests, and small fleets. For durable, cross-restart
// replay, swap the MemLog-backed RecordReader for one over your durable log
// (Kafka, etc.); nothing else changes, because RecordReader is the seam.
package refserver

import (
	"sync/atomic"
	"time"

	"github.com/sagarsinghdev/coherent/examples/grpc/gen/coherentv1"
	"github.com/sagarsinghdev/coherent/server"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Server is an in-memory InvalidationService with watermark replay. The zero
// value is not usable; call New.
type Server struct {
	coherentv1.UnimplementedInvalidationServiceServer
	mgr    *server.ConnectionManager[*coherentv1.InvalidationEvent]
	log    *server.MemLog
	replay *server.ReplayService
	lastTS atomic.Int64
}

// New returns a Server whose per-consumer send buffers hold bufSize events and
// whose replay log retains the newest retention events for reconnecting
// consumers.
func New(bufSize, retention int) *Server {
	log := server.NewMemLog(retention)
	return &Server{
		mgr:    server.NewConnectionManager[*coherentv1.InvalidationEvent](bufSize),
		log:    log,
		replay: server.NewReplayService(func() (server.RecordReader, error) { return log.NewReader(), nil }),
	}
}

// Register installs s on gs. Call before gs.Serve.
func (s *Server) Register(gs *grpc.Server) {
	coherentv1.RegisterInvalidationServiceServer(gs, s)
}

// Subscribe streams invalidation events to one consumer:
//
//  1. register the live channel first (so nothing published mid-replay is lost),
//  2. replay events after req.ResumeAfterMs — or send one cache-clear if the
//     watermark predates the retained history (retention gap),
//  3. drain events buffered during replay and stream live.
//
// A fresh consumer (ResumeAfterMs == 0) skips replay; its client emits a
// clear-on-connect instead.
func (s *Server) Subscribe(req *coherentv1.SubscribeRequest, stream coherentv1.InvalidationService_SubscribeServer) error {
	id := req.GetSubscriberId()
	ch := s.mgr.Register(id) // (1) register BEFORE replay
	defer s.mgr.Deregister(id)

	ctx := stream.Context()

	if req.GetResumeAfterMs() > 0 { // (2) replay what was missed
		err := s.replay.Replay(ctx, req.GetResumeAfterMs(),
			func(raw []byte) error {
				ev := &coherentv1.InvalidationEvent{}
				if err := proto.Unmarshal(raw, ev); err != nil {
					return err
				}
				return stream.Send(ev)
			},
			func() error {
				return stream.Send(&coherentv1.InvalidationEvent{IsCacheClear: true})
			},
		)
		if err != nil {
			return err
		}
	}

	for { // (3) drain buffered + stream live
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

// Publish broadcasts a key-level invalidation to all connected consumers, appends
// it to the replay log, and returns the strictly-increasing timestamp (Unix ms)
// assigned to it. Call it after committing a mutation to the source of truth.
func (s *Server) Publish(key, eventType string) int64 {
	return s.emit(&coherentv1.InvalidationEvent{Key: key, EventType: eventType})
}

// PublishClear broadcasts a cache-clear signal and records it in the replay log.
func (s *Server) PublishClear() int64 {
	return s.emit(&coherentv1.InvalidationEvent{IsCacheClear: true})
}

// emit assigns a strictly-increasing timestamp, appends the event to the replay
// log, then broadcasts it. Append-before-broadcast guarantees that any event a
// reconnecting consumer might observe live is already replayable — the other half
// of register-before-replay, together closing the reconnect gap.
func (s *Server) emit(ev *coherentv1.InvalidationEvent) int64 {
	ts := s.nextTS()
	ev.TimestampMs = ts
	if raw, err := proto.Marshal(ev); err == nil {
		s.log.Append(server.LogRecord{Payload: raw, TimestampMs: ts})
	}
	s.mgr.Broadcast(ev)
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
