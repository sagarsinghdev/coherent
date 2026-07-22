package server

import (
	"context"
	"sync"
)

// LogRecord is one event retained for replay. Payload is the opaque, already
// encoded event (for example a marshaled protobuf); TimestampMs is its source
// time in Unix milliseconds and serves as the replay watermark. Append records in
// non-decreasing TimestampMs order.
type LogRecord struct {
	Payload     []byte
	TimestampMs int64
}

// MemLog is a bounded, in-memory, append-only event log for replay. It retains
// the most recent capacity records; older ones are overwritten. It is the
// zero-dependency source side of replay: an owner appends every published event,
// and a reconnecting consumer replays the tail after its watermark via a
// RecordReader obtained from NewReader.
//
// MemLog fits single-writer owners, tests, and small fleets. For durable,
// cross-restart replay, back a RecordReader with a real log (for example Kafka)
// instead — the ReplayService and Subscribe wiring are identical, because
// RecordReader is the seam.
//
// MemLog is safe for concurrent use.
type MemLog struct {
	mu       sync.Mutex
	buf      []LogRecord
	capacity int
	size     int  // number of valid records currently held
	head     int  // index of the oldest valid record
	dropped  bool // whether any record has ever been overwritten for capacity
}

// NewMemLog returns a MemLog that retains the newest capacity records. A capacity
// below 1 is raised to 1.
func NewMemLog(capacity int) *MemLog {
	if capacity < 1 {
		capacity = 1
	}
	return &MemLog{
		buf:      make([]LogRecord, capacity),
		capacity: capacity,
	}
}

// Append adds rec, overwriting the oldest record once capacity is reached.
func (l *MemLog) Append(rec LogRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.size < l.capacity {
		l.buf[(l.head+l.size)%l.capacity] = rec
		l.size++
		return
	}
	l.buf[l.head] = rec
	l.head = (l.head + 1) % l.capacity
	l.dropped = true
}

// Len returns the number of records currently retained.
func (l *MemLog) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.size
}

// snapshot returns the retained records in append order and whether any record
// has been dropped for capacity.
func (l *MemLog) snapshot() ([]LogRecord, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]LogRecord, l.size)
	for i := 0; i < l.size; i++ {
		out[i] = l.buf[(l.head+i)%l.capacity]
	}
	return out, l.dropped
}

// NewReader returns a RecordReader over the log's current contents. Each reader
// takes an independent snapshot when Seek is called, so events appended during a
// replay are delivered via the live path (register-before-replay), not the
// replay, avoiding gaps.
func (l *MemLog) NewReader() RecordReader { return &memReader{log: l} }

// memReader is a RecordReader over a MemLog snapshot.
type memReader struct {
	log  *MemLog
	recs []LogRecord
	pos  int
}

// Seek snapshots the log and positions just after resumeAfterMs. It reports a
// retention gap when the resume point predates the oldest retained record and
// older records have been dropped — meaning events between resumeAfterMs and the
// oldest retained record may have been lost, so the consumer's history cannot be
// fully reconstructed.
func (r *memReader) Seek(_ context.Context, resumeAfterMs int64) (bool, error) {
	recs, dropped := r.log.snapshot()
	r.recs = recs
	r.pos = 0
	if len(recs) == 0 {
		return false, nil
	}
	if dropped && recs[0].TimestampMs > resumeAfterMs {
		return true, nil // retention gap
	}
	for r.pos < len(r.recs) && r.recs[r.pos].TimestampMs <= resumeAfterMs {
		r.pos++
	}
	return false, nil
}

// Next returns the next snapshot record after the resume point. ok is false once
// the snapshot is exhausted.
func (r *memReader) Next(_ context.Context) (Record, bool, error) {
	if r.pos >= len(r.recs) {
		return Record{}, false, nil
	}
	rec := r.recs[r.pos]
	r.pos++
	return Record{Value: rec.Payload, TimestampMs: rec.TimestampMs}, true, nil
}

// Close releases the snapshot. It never errors.
func (r *memReader) Close() error {
	r.recs = nil
	return nil
}
