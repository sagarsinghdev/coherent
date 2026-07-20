package server

import "context"

// Record is a single stored event to be replayed. TimestampMs is its source time
// in Unix milliseconds (informational); Value is the opaque encoded event.
type Record struct {
	Value       []byte
	TimestampMs int64
}

// RecordReader streams stored records in timestamp order for replay. It abstracts
// the durable log (Kafka, a database, an object store, ...). Implementations
// should create an ephemeral, non-committing reader so replay does not disturb
// primary consumer offsets.
type RecordReader interface {
	// Seek positions the reader just after resumeAfterMs and reports whether a
	// retention gap exists — that is, whether resumeAfterMs precedes the earliest
	// still-available record, meaning the consumer's history cannot be fully
	// reconstructed.
	Seek(ctx context.Context, resumeAfterMs int64) (gap bool, err error)
	// Next returns the next record. ok is false when the reader has caught up to
	// the present (within its lag tolerance) or ctx is done; err is non-nil only
	// on a genuine failure.
	Next(ctx context.Context) (rec Record, ok bool, err error)
	// Close releases reader resources.
	Close() error
}

// ReplayService replays events a reconnecting consumer missed, based on a
// watermark, over a caller-provided RecordReader.
type ReplayService struct {
	newReader func() (RecordReader, error)
}

// NewReplayService returns a ReplayService that obtains a fresh RecordReader for
// each replay via newReader.
func NewReplayService(newReader func() (RecordReader, error)) *ReplayService {
	return &ReplayService{newReader: newReader}
}

// Replay streams the records after resumeAfterMs to send, in order.
//
// If Seek reports a retention gap (resumeAfterMs is older than the earliest
// retained record), the consumer's history cannot be reconstructed: sendClear is
// invoked once to instruct the consumer to flush its cache and lazily re-fill,
// and Replay returns without streaming further. Otherwise Replay streams every
// record after the resume point and returns nil once the reader has caught up.
func (s *ReplayService) Replay(
	ctx context.Context,
	resumeAfterMs int64,
	send func(value []byte) error,
	sendClear func() error,
) error {
	r, err := s.newReader()
	if err != nil {
		return err
	}
	defer r.Close()

	gap, err := r.Seek(ctx, resumeAfterMs)
	if err != nil {
		return err
	}
	if gap {
		return sendClear()
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rec, ok, err := r.Next(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil // caught up
		}
		if err := send(rec.Value); err != nil {
			return err
		}
	}
}
