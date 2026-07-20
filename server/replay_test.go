package server

import (
	"context"
	"testing"
)

type fakeReader struct {
	records []Record
	gap     bool
	pos     int
	closed  bool
}

func (r *fakeReader) Seek(_ context.Context, _ int64) (bool, error) { return r.gap, nil }

func (r *fakeReader) Next(_ context.Context) (Record, bool, error) {
	if r.pos >= len(r.records) {
		return Record{}, false, nil
	}
	rec := r.records[r.pos]
	r.pos++
	return rec, true, nil
}

func (r *fakeReader) Close() error { r.closed = true; return nil }

func TestReplayStreamsRecords(t *testing.T) {
	reader := &fakeReader{records: []Record{
		{Value: []byte("a"), TimestampMs: 11},
		{Value: []byte("b"), TimestampMs: 12},
		{Value: []byte("c"), TimestampMs: 13},
	}}
	svc := NewReplayService(func() (RecordReader, error) { return reader, nil })

	var sent []string
	var cleared int
	err := svc.Replay(context.Background(), 10,
		func(v []byte) error { sent = append(sent, string(v)); return nil },
		func() error { cleared++; return nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if cleared != 0 {
		t.Fatalf("sendClear called %d times; want 0", cleared)
	}
	if len(sent) != 3 || sent[0] != "a" || sent[2] != "c" {
		t.Fatalf("sent = %v; want [a b c]", sent)
	}
	if !reader.closed {
		t.Fatal("reader was not closed")
	}
}

func TestReplayRetentionGapClearsOnce(t *testing.T) {
	reader := &fakeReader{gap: true, records: []Record{{Value: []byte("stale"), TimestampMs: 1}}}
	svc := NewReplayService(func() (RecordReader, error) { return reader, nil })

	var sent []string
	var cleared int
	err := svc.Replay(context.Background(), 100,
		func(v []byte) error { sent = append(sent, string(v)); return nil },
		func() error { cleared++; return nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if cleared != 1 {
		t.Fatalf("sendClear called %d times; want 1", cleared)
	}
	if len(sent) != 0 {
		t.Fatalf("sent = %v; want none on retention gap", sent)
	}
}
