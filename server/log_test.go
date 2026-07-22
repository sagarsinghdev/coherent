package server

import (
	"context"
	"testing"
)

// drain reads a memReader to completion and returns the payloads as strings.
func drain(t *testing.T, r RecordReader) []string {
	t.Helper()
	var out []string
	for {
		rec, ok, err := r.Next(context.Background())
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			return out
		}
		out = append(out, string(rec.Value))
	}
}

func appendN(l *MemLog, kv ...any) {
	for i := 0; i+1 < len(kv); i += 2 {
		l.Append(LogRecord{Payload: []byte(kv[i].(string)), TimestampMs: int64(kv[i+1].(int))})
	}
}

func TestMemLogRetentionTrims(t *testing.T) {
	l := NewMemLog(3)
	appendN(l, "a", 10, "b", 20, "c", 30, "d", 40, "e", 50) // a,b evicted
	if l.Len() != 3 {
		t.Fatalf("Len = %d; want 3", l.Len())
	}
	// Replay from the very beginning: with drops, oldest (30) > 0-ish resume,
	// but resume 0 means "from the start" — a fresh consumer never calls replay.
	// Resume after 20 should still gap because c(30) is oldest and 20 < 30 with drops.
	r := l.NewReader()
	gap, err := r.Seek(context.Background(), 20)
	if err != nil {
		t.Fatal(err)
	}
	if !gap {
		t.Fatal("expected retention gap: resume 20 predates oldest retained (30) after drops")
	}
}

func TestMemLogReplaysTail(t *testing.T) {
	l := NewMemLog(10)
	appendN(l, "a", 10, "b", 20, "c", 30)
	r := l.NewReader()
	gap, err := r.Seek(context.Background(), 10) // resume after 10 -> want b, c
	if err != nil {
		t.Fatal(err)
	}
	if gap {
		t.Fatal("unexpected gap; nothing was dropped")
	}
	got := drain(t, r)
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("replayed %v; want [b c]", got)
	}
}

func TestMemLogNoGapWhenNothingDropped(t *testing.T) {
	l := NewMemLog(10)
	appendN(l, "x", 100, "y", 200)
	r := l.NewReader()
	// Resume 50 predates oldest (100) but nothing was dropped: 100 is the true
	// start, so there is no gap — replay everything after 50.
	gap, err := r.Seek(context.Background(), 50)
	if err != nil {
		t.Fatal(err)
	}
	if gap {
		t.Fatal("did not expect a gap when no records were dropped")
	}
	if got := drain(t, r); len(got) != 2 || got[0] != "x" || got[1] != "y" {
		t.Fatalf("replayed %v; want [x y]", got)
	}
}

func TestMemLogCaughtUpResumeAfterNewest(t *testing.T) {
	l := NewMemLog(10)
	appendN(l, "a", 10, "b", 20)
	r := l.NewReader()
	gap, err := r.Seek(context.Background(), 20) // caller already has everything
	if err != nil {
		t.Fatal(err)
	}
	if gap {
		t.Fatal("unexpected gap")
	}
	if got := drain(t, r); len(got) != 0 {
		t.Fatalf("replayed %v; want nothing", got)
	}
}

func TestMemLogEmpty(t *testing.T) {
	l := NewMemLog(4)
	r := l.NewReader()
	gap, err := r.Seek(context.Background(), 5)
	if err != nil || gap {
		t.Fatalf("empty log: gap=%v err=%v; want false,nil", gap, err)
	}
	if got := drain(t, r); len(got) != 0 {
		t.Fatalf("replayed %v; want nothing", got)
	}
}

func TestMemLogIndependentReaders(t *testing.T) {
	l := NewMemLog(10)
	appendN(l, "a", 10, "b", 20)
	r1 := l.NewReader()
	if _, err := r1.Seek(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	// Append after r1's snapshot; r1 must not see it, a fresh reader must.
	appendN(l, "c", 30)
	if got := drain(t, r1); len(got) != 2 {
		t.Fatalf("r1 saw %v; want 2 records from its snapshot", got)
	}
	r2 := l.NewReader()
	if _, err := r2.Seek(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if got := drain(t, r2); len(got) != 3 {
		t.Fatalf("r2 saw %v; want 3 records", got)
	}
}
