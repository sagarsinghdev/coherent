package server

import "testing"

func TestRegisterBroadcastDeregister(t *testing.T) {
	m := NewConnectionManager[int](4)
	ch := m.Register("c1")
	if m.Active() != 1 {
		t.Fatalf("Active = %d; want 1", m.Active())
	}

	m.Broadcast(7)
	select {
	case v := <-ch:
		if v != 7 {
			t.Fatalf("received %d; want 7", v)
		}
	default:
		t.Fatal("expected broadcast to deliver")
	}
	if m.Sent() != 1 {
		t.Fatalf("Sent = %d; want 1", m.Sent())
	}

	m.Deregister("c1")
	if m.Active() != 0 {
		t.Fatalf("Active = %d; want 0", m.Active())
	}
	if _, open := <-ch; open {
		t.Fatal("channel should be closed after Deregister")
	}
	m.Deregister("c1") // idempotent: must not panic
}

func TestBroadcastDropsWhenFull(t *testing.T) {
	m := NewConnectionManager[int](1)
	_ = m.Register("c1") // never drained

	m.Broadcast(1) // fills the buffer
	m.Broadcast(2) // dropped

	if m.Sent() != 1 {
		t.Fatalf("Sent = %d; want 1", m.Sent())
	}
	if m.Dropped() != 1 {
		t.Fatalf("Dropped = %d; want 1", m.Dropped())
	}
}

func TestRegisterReplaceClosesOld(t *testing.T) {
	m := NewConnectionManager[int](1)
	old := m.Register("c1")
	_ = m.Register("c1") // replaces
	if _, open := <-old; open {
		t.Fatal("old channel should be closed when id re-registers")
	}
	if m.Active() != 1 {
		t.Fatalf("Active = %d; want 1", m.Active())
	}
}
