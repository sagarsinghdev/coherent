package server

import (
	"sync"
	"sync/atomic"
)

// ConnectionManager maintains a registry of connected consumers and broadcasts
// events to all of them using non-blocking sends. An event destined for a
// consumer whose buffer is full is dropped and counted rather than blocking the
// broadcast; the consumer self-heals via cache-clear-on-reconnect and TTL.
//
// ConnectionManager is safe for concurrent use.
type ConnectionManager[E any] struct {
	mu      sync.RWMutex
	conns   map[string]chan E
	bufSize int

	sent    atomic.Int64
	dropped atomic.Int64
}

// NewConnectionManager returns a manager whose per-consumer channels have the
// given buffer size. A bufSize below 1 is raised to 1.
func NewConnectionManager[E any](bufSize int) *ConnectionManager[E] {
	if bufSize < 1 {
		bufSize = 1
	}
	return &ConnectionManager[E]{
		conns:   make(map[string]chan E),
		bufSize: bufSize,
	}
}

// Register allocates a buffered channel for id and returns it for the caller to
// stream from. It MUST be called before starting replay for that consumer. If id
// is already registered, the previous channel is closed and replaced.
func (m *ConnectionManager[E]) Register(id string) <-chan E {
	ch := make(chan E, m.bufSize)
	m.mu.Lock()
	if old, ok := m.conns[id]; ok {
		close(old)
	}
	m.conns[id] = ch
	m.mu.Unlock()
	return ch
}

// Deregister removes id and closes its channel. It is safe to call more than
// once.
func (m *ConnectionManager[E]) Deregister(id string) {
	m.mu.Lock()
	if ch, ok := m.conns[id]; ok {
		delete(m.conns, id)
		close(ch)
	}
	m.mu.Unlock()
}

// Broadcast attempts a non-blocking send of ev to every connected consumer.
// Sends to full channels are dropped and counted (see Dropped).
func (m *ConnectionManager[E]) Broadcast(ev E) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, ch := range m.conns {
		select {
		case ch <- ev:
			m.sent.Add(1)
		default:
			m.dropped.Add(1)
		}
	}
}

// Active returns the number of connected consumers.
func (m *ConnectionManager[E]) Active() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.conns)
}

// Sent returns the total number of successful broadcast sends.
func (m *ConnectionManager[E]) Sent() int64 { return m.sent.Load() }

// Dropped returns the total number of broadcast sends dropped due to full
// consumer buffers.
func (m *ConnectionManager[E]) Dropped() int64 { return m.dropped.Load() }
