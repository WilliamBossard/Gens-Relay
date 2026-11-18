package logs

import (
	"bytes"
	"sync"
	"time"
)

type Entry struct {
	ID        uint64 `json:"id"`
	Timestamp int64  `json:"timestamp"`
	Message   string `json:"message"`
}

// RingBuffer captures log lines in memory for live Web UI inspection.
type RingBuffer struct {
	mu      sync.RWMutex
	max     int
	entries []Entry
	nextID  uint64
	partial []byte
	onEntry func(Entry)
}

func NewRingBuffer(max int, onEntry func(Entry)) *RingBuffer {
	if max <= 0 {
		max = 500
	}
	return &RingBuffer{
		max:     max,
		onEntry: onEntry,
	}
}

func (r *RingBuffer) SetCallback(fn func(Entry)) {
	r.mu.Lock()
	r.onEntry = fn
	r.mu.Unlock()
}

func (r *RingBuffer) Write(p []byte) (int, error) {
	r.mu.Lock()
	r.partial = append(r.partial, p...)
	var emitted []Entry
	for {
		idx := bytes.IndexByte(r.partial, '\n')
		if idx < 0 {
			break
		}
		line := string(bytes.TrimRight(r.partial[:idx], "\r"))
		r.partial = r.partial[idx+1:]
		if line == "" {
			continue
		}
		r.nextID++
		e := Entry{
			ID:        r.nextID,
			Timestamp: time.Now().Unix(),
			Message:   line,
		}
		r.entries = append(r.entries, e)
		if len(r.entries) > r.max {
			r.entries = r.entries[len(r.entries)-r.max:]
		}
		emitted = append(emitted, e)
	}
	cb := r.onEntry
	r.mu.Unlock()

	if cb != nil {
		for _, e := range emitted {
			cb(e)
		}
	}
	return len(p), nil
}

func (r *RingBuffer) List() []Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Entry, len(r.entries))
	copy(out, r.entries)
	return out
}
