package logs

import (
	"strings"
	"testing"
)

func TestWriteEmitsCompleteLines(t *testing.T) {
	rb := NewRingBuffer(10, nil)
	n, err := rb.Write([]byte("a\nb\nc"))
	if err != nil || n != 5 {
		t.Fatalf("Write = %d, %v; want 5, nil", n, err)
	}
	list := rb.List()
	if len(list) != 2 {
		t.Fatalf("got %d entries, want 2 (partial 'c' must stay buffered)", len(list))
	}
	if list[0].Message != "a" || list[1].Message != "b" {
		t.Fatalf("unexpected entries: %+v", list)
	}
	// Flushing the pending line must complete it.
	rb.Write([]byte("d\n"))
	list = rb.List()
	if len(list) != 3 || list[2].Message != "cd" {
		t.Fatalf("flush: got %+v, want last entry 'cd'", list)
	}
}

func TestRingMaxBoundsEntries(t *testing.T) {
	rb := NewRingBuffer(5, nil)
	for i := 0; i < 10; i++ {
		rb.Write([]byte("line\n"))
	}
	if got := len(rb.List()); got != 5 {
		t.Fatalf("got %d entries, want 5", got)
	}
}

func TestPartialBufferBounded(t *testing.T) {
	rb := NewRingBuffer(10, nil)
	big := make([]byte, maxPartialBytes*2)
	for i := range big {
		big[i] = 'x'
	}
	if _, err := rb.Write(big); err != nil {
		t.Fatal(err)
	}
	if got := len(rb.partial); got > maxPartialBytes {
		t.Fatalf("partial buffer exceeded cap: %d bytes", got)
	}
	list := rb.List()
	if len(list) == 0 {
		t.Fatal("expected a truncated entry to be emitted")
	}
	if !strings.Contains(list[len(list)-1].Message, "tronquée") {
		t.Fatalf("expected truncation marker, got: %.60q", list[len(list)-1].Message)
	}
}
