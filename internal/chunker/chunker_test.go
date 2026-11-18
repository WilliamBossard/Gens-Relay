package chunker

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestFileHandleCacheReadWriteAndClose(t *testing.T) {
	cache := NewFileHandleCache(2, 5*time.Second)
	defer cache.CloseAll()

	p1 := filepath.Join(t.TempDir(), "a.bin")
	p2 := filepath.Join(t.TempDir(), "b.bin")
	p3 := filepath.Join(t.TempDir(), "c.bin")

	data := []byte("hello-chunker-cache-12345")
	if err := cache.WriteAt(p1, 0, data); err != nil {
		t.Fatal(err)
	}
	if err := cache.WriteAt(p2, 0, data); err != nil {
		t.Fatal(err)
	}
	// Trigger LRU eviction (maxOpen = 2)
	if err := cache.WriteAt(p3, 0, data); err != nil {
		t.Fatal(err)
	}
	cache.ClosePath(p1)
	cache.ClosePath(p2)
	cache.ClosePath(p3)

	dst := make([]byte, len(data))
	if err := cache.ReadBlockInto(p1, 0, len(data), HashBytes(data), dst); err != nil {
		t.Fatalf("ReadBlockInto failed: %v", err)
	}
	if !bytes.Equal(dst, data) {
		t.Fatalf("expected %q, got %q", data, dst)
	}
}

func TestFileHandleCacheConcurrentSafety(t *testing.T) {
	cache := NewFileHandleCache(3, 50*time.Millisecond)
	defer cache.CloseAll()

	dir := t.TempDir()
	paths := make([]string, 8)
	payload := []byte("concurrent-block-payload-0123456789")
	hash := HashBytes(payload)
	for i := range paths {
		paths[i] = filepath.Join(dir, fmt.Sprintf("f%d.bin", i))
		if err := cache.WriteAt(paths[i], 0, payload); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			dst := make([]byte, len(payload))
			for iter := 0; iter < 40; iter++ {
				p := paths[(id+iter)%len(paths)]
				if err := cache.ReadBlockInto(p, 0, len(payload), hash, dst); err != nil {
					errs <- err
					return
				}
				if iter%10 == 0 {
					cache.ClosePath(p)
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent ReadBlockInto error: %v", err)
	}
}
