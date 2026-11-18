package chunker

import (
	"container/list"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type cachedHandle struct {
	key      string
	file     *os.File
	writable bool
	lastUsed time.Time
	elem     *list.Element
	refs     int
	closing  bool
	uncached bool
}

// FileHandleCache pools open *os.File handles for high-throughput random ReadAt/WriteAt
// while reference-counting active I/O and evicting idle handles so Windows file locks
// are never closed mid-operation and are released promptly when idle.
type FileHandleCache struct {
	mu      sync.Mutex
	cond    *sync.Cond
	maxOpen int
	idleTTL time.Duration
	entries map[string]*cachedHandle
	lru     *list.List
	stopCh  chan struct{}
	closed  bool
}

func NewFileHandleCache(maxOpen int, idleTTL time.Duration) *FileHandleCache {
	if maxOpen <= 0 {
		maxOpen = 48
	}
	if idleTTL <= 0 {
		idleTTL = 5 * time.Second
	}
	c := &FileHandleCache{
		maxOpen: maxOpen,
		idleTTL: idleTTL,
		entries: make(map[string]*cachedHandle, maxOpen),
		lru:     list.New(),
		stopCh:  make(chan struct{}),
	}
	c.cond = sync.NewCond(&c.mu)
	go c.reapLoop()
	return c
}

func (c *FileHandleCache) reapLoop() {
	interval := c.idleTTL / 2
	if interval < 500*time.Millisecond {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case now := <-ticker.C:
			c.mu.Lock()
			if !c.closed {
				c.evictExpiredLocked(now)
			}
			c.mu.Unlock()
		}
	}
}

func (c *FileHandleCache) acquire(path string, writable bool) (*cachedHandle, error) {
	key := fmt.Sprintf("%v|%s", writable, path)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, os.ErrClosed
	}

	c.evictExpiredLocked(time.Now())

	if ent, ok := c.entries[key]; ok {
		ent.lastUsed = time.Now()
		ent.refs++
		c.lru.MoveToFront(ent.elem)
		return ent, nil
	}

	// Open file without holding c.mu if needed? No, keeping open fast; evict idle slots first.
	for len(c.entries) >= c.maxOpen {
		if !c.evictOldestIdleLocked() {
			break
		}
	}

	var f *os.File
	var err error
	if writable {
		f, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	} else {
		f, err = os.Open(path)
	}
	if err != nil {
		return nil, err
	}

	if len(c.entries) >= c.maxOpen {
		// All cached slots are actively pinned by concurrent I/O; use an uncached handle closed on release.
		return &cachedHandle{
			key:      key,
			file:     f,
			writable: writable,
			refs:     1,
			uncached: true,
		}, nil
	}

	ent := &cachedHandle{
		key:      key,
		file:     f,
		writable: writable,
		lastUsed: time.Now(),
		refs:     1,
	}
	ent.elem = c.lru.PushFront(ent)
	c.entries[key] = ent
	return ent, nil
}

func (c *FileHandleCache) release(ent *cachedHandle, invalidate bool) {
	var toClose *os.File

	c.mu.Lock()
	ent.refs--
	ent.lastUsed = time.Now()
	if invalidate && !ent.uncached && !ent.closing {
		if cur, ok := c.entries[ent.key]; ok && cur == ent {
			c.lru.Remove(ent.elem)
			delete(c.entries, ent.key)
		}
		ent.closing = true
	}
	if ent.uncached || (ent.closing && ent.refs <= 0) {
		toClose = ent.file
		ent.file = nil
	}
	if ent.refs <= 0 {
		c.cond.Broadcast()
	}
	c.mu.Unlock()

	if toClose != nil {
		_ = toClose.Close()
	}
}

func (c *FileHandleCache) evictOldestIdleLocked() bool {
	for elem := c.lru.Back(); elem != nil; elem = elem.Prev() {
		ent := elem.Value.(*cachedHandle)
		if ent.refs == 0 {
			c.lru.Remove(elem)
			delete(c.entries, ent.key)
			if ent.file != nil {
				_ = ent.file.Close()
				ent.file = nil
			}
			return true
		}
	}
	return false
}

func (c *FileHandleCache) evictExpiredLocked(now time.Time) {
	for elem := c.lru.Back(); elem != nil; {
		prev := elem.Prev()
		ent := elem.Value.(*cachedHandle)
		if now.Sub(ent.lastUsed) < c.idleTTL {
			break
		}
		if ent.refs == 0 {
			c.lru.Remove(elem)
			delete(c.entries, ent.key)
			if ent.file != nil {
				_ = ent.file.Close()
				ent.file = nil
			}
		}
		elem = prev
	}
}

// ReadBlockInto reads a byte range using a pooled file handle and optionally verifies BLAKE3.
func (c *FileHandleCache) ReadBlockInto(path string, offset int64, size int, expectedHash string, dst []byte) error {
	if len(dst) < size {
		return fmt.Errorf("destination buffer too small: %d < %d", len(dst), size)
	}
	ent, err := c.acquire(path, false)
	if err != nil {
		return err
	}
	buf := dst[:size]
	n, err := ent.file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		c.release(ent, true)
		return err
	}
	c.release(ent, false)

	if n != size {
		return fmt.Errorf("short block read: got %d, want %d", n, size)
	}
	if expectedHash != "" && HashBytes(buf) != expectedHash {
		return fmt.Errorf("block hash mismatch at offset %d", offset)
	}
	return nil
}

// WriteAt writes data at offset using a pooled writable file handle.
func (c *FileHandleCache) WriteAt(path string, offset int64, data []byte) error {
	ent, err := c.acquire(path, true)
	if err != nil {
		return err
	}
	_, err = ent.file.WriteAt(data, offset)
	c.release(ent, err != nil)
	return err
}

// ClosePath closes any cached read/write handles for path and waits for in-flight I/O to finish
// (required before Rename/Remove on Windows).
func (c *FileHandleCache) ClosePath(path string) {
	var toClose []*os.File

	c.mu.Lock()
	for _, writable := range []bool{false, true} {
		key := fmt.Sprintf("%v|%s", writable, path)
		if ent, ok := c.entries[key]; ok {
			c.lru.Remove(ent.elem)
			delete(c.entries, key)
			ent.closing = true
			for ent.refs > 0 {
				c.cond.Wait()
			}
			if ent.file != nil {
				toClose = append(toClose, ent.file)
				ent.file = nil
			}
		}
	}
	c.mu.Unlock()

	for _, f := range toClose {
		_ = f.Close()
	}
}

// CloseAll stops the background reaper and closes all open handles in the cache.
func (c *FileHandleCache) CloseAll() {
	var toClose []*os.File

	c.mu.Lock()
	if !c.closed {
		c.closed = true
		close(c.stopCh)
	}
	for k, ent := range c.entries {
		ent.closing = true
		delete(c.entries, k)
		for ent.refs > 0 {
			c.cond.Wait()
		}
		if ent.file != nil {
			toClose = append(toClose, ent.file)
			ent.file = nil
		}
	}
	c.lru.Init()
	c.mu.Unlock()

	for _, f := range toClose {
		_ = f.Close()
	}
}
