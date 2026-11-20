package chunker

import (
	"container/list"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type cachedHandle struct {
	key       string
	cleanPath string
	file      *os.File
	writable  bool
	lastUsed  time.Time
	elem      *list.Element
	refs      int
	closing   bool
	uncached  bool
}

// FileHandleCache pools open *os.File handles for high-throughput random ReadAt/WriteAt
// while reference-counting active I/O and evicting idle handles so Windows file locks
// are never closed mid-operation and are released promptly when idle.
// File Open and Close syscalls are performed outside the cache mutex to prevent stalls.
type FileHandleCache struct {
	mu             sync.Mutex
	cond           *sync.Cond
	maxOpen        int
	idleTTL        time.Duration
	entries        map[string]*cachedHandle
	activeUncached map[string]int
	lru            *list.List
	stopCh         chan struct{}
	closed         bool
}

func NewFileHandleCache(maxOpen int, idleTTL time.Duration) *FileHandleCache {
	if maxOpen <= 0 {
		maxOpen = 48
	}
	if idleTTL <= 0 {
		idleTTL = 5 * time.Second
	}
	c := &FileHandleCache{
		maxOpen:        maxOpen,
		idleTTL:        idleTTL,
		entries:        make(map[string]*cachedHandle, maxOpen),
		activeUncached: make(map[string]int),
		lru:            list.New(),
		stopCh:         make(chan struct{}),
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
			var toClose []*os.File
			if !c.closed {
				toClose = c.evictExpiredLocked(now)
			}
			c.mu.Unlock()
			closeAllFiles(toClose)
		}
	}
}

func closeAllFiles(files []*os.File) {
	for _, f := range files {
		if f != nil {
			_ = f.Close()
		}
	}
}

func (c *FileHandleCache) acquire(path string, writable bool) (*cachedHandle, error) {
	cleanPath := filepath.Clean(path)
	key := fmt.Sprintf("%v|%s", writable, cleanPath)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, os.ErrClosed
	}

	var toClose []*os.File
	toClose = append(toClose, c.evictExpiredLocked(time.Now())...)

	if ent, ok := c.entries[key]; ok {
		ent.lastUsed = time.Now()
		ent.refs++
		c.lru.MoveToFront(ent.elem)
		c.mu.Unlock()
		closeAllFiles(toClose)
		return ent, nil
	}

	for len(c.entries) >= c.maxOpen {
		f := c.evictOldestIdleLocked()
		if f == nil {
			break
		}
		toClose = append(toClose, f)
	}
	c.mu.Unlock()

	closeAllFiles(toClose)

	// Perform OS Open outside mutex so slow disks/AV scans do not block other workers.
	var f *os.File
	var err error
	if writable {
		f, err = os.OpenFile(cleanPath, os.O_CREATE|os.O_WRONLY, 0644)
	} else {
		f, err = os.Open(cleanPath)
	}
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = f.Close()
		return nil, os.ErrClosed
	}
	if ent, ok := c.entries[key]; ok {
		ent.lastUsed = time.Now()
		ent.refs++
		c.lru.MoveToFront(ent.elem)
		c.mu.Unlock()
		_ = f.Close()
		return ent, nil
	}

	var postEvict []*os.File
	for len(c.entries) >= c.maxOpen {
		ef := c.evictOldestIdleLocked()
		if ef == nil {
			break
		}
		postEvict = append(postEvict, ef)
	}

	if len(c.entries) >= c.maxOpen {
		// All cached slots are actively pinned by concurrent I/O; track uncached handle so ClosePath waits for it.
		c.activeUncached[cleanPath]++
		c.mu.Unlock()
		closeAllFiles(postEvict)
		return &cachedHandle{
			key:       key,
			cleanPath: cleanPath,
			file:      f,
			writable:  writable,
			refs:      1,
			uncached:  true,
		}, nil
	}

	ent := &cachedHandle{
		key:       key,
		cleanPath: cleanPath,
		file:      f,
		writable:  writable,
		lastUsed:  time.Now(),
		refs:      1,
	}
	ent.elem = c.lru.PushFront(ent)
	c.entries[key] = ent
	c.mu.Unlock()
	closeAllFiles(postEvict)
	return ent, nil
}

func (c *FileHandleCache) release(ent *cachedHandle, invalidate bool) {
	var toClose *os.File

	c.mu.Lock()
	ent.refs--
	ent.lastUsed = time.Now()
	if ent.uncached {
		if c.activeUncached[ent.cleanPath] <= 1 {
			delete(c.activeUncached, ent.cleanPath)
		} else {
			c.activeUncached[ent.cleanPath]--
		}
		toClose = ent.file
		ent.file = nil
		c.cond.Broadcast()
		c.mu.Unlock()
		if toClose != nil {
			_ = toClose.Close()
		}
		return
	}

	if invalidate && !ent.closing {
		if cur, ok := c.entries[ent.key]; ok && cur == ent {
			c.lru.Remove(ent.elem)
			delete(c.entries, ent.key)
		}
		ent.closing = true
	}
	if ent.closing && ent.refs <= 0 {
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

func (c *FileHandleCache) evictOldestIdleLocked() *os.File {
	for elem := c.lru.Back(); elem != nil; elem = elem.Prev() {
		ent := elem.Value.(*cachedHandle)
		if ent.refs == 0 {
			c.lru.Remove(elem)
			delete(c.entries, ent.key)
			f := ent.file
			ent.file = nil
			return f
		}
	}
	return nil
}

func (c *FileHandleCache) evictExpiredLocked(now time.Time) []*os.File {
	var toClose []*os.File
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
				toClose = append(toClose, ent.file)
				ent.file = nil
			}
		}
		elem = prev
	}
	return toClose
}

// ReadBlockInto reads a byte range using a pooled file handle and optionally verifies BLAKE3.
// If a cached handle returns a hash mismatch (e.g. file replaced underneath on Unix), it invalidates
// the cached handle and retries once on a freshly opened handle.
func (c *FileHandleCache) ReadBlockInto(path string, offset int64, size int, expectedHash string, dst []byte) error {
	if len(dst) < size {
		return fmt.Errorf("destination buffer too small: %d < %d", len(dst), size)
	}
	for attempt := 0; attempt < 2; attempt++ {
		ent, err := c.acquire(path, false)
		if err != nil {
			return err
		}
		buf := dst[:size]
		n, rErr := ent.file.ReadAt(buf, offset)
		if rErr != nil && rErr != io.EOF {
			c.release(ent, true)
			return rErr
		}
		if n != size {
			if attempt == 0 && !ent.uncached {
				c.release(ent, true)
				continue
			}
			c.release(ent, false)
			return fmt.Errorf("short block read: got %d, want %d", n, size)
		}
		if expectedHash != "" && HashBytes(buf) != expectedHash {
			if attempt == 0 && !ent.uncached {
				c.release(ent, true)
				continue
			}
			c.release(ent, true)
			return fmt.Errorf("block hash mismatch at offset %d", offset)
		}
		c.release(ent, false)
		return nil
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

// ClosePath closes any cached read/write handles for path and waits for all in-flight I/O
// (including active uncached handles) to finish (required before Rename/Remove on Windows).
func (c *FileHandleCache) ClosePath(path string) {
	cleanPath := filepath.Clean(path)
	var toClose []*os.File

	c.mu.Lock()
	for _, writable := range []bool{false, true} {
		key := fmt.Sprintf("%v|%s", writable, cleanPath)
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
	for c.activeUncached[cleanPath] > 0 {
		c.cond.Wait()
	}
	c.mu.Unlock()

	closeAllFiles(toClose)
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
	for len(c.activeUncached) > 0 {
		c.cond.Wait()
	}
	c.lru.Init()
	c.mu.Unlock()

	closeAllFiles(toClose)
}
