package lock

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// InstanceLock prevents two daemon processes from sharing the same data directory simultaneously.
type InstanceLock struct {
	mu            sync.Mutex
	path          string
	heartbeatDone chan struct{}
}

func Acquire(dataDir string) (*InstanceLock, error) {
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, err
	}
	lockPath := filepath.Join(dataDir, "gens-daemon.lock")

	for attempt := 0; attempt < 5; attempt++ {
		if _, err := os.Stat(lockPath); err == nil {
			if isLockStale(lockPath) {
				staleTmp := fmt.Sprintf("%s.stale.%d.%d", lockPath, os.Getpid(), attempt)
				if renErr := os.Rename(lockPath, staleTmp); renErr == nil {
					_ = os.Remove(staleTmp)
				} else if !os.IsNotExist(renErr) {
					time.Sleep(50 * time.Millisecond)
					continue
				}
			} else {
				return nil, fmt.Errorf("another gens-daemon instance is already running (lock: %s)", lockPath)
			}
		}

		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			_, writeErr := f.WriteString(fmt.Sprintf("%d", os.Getpid()))
			syncErr := f.Sync()
			closeErr := f.Close()
			if writeErr != nil || syncErr != nil || closeErr != nil {
				_ = os.Remove(lockPath)
				time.Sleep(50 * time.Millisecond)
				continue
			}

			l := &InstanceLock{
				path:          lockPath,
				heartbeatDone: make(chan struct{}),
			}
			l.startHeartbeat()
			return l, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, fmt.Errorf("failed to acquire lock %s", lockPath)
}

func (l *InstanceLock) startHeartbeat() {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-l.heartbeatDone:
				return
			case t := <-ticker.C:
				_ = os.Chtimes(l.path, t, t)
			}
		}
	}()
}

func (l *InstanceLock) Release() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.heartbeatDone != nil {
		close(l.heartbeatDone)
		l.heartbeatDone = nil
	}

	if content, err := os.ReadFile(l.path); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(content))); err == nil && pid == os.Getpid() {
			_ = os.Remove(l.path)
		}
	}
}

func isLockStale(lockPath string) bool {
	info, err := os.Stat(lockPath)
	if err != nil {
		return true
	}
	age := time.Since(info.ModTime())
	content, err := os.ReadFile(lockPath)
	if err != nil {
		return true
	}
	trimmed := strings.TrimSpace(string(content))
	if trimmed == "" {
		return age > 2*time.Second
	}
	pid, err := strconv.Atoi(trimmed)
	if err != nil {
		return true
	}
	if !isProcessAlive(pid) {
		return true
	}
	// Process is alive (even after OS sleep/hibernation), so the lock is valid.
	return false
}
