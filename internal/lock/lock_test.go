package lock

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLockNotStaleAfterSleepWhenProcessAlive(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "gens-daemon.lock")
	if err := os.WriteFile(lockPath, []byte(fmt.Sprintf("%d", os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(lockPath, old, old); err != nil {
		t.Fatal(err)
	}
	if isLockStale(lockPath) {
		t.Fatal("expected lock held by live PID to NOT be stale even with old mtime")
	}
}
