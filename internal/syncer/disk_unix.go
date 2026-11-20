//go:build !windows

package syncer

import (
	"os"
	"path/filepath"
	"syscall"
)

// AvailableDiskBytes returns the free bytes available to unprivileged callers on the volume containing path.
func AvailableDiskBytes(path string) (uint64, error) {
	probe := filepath.Clean(path)
	for {
		if _, err := os.Stat(probe); err == nil {
			break
		}
		parent := filepath.Dir(probe)
		if parent == probe {
			break
		}
		probe = parent
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(probe, &st); err != nil {
		return 0, err
	}
	return st.Bavail * uint64(st.Bsize), nil
}
