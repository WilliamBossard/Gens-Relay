//go:build windows

package syncer

import (
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"
)

// AvailableDiskBytes returns the free bytes available to the caller on the volume containing path.
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
	ptr, err := windows.UTF16PtrFromString(probe)
	if err != nil {
		return 0, err
	}
	var freeBytesAvail uint64
	err = windows.GetDiskFreeSpaceEx(ptr, &freeBytesAvail, nil, nil)
	if err != nil {
		return 0, err
	}
	_ = unsafe.Sizeof(freeBytesAvail)
	return freeBytesAvail, nil
}
