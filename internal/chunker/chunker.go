package chunker

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
	"github.com/zeebo/blake3"
)

// DefaultBlockSize is 512 KiB, balancing metadata overhead and delta granularity.
const DefaultBlockSize = 512 * 1024

var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, DefaultBlockSize)
		return &b
	},
}

// GetBuffer borrows a DefaultBlockSize buffer from the pool.
func GetBuffer() *[]byte {
	return bufPool.Get().(*[]byte)
}

// PutBuffer returns a borrowed buffer to the pool if its capacity matches DefaultBlockSize.
func PutBuffer(ptr *[]byte) {
	if ptr != nil && cap(*ptr) >= DefaultBlockSize {
		*ptr = (*ptr)[:DefaultBlockSize]
		bufPool.Put(ptr)
	}
}

// HashFileBlocks computes 512 KiB BLAKE3 block hashes and the whole-file BLAKE3 hash in a single read pass.
func HashFileBlocks(path string, blockSize int) (blocks []protocol.BlockInfo, fileHash string, size int64, err error) {
	if blockSize <= 0 {
		blockSize = DefaultBlockSize
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, "", 0, err
	}
	defer f.Close()

	var buf []byte
	if blockSize == DefaultBlockSize {
		ptr := bufPool.Get().(*[]byte)
		defer bufPool.Put(ptr)
		buf = *ptr
	} else {
		buf = make([]byte, blockSize)
	}

	fullHasher := blake3.New()
	var offset int64

	for {
		n, readErr := io.ReadFull(f, buf)
		if n > 0 {
			chunk := buf[:n]
			_, _ = fullHasher.Write(chunk)
			sum := blake3.Sum256(chunk)
			blocks = append(blocks, protocol.BlockInfo{
				Offset: offset,
				Size:   n,
				Hash:   hex.EncodeToString(sum[:]),
			})
			offset += int64(n)
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			return nil, "", 0, readErr
		}
	}

	return blocks, hex.EncodeToString(fullHasher.Sum(nil)), offset, nil
}

// HashBytes computes the hex-encoded BLAKE3 digest of a slice.
func HashBytes(data []byte) string {
	sum := blake3.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// ReadBlock reads a specific byte range from path and optionally verifies its BLAKE3 hash.
func ReadBlock(path string, offset int64, size int, expectedHash string) ([]byte, error) {
	buf := make([]byte, size)
	if err := ReadBlockInto(path, offset, size, expectedHash, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// ReadBlockInto reads a specific byte range from path into dst[:size] and optionally verifies its BLAKE3 hash.
func ReadBlockInto(path string, offset int64, size int, expectedHash string, dst []byte) error {
	if len(dst) < size {
		return fmt.Errorf("destination buffer too small: %d < %d", len(dst), size)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := dst[:size]
	n, err := f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return err
	}
	if n != size {
		return fmt.Errorf("short block read: got %d, want %d", n, size)
	}
	if expectedHash != "" && HashBytes(buf) != expectedHash {
		return fmt.Errorf("block hash mismatch at offset %d", offset)
	}
	return nil
}
