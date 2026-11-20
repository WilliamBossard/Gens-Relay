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

// DefaultBlockSize is 512 KiB, used when a file size is unknown or in the default tier.
const DefaultBlockSize = 512 * 1024

// MaxBlockSize caps adaptive blocks at 16 MiB.
const MaxBlockSize = 16 * 1024 * 1024

// AdaptiveBlockSize selects a block size from 256 KiB up to 16 MiB based on file size
// so files >100 GiB remain within a few thousand blocks.
func AdaptiveBlockSize(fileSize int64) int {
	switch {
	case fileSize <= 128*1024*1024:
		return 256 * 1024
	case fileSize <= 1024*1024*1024:
		return 512 * 1024
	case fileSize <= 8*1024*1024*1024:
		return 2 * 1024 * 1024
	case fileSize <= 32*1024*1024*1024:
		return 4 * 1024 * 1024
	case fileSize <= 128*1024*1024*1024:
		return 8 * 1024 * 1024
	default:
		return 16 * 1024 * 1024
	}
}

var (
	pool256K = newSizedPool(256 * 1024)
	pool512K = newSizedPool(512 * 1024)
	pool2M   = newSizedPool(2 * 1024 * 1024)
	pool4M   = newSizedPool(4 * 1024 * 1024)
	pool8M   = newSizedPool(8 * 1024 * 1024)
	pool16M  = newSizedPool(16 * 1024 * 1024)
)

func newSizedPool(sz int) *sync.Pool {
	return &sync.Pool{
		New: func() any {
			b := make([]byte, sz)
			return &b
		},
	}
}

// GetBuffer borrows a DefaultBlockSize (512 KiB) buffer from the pool.
func GetBuffer() *[]byte {
	return pool512K.Get().(*[]byte)
}

// PutBuffer returns a borrowed 512 KiB buffer (or tiered buffer) to the appropriate pool.
func PutBuffer(ptr *[]byte) {
	PutBufferSized(ptr)
}

// GetBufferSize borrows a pooled buffer of at least size bytes.
func GetBufferSize(size int) *[]byte {
	switch {
	case size <= 256*1024:
		return pool256K.Get().(*[]byte)
	case size <= 512*1024:
		return pool512K.Get().(*[]byte)
	case size <= 2*1024*1024:
		return pool2M.Get().(*[]byte)
	case size <= 4*1024*1024:
		return pool4M.Get().(*[]byte)
	case size <= 8*1024*1024:
		return pool8M.Get().(*[]byte)
	case size <= 16*1024*1024:
		return pool16M.Get().(*[]byte)
	default:
		b := make([]byte, size)
		return &b
	}
}

// PutBufferSized returns a buffer borrowed via GetBuffer/GetBufferSize to its matching tier pool.
func PutBufferSized(ptr *[]byte) {
	if ptr == nil {
		return
	}
	c := cap(*ptr)
	switch {
	case c == 256*1024:
		*ptr = (*ptr)[:256*1024]
		pool256K.Put(ptr)
	case c == 512*1024:
		*ptr = (*ptr)[:512*1024]
		pool512K.Put(ptr)
	case c == 2*1024*1024:
		*ptr = (*ptr)[:2*1024*1024]
		pool2M.Put(ptr)
	case c == 4*1024*1024:
		*ptr = (*ptr)[:4*1024*1024]
		pool4M.Put(ptr)
	case c == 8*1024*1024:
		*ptr = (*ptr)[:8*1024*1024]
		pool8M.Put(ptr)
	case c == 16*1024*1024:
		*ptr = (*ptr)[:16*1024*1024]
		pool16M.Put(ptr)
	}
}

// ComputeMerkleFileHash computes a deterministic BLAKE3 Merkle root over the ordered block hashes.
// This eliminates full-file re-reads on the receiving side after block hashes have been verified.
func ComputeMerkleFileHash(blocks []protocol.BlockInfo) string {
	h := blake3.New()
	for _, blk := range blocks {
		_, _ = h.Write([]byte(blk.Hash))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// HashFileBlocks computes BLAKE3 block hashes and the Merkle FileHash in a single pass.
// If blockSize <= 0, an adaptive block size is chosen automatically based on file size.
func HashFileBlocks(path string, blockSize int) (blocks []protocol.BlockInfo, fileHash string, size int64, err error) {
	blocks, fileHash, size, _, err = hashFileBlocksInternal(path, blockSize)
	return
}

// HashFileBlocksAdaptive computes adaptive BLAKE3 block hashes and returns the chosen blockSize.
func HashFileBlocksAdaptive(path string) (blocks []protocol.BlockInfo, fileHash string, size int64, blockSize int, err error) {
	return hashFileBlocksInternal(path, 0)
}

func hashFileBlocksInternal(path string, blockSize int) (blocks []protocol.BlockInfo, fileHash string, size int64, usedBlockSize int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", 0, 0, err
	}
	defer f.Close()

	if blockSize <= 0 {
		if info, sErr := f.Stat(); sErr == nil {
			blockSize = AdaptiveBlockSize(info.Size())
		} else {
			blockSize = DefaultBlockSize
		}
	}

	blocks, fileHash, size, err = HashReaderAdaptive(f, blockSize)
	return blocks, fileHash, size, blockSize, err
}

// HashReaderAdaptive hashes an io.Reader stream into blocks of blockSize and returns block list + Merkle FileHash.
func HashReaderAdaptive(r io.Reader, blockSize int) (blocks []protocol.BlockInfo, fileHash string, size int64, err error) {
	if blockSize <= 0 {
		blockSize = DefaultBlockSize
	}

	ptr := GetBufferSize(blockSize)
	defer PutBufferSized(ptr)
	buf := (*ptr)[:blockSize]

	var offset int64
	for {
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			chunk := buf[:n]
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

	return blocks, ComputeMerkleFileHash(blocks), offset, nil
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
