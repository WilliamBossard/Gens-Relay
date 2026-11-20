package chunker

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	zstdEncPool = sync.Pool{
		New: func() any {
			enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
			return enc
		},
	}
	zstdDecPool = sync.Pool{
		New: func() any {
			dec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20))
			return dec
		},
	}
)

var incompressibleExts = map[string]struct{}{
	".mp4": {}, ".mkv": {}, ".mov": {}, ".avi": {}, ".webm": {}, ".m4v": {},
	".jpg": {}, ".jpeg": {}, ".png": {}, ".webp": {}, ".gif": {}, ".heic": {}, ".avif": {},
	".zip": {}, ".gz": {}, ".tgz": {}, ".bz2": {}, ".xz": {}, ".zst": {}, ".7z": {}, ".rar": {},
	".pdf": {}, ".mp3": {}, ".aac": {}, ".ogg": {}, ".opus": {}, ".flac": {}, ".m4a": {},
	".woff": {}, ".woff2": {}, ".br": {}, ".apk": {}, ".ipa": {}, ".iso": {}, ".dmg": {},
}

// IsCompressibleExt returns false for formats that are already entropy-compressed.
func IsCompressibleExt(relPath string) bool {
	ext := strings.ToLower(filepath.Ext(relPath))
	if ext == "" {
		return true
	}
	_, skip := incompressibleExts[ext]
	return !skip
}

// CompressBlockIfWorthwhile compresses raw using Zstd if relPath is compressible and savings exceed 10%.
// Otherwise it returns raw unchanged and compressed=false.
func CompressBlockIfWorthwhile(relPath string, raw []byte) ([]byte, bool) {
	if len(raw) < 1024 || !IsCompressibleExt(relPath) {
		return raw, false
	}
	enc := zstdEncPool.Get().(*zstd.Encoder)
	dst := make([]byte, 0, len(raw)/2)
	comp := enc.EncodeAll(raw, dst)
	zstdEncPool.Put(enc)

	// Require at least 10% reduction to pay for CPU cost
	if len(comp) >= (len(raw)*9)/10 {
		return raw, false
	}
	return comp, true
}

// DecompressBlock decompresses a Zstd-compressed block into dst (or allocates if dst is too small)
// and verifies the decoded length matches expectedSize.
func DecompressBlock(src, dst []byte, expectedSize int) ([]byte, error) {
	if expectedSize <= 0 || expectedSize > MaxBlockSize {
		return nil, fmt.Errorf("invalid expected block size: %d", expectedSize)
	}
	dec := zstdDecPool.Get().(*zstd.Decoder)
	defer zstdDecPool.Put(dec)

	var outBuf []byte
	if cap(dst) >= expectedSize {
		outBuf = dst[:0]
	} else {
		outBuf = make([]byte, 0, expectedSize)
	}
	out, err := dec.DecodeAll(src, outBuf)
	if err != nil {
		return nil, fmt.Errorf("zstd decode: %w", err)
	}
	if len(out) != expectedSize {
		return nil, fmt.Errorf("zstd decompressed size mismatch: got %d, want %d", len(out), expectedSize)
	}
	return out, nil
}
