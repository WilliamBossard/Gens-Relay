package chunker

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestAdaptiveBlockSizeAndMerkleHash(t *testing.T) {
	if AdaptiveBlockSize(1024) != 256*1024 {
		t.Fatalf("expected 256 KiB for small file")
	}
	if AdaptiveBlockSize(512*1024*1024) != 512*1024 {
		t.Fatalf("expected 512 KiB for 512 MiB file")
	}
	if AdaptiveBlockSize(4*1024*1024*1024) != 2*1024*1024 {
		t.Fatalf("expected 2 MiB for 4 GiB file")
	}
	if AdaptiveBlockSize(20*1024*1024*1024) != 4*1024*1024 {
		t.Fatalf("expected 4 MiB for 20 GiB file")
	}
	if AdaptiveBlockSize(64*1024*1024*1024) != 8*1024*1024 {
		t.Fatalf("expected 8 MiB for 64 GiB file")
	}
	if AdaptiveBlockSize(200*1024*1024*1024) != 16*1024*1024 {
		t.Fatalf("expected 16 MiB for 200 GiB file")
	}

	p := filepath.Join(t.TempDir(), "data.txt")
	content := bytes.Repeat([]byte("merkle-block-check-"), 25000)
	if err := os.WriteFile(p, content, 0644); err != nil {
		t.Fatal(err)
	}
	blocks, fileHash, sz, usedBS, err := HashFileBlocksAdaptive(p)
	if err != nil {
		t.Fatal(err)
	}
	if sz != int64(len(content)) || usedBS != 256*1024 || len(blocks) < 2 {
		t.Fatalf("unexpected adaptive result: sz=%d bs=%d blocks=%d", sz, usedBS, len(blocks))
	}
	if fileHash != ComputeMerkleFileHash(blocks) {
		t.Fatalf("merkle fileHash mismatch")
	}

	// Reader streaming hash must produce identical blocks and merkle root
	rBlocks, rHash, rSz, err := HashReaderAdaptive(bytes.NewReader(content), usedBS)
	if err != nil || rSz != sz || rHash != fileHash || len(rBlocks) != len(blocks) {
		t.Fatalf("HashReaderAdaptive mismatch: err=%v rHash=%s fileHash=%s", err, rHash, fileHash)
	}
}

func TestZstdBlockCompressionAndIncompressibleSkip(t *testing.T) {
	if IsCompressibleExt("video.mp4") || IsCompressibleExt("archive.ZIP") || IsCompressibleExt("img.jpg") {
		t.Fatal("expected compressed extensions to be skipped")
	}
	if !IsCompressibleExt("notes.txt") || !IsCompressibleExt("main.go") {
		t.Fatal("expected text/source extensions to be compressible")
	}

	raw := bytes.Repeat([]byte("SELECT * FROM logs WHERE id = 12345;\n"), 500)
	comp, ok := CompressBlockIfWorthwhile("dump.sql", raw)
	if !ok || len(comp) >= len(raw) {
		t.Fatalf("expected zstd compression on sql dump: ok=%v len=%d/%d", ok, len(comp), len(raw))
	}
	dec, err := DecompressBlock(comp, nil, len(raw))
	if err != nil || !bytes.Equal(dec, raw) {
		t.Fatalf("zstd roundtrip failed: err=%v len=%d", err, len(dec))
	}

	// Video extension must skip compression immediately
	compSkip, okSkip := CompressBlockIfWorthwhile("clip.mkv", raw)
	if okSkip || len(compSkip) != len(raw) {
		t.Fatal("expected .mkv to bypass compression")
	}
}

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

func TestFileHandleCacheConcurrentSafetyWithUncachedOverflow(t *testing.T) {
	// maxOpen=2 with 16 concurrent goroutines across 8 paths forces uncached overflow + ClosePath waiting
	cache := NewFileHandleCache(2, 50*time.Millisecond)
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

func BenchmarkZstdCompress512K(b *testing.B) {
	raw := bytes.Repeat([]byte("2026-11-19 INFO syncer reconciled folder delta batch 42\n"), 9500)
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CompressBlockIfWorthwhile("app.log", raw)
	}
}
