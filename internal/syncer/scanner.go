package syncer

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/WilliamBossard/Gens-Relay/internal/chunker"
	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
)

const FolderMarkerName = ".gensfolder"

type scanCandidate struct {
	fullPath string
	relPath  string
	size     int64
	mtimeNs  int64
	modeBits uint32
}

type ScanResult struct {
	Changed         []protocol.FileMetadata
	IgnoredRelPaths []string
	FileCount       int
	TotalSize       int64
}

// ScanFolder walks rootDir, hashes new/modified files concurrently (16 workers),
// detects deletions compared to prevIndex, and returns changed metadata entries.
func ScanFolder(rootDir, myDeviceID, ignorePatterns string, prevIndex map[string]protocol.FileMetadata) (*ScanResult, error) {
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, err
	}

	// Protect against unmounted external/NAS volumes appearing as empty directories:
	// if marker is missing while we previously indexed live files and directory is empty, abort scan.
	markerPath := filepath.Join(rootDir, FolderMarkerName)
	if _, mErr := os.Stat(markerPath); os.IsNotExist(mErr) {
		hasLivePrev := false
		for _, prev := range prevIndex {
			if !prev.Deleted {
				hasLivePrev = true
				break
			}
		}
		if hasLivePrev {
			entries, rErr := os.ReadDir(rootDir)
			if rErr == nil && len(entries) == 0 {
				return nil, fmt.Errorf("folder marker %s missing in empty directory (possible unmounted drive)", FolderMarkerName)
			}
		}
		_ = os.WriteFile(markerPath, []byte("gens-relay-folder-marker\n"), 0644)
	}

	matcher := LoadIgnoreMatcher(rootDir, ignorePatterns)
	seen := make(map[string]struct{}, len(prevIndex))
	var candidates []scanCandidate
	var changed []protocol.FileMetadata
	var ignoredRelPaths []string
	var fileCount int
	var totalSize int64

	err := filepath.WalkDir(rootDir, func(currentPath string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if currentPath == rootDir {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}

		rel, err := filepath.Rel(rootDir, currentPath)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)

		if matcher.Match(rel, d.IsDir()) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return nil
		}

		seen[rel] = struct{}{}
		mtimeNs := info.ModTime().UnixNano()
		modeBits := uint32(info.Mode().Perm())

		if d.IsDir() {
			if prev, ok := prevIndex[rel]; !ok || prev.Deleted || !prev.IsDir {
				vc := BumpClock(prev.Version, myDeviceID)
				changed = append(changed, protocol.FileMetadata{
					RelPath:  rel,
					MtimeNs:  mtimeNs,
					ModeBits: modeBits,
					IsDir:    true,
					Deleted:  false,
					Version:  vc,
				})
			}
			return nil
		}

		size := info.Size()
		fileCount++
		totalSize += size

		if prev, ok := prevIndex[rel]; ok && !prev.Deleted && !prev.IsDir {
			if prev.Size == size {
				if prev.MtimeNs == mtimeNs {
					return nil
				}
				// Tolerate 1-2s filesystem timestamp rounding (FAT32/exFAT/SMB) when both mtimes align on whole seconds
				if (prev.MtimeNs%1e9 == 0 || mtimeNs%1e9 == 0) && abs64(prev.MtimeNs-mtimeNs) <= 2*1e9 {
					return nil
				}
			}
		}

		candidates = append(candidates, scanCandidate{
			fullPath: currentPath,
			relPath:  rel,
			size:     size,
			mtimeNs:  mtimeNs,
			modeBits: modeBits,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Parallel BLAKE3 block hashing (16 workers) with stability re-check after hashing
	if len(candidates) > 0 {
		workers := 16
		if len(candidates) < workers {
			workers = len(candidates)
		}
		ch := make(chan scanCandidate, len(candidates))
		for _, c := range candidates {
			ch <- c
		}
		close(ch)

		var mu sync.Mutex
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for c := range ch {
					blocks, fileHash, actualSize, hashErr := chunker.HashFileBlocks(c.fullPath, chunker.DefaultBlockSize)
					if hashErr != nil {
						continue
					}
					// Skip files actively being written to right now; next watcher/scan event will pick them up once settled.
					if postInfo, sErr := os.Stat(c.fullPath); sErr != nil ||
						postInfo.Size() != actualSize ||
						postInfo.ModTime().UnixNano() != c.mtimeNs {
						continue
					}

					prev := prevIndex[c.relPath]
					// If content hash didn't change despite mtime touch, preserve version and store freshly hashed blocks
					if !prev.Deleted && prev.FileHash == fileHash && prev.Size == actualSize && len(prev.Version) > 0 {
						entry := prev
						entry.MtimeNs = c.mtimeNs
						entry.ModeBits = c.modeBits
						entry.Blocks = blocks
						mu.Lock()
						changed = append(changed, entry)
						mu.Unlock()
						continue
					}
					vc := BumpClock(prev.Version, myDeviceID)
					entry := protocol.FileMetadata{
						RelPath:  c.relPath,
						Size:     actualSize,
						MtimeNs:  c.mtimeNs,
						ModeBits: c.modeBits,
						IsDir:    false,
						Deleted:  false,
						Version:  vc,
						Blocks:   blocks,
						FileHash: fileHash,
					}
					mu.Lock()
					changed = append(changed, entry)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
	}

	// Detect deletions and purge newly-ignored files from local index
	for rel, prev := range prevIndex {
		if _, ok := seen[rel]; !ok {
			if matcher.Match(rel, prev.IsDir) {
				ignoredRelPaths = append(ignoredRelPaths, rel)
				continue
			}
			if prev.Deleted {
				continue
			}
			vc := BumpClock(prev.Version, myDeviceID)
			changed = append(changed, protocol.FileMetadata{
				RelPath:  rel,
				IsDir:    prev.IsDir,
				Deleted:  true,
				Version:  vc,
				MtimeNs:  prev.MtimeNs,
				ModeBits: prev.ModeBits,
			})
		}
	}

	return &ScanResult{
		Changed:         changed,
		IgnoredRelPaths: ignoredRelPaths,
		FileCount:       fileCount,
		TotalSize:       totalSize,
	}, nil
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
