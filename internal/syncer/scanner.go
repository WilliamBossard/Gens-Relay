package syncer

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	CaseCollisions  []string
}

func checkFolderMountMarker(rootDir string, prevIndex map[string]protocol.FileMetadata) error {
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return err
	}
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
				return fmt.Errorf("folder marker %s missing in empty directory (possible unmounted drive)", FolderMarkerName)
			}
		}
		_ = os.WriteFile(markerPath, []byte("gens-relay-folder-marker\n"), 0644)
	}
	return nil
}

// ScanFolder walks rootDir, hashes new/modified files concurrently (16 workers) with adaptive block sizes
// and Merkle FileHash, detects deletions compared to prevIndex, and identifies case collisions.
func ScanFolder(rootDir, myDeviceID, ignorePatterns string, prevIndex map[string]protocol.FileMetadata) (*ScanResult, error) {
	if err := checkFolderMountMarker(rootDir, prevIndex); err != nil {
		return nil, err
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

		if isUnchangedFast(prevIndex[rel], size, mtimeNs) {
			return nil
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

	changed = append(changed, hashCandidates(candidates, myDeviceID, prevIndex)...)

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

	// Build projected live map to detect case collisions accurately
	liveMap := make(map[string]protocol.FileMetadata, len(prevIndex)+len(changed))
	for k, v := range prevIndex {
		liveMap[k] = v
	}
	for _, k := range ignoredRelPaths {
		delete(liveMap, k)
	}
	for _, ch := range changed {
		liveMap[ch.RelPath] = ch
	}

	return &ScanResult{
		Changed:         changed,
		IgnoredRelPaths: ignoredRelPaths,
		FileCount:       fileCount,
		TotalSize:       totalSize,
		CaseCollisions:  DetectCaseCollisions(liveMap),
	}, nil
}

// ScanPaths performs an incremental scan restricted to dirtyRelPaths (and any newly created subdirectories under them),
// avoiding full-tree filepath.WalkDir on large folders after fsnotify events.
func ScanPaths(rootDir, myDeviceID, ignorePatterns string, dirtyRelPaths []string, prevIndex map[string]protocol.FileMetadata) (*ScanResult, error) {
	if err := checkFolderMountMarker(rootDir, prevIndex); err != nil {
		return nil, err
	}
	matcher := LoadIgnoreMatcher(rootDir, ignorePatterns)

	dirtySet := make(map[string]struct{}, len(dirtyRelPaths))
	var candidates []scanCandidate
	var changed []protocol.FileMetadata
	var ignoredRelPaths []string
	seenOnDisk := make(map[string]struct{})
	checkedDirs := make(map[string]struct{})

	for _, raw := range dirtyRelPaths {
		rel := filepath.ToSlash(strings.TrimPrefix(filepath.Clean(raw), "./"))
		if rel == "" || rel == "." || strings.HasPrefix(rel, "..") {
			continue
		}
		dirtySet[rel] = struct{}{}
	}

	for rel := range dirtySet {
		absPath := filepath.Join(rootDir, filepath.FromSlash(rel))
		info, err := os.Lstat(absPath)
		if err != nil {
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		if matcher.Match(rel, info.IsDir()) {
			if _, ok := prevIndex[rel]; ok {
				ignoredRelPaths = append(ignoredRelPaths, rel)
			}
			continue
		}
		if !info.IsDir() {
			seenOnDisk[rel] = struct{}{}
			mtimeNs := info.ModTime().UnixNano()
			modeBits := uint32(info.Mode().Perm())
			size := info.Size()
			if !isUnchangedFast(prevIndex[rel], size, mtimeNs) {
				candidates = append(candidates, scanCandidate{
					fullPath: absPath,
					relPath:  rel,
					size:     size,
					mtimeNs:  mtimeNs,
					modeBits: modeBits,
				})
			}
			continue
		}

		// Directory: walk subtree to capture newly added/moved files inside
		checkedDirs[rel] = struct{}{}
		_ = filepath.WalkDir(absPath, func(cur string, d os.DirEntry, wErr error) error {
			if wErr != nil || d.Type()&os.ModeSymlink != 0 {
				return nil
			}
			subRel, rErr := filepath.Rel(rootDir, cur)
			if rErr != nil {
				return nil
			}
			subRel = filepath.ToSlash(subRel)
			if matcher.Match(subRel, d.IsDir()) {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			fi, iErr := d.Info()
			if iErr != nil {
				return nil
			}
			seenOnDisk[subRel] = struct{}{}
			mtimeNs := fi.ModTime().UnixNano()
			modeBits := uint32(fi.Mode().Perm())
			if d.IsDir() {
				if prev, ok := prevIndex[subRel]; !ok || prev.Deleted || !prev.IsDir {
					changed = append(changed, protocol.FileMetadata{
						RelPath:  subRel,
						MtimeNs:  mtimeNs,
						ModeBits: modeBits,
						IsDir:    true,
						Version:  BumpClock(prev.Version, myDeviceID),
					})
				}
				return nil
			}
			sz := fi.Size()
			if !isUnchangedFast(prevIndex[subRel], sz, mtimeNs) {
				candidates = append(candidates, scanCandidate{
					fullPath: cur,
					relPath:  subRel,
					size:     sz,
					mtimeNs:  mtimeNs,
					modeBits: modeBits,
				})
			}
			return nil
		})
	}

	changed = append(changed, hashCandidates(candidates, myDeviceID, prevIndex)...)

	// Detect deletions for dirty paths (and subtrees under deleted/walked dirs)
	for rel, prev := range prevIndex {
		if prev.Deleted {
			continue
		}
		if _, seen := seenOnDisk[rel]; seen {
			continue
		}
		inScope := false
		if _, direct := dirtySet[rel]; direct {
			inScope = true
		} else {
			for d := range dirtySet {
				if strings.HasPrefix(rel, d+"/") {
					inScope = true
					break
				}
			}
		}
		if !inScope {
			continue
		}
		// Double-check if still absent on disk
		absPath := filepath.Join(rootDir, filepath.FromSlash(rel))
		if _, err := os.Lstat(absPath); err == nil && !matcher.Match(rel, prev.IsDir) {
			continue
		}
		if matcher.Match(rel, prev.IsDir) {
			ignoredRelPaths = append(ignoredRelPaths, rel)
			continue
		}
		changed = append(changed, protocol.FileMetadata{
			RelPath:  rel,
			IsDir:    prev.IsDir,
			Deleted:  true,
			Version:  BumpClock(prev.Version, myDeviceID),
			MtimeNs:  prev.MtimeNs,
			ModeBits: prev.ModeBits,
		})
	}
	_ = checkedDirs

	// Recompute FileCount, TotalSize, and CaseCollisions from updated index projection
	projected := make(map[string]protocol.FileMetadata, len(prevIndex)+len(changed))
	for k, v := range prevIndex {
		projected[k] = v
	}
	for _, k := range ignoredRelPaths {
		delete(projected, k)
	}
	for _, ch := range changed {
		projected[ch.RelPath] = ch
	}

	var fileCount int
	var totalSize int64
	for _, entry := range projected {
		if !entry.Deleted && !entry.IsDir {
			fileCount++
			totalSize += entry.Size
		}
	}

	return &ScanResult{
		Changed:         changed,
		IgnoredRelPaths: ignoredRelPaths,
		FileCount:       fileCount,
		TotalSize:       totalSize,
		CaseCollisions:  DetectCaseCollisions(projected),
	}, nil
}

func isUnchangedFast(prev protocol.FileMetadata, size, mtimeNs int64) bool {
	if prev.RelPath == "" || prev.Deleted || prev.IsDir {
		return false
	}
	if prev.Size != size {
		return false
	}
	if prev.MtimeNs == mtimeNs {
		return true
	}
	if (prev.MtimeNs%1e9 == 0 || mtimeNs%1e9 == 0) && abs64(prev.MtimeNs-mtimeNs) <= 2*1e9 {
		return true
	}
	return false
}

func hashCandidates(candidates []scanCandidate, myDeviceID string, prevIndex map[string]protocol.FileMetadata) []protocol.FileMetadata {
	if len(candidates) == 0 {
		return nil
	}
	workers := 16
	if len(candidates) < workers {
		workers = len(candidates)
	}
	ch := make(chan scanCandidate, len(candidates))
	for _, c := range candidates {
		ch <- c
	}
	close(ch)

	var changed []protocol.FileMetadata
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range ch {
				blocks, fileHash, actualSize, usedBlockSize, hashErr := chunker.HashFileBlocksAdaptive(c.fullPath)
				if hashErr != nil {
					continue
				}
				if postInfo, sErr := os.Stat(c.fullPath); sErr != nil ||
					postInfo.Size() != actualSize ||
					postInfo.ModTime().UnixNano() != c.mtimeNs {
					continue
				}

				prev := prevIndex[c.relPath]
				if !prev.Deleted && prev.FileHash == fileHash && prev.Size == actualSize && len(prev.Version) > 0 {
					entry := prev
					entry.MtimeNs = c.mtimeNs
					entry.ModeBits = c.modeBits
					entry.BlockSize = usedBlockSize
					entry.Blocks = blocks
					mu.Lock()
					changed = append(changed, entry)
					mu.Unlock()
					continue
				}
				vc := BumpClock(prev.Version, myDeviceID)
				entry := protocol.FileMetadata{
					RelPath:   c.relPath,
					Size:      actualSize,
					MtimeNs:   c.mtimeNs,
					ModeBits:  c.modeBits,
					IsDir:     false,
					Deleted:   false,
					Version:   vc,
					BlockSize: usedBlockSize,
					Blocks:    blocks,
					FileHash:  fileHash,
				}
				mu.Lock()
				changed = append(changed, entry)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return changed
}

// DetectCaseCollisions returns live paths that collide under case-insensitive comparison
// (e.g., "Notes.txt" and "notes.txt" created on Linux and synced to Windows).
func DetectCaseCollisions(index map[string]protocol.FileMetadata) []string {
	byLower := make(map[string][]string, len(index))
	for rel, entry := range index {
		if entry.Deleted {
			continue
		}
		low := strings.ToLower(rel)
		byLower[low] = append(byLower[low], rel)
	}
	var collisions []string
	for _, list := range byLower {
		if len(list) > 1 {
			collisions = append(collisions, list...)
		}
	}
	sort.Strings(collisions)
	return collisions
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
