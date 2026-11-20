package syncer

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ArchiveFile moves an existing file (or directory tree) before overwrite or deletion according to the folder's versioning policy.
func ArchiveFile(folderRoot, relPath, policy string, keepVersions int) error {
	src := filepath.Join(folderRoot, filepath.FromSlash(relPath))
	return ArchiveSourceFile(folderRoot, relPath, src, policy, keepVersions)
}

// ArchiveSourceFile archives srcPath under relPath's versioning location in folderRoot.
func ArchiveSourceFile(folderRoot, relPath, src, policy string, keepVersions int) error {
	info, err := os.Stat(src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.IsDir() {
		if policy == "" || policy == "none" {
			return os.RemoveAll(src)
		}
		// Archive every file inside the directory before removing the directory tree.
		_ = filepath.WalkDir(src, func(p string, d os.DirEntry, walkErr error) error {
			if walkErr != nil || d.IsDir() {
				return nil
			}
			if subRel, rErr := filepath.Rel(folderRoot, p); rErr == nil {
				_ = ArchiveSourceFile(folderRoot, filepath.ToSlash(subRel), p, policy, keepVersions)
			}
			return nil
		})
		return os.RemoveAll(src)
	}

	switch policy {
	case "", "none":
		return os.Remove(src)

	case "trashcan":
		dst := filepath.Join(folderRoot, ".gensversions", filepath.FromSlash(relPath))
		if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
			return err
		}
		_ = os.Remove(dst)
		return moveFileFallback(src, dst)

	case "simple", "staggered":
		if keepVersions <= 0 {
			keepVersions = 5
		}
		dir := filepath.Join(folderRoot, ".gensversions", filepath.Dir(filepath.FromSlash(relPath)))
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
		base := filepath.Base(relPath)
		ext := filepath.Ext(base)
		stem := strings.TrimSuffix(base, ext)
		stamp := time.Now().UTC().Format("20060102-150405-000")
		dst := filepath.Join(dir, fmt.Sprintf("%s~%s%s", stem, stamp, ext))
		for n := 1; ; n++ {
			if _, sErr := os.Stat(dst); os.IsNotExist(sErr) {
				break
			}
			dst = filepath.Join(dir, fmt.Sprintf("%s~%s-%d%s", stem, stamp, n, ext))
		}
		if err := moveFileFallback(src, dst); err != nil {
			return err
		}
		if policy == "simple" {
			pruneSimpleVersions(dir, stem, ext, keepVersions)
		} else {
			pruneStaggeredVersions(dir, stem, ext)
		}
		return nil

	default:
		return os.Remove(src)
	}
}

// PruneTrashcan enforces age (maxDays) and total size (maxMB) quotas on .gensversions/.
func PruneTrashcan(folderRoot string, maxDays int, maxMB int) {
	if maxDays <= 0 && maxMB <= 0 {
		return
	}
	vRoot := filepath.Join(folderRoot, ".gensversions")
	if _, err := os.Stat(vRoot); os.IsNotExist(err) {
		return
	}

	type archivedFile struct {
		path    string
		size    int64
		modTime time.Time
	}
	var kept []archivedFile
	var totalBytes int64
	var cutoff time.Time
	if maxDays > 0 {
		cutoff = time.Now().Add(-time.Duration(maxDays) * 24 * time.Hour)
	}

	_ = filepath.WalkDir(vRoot, func(p string, d os.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		if maxDays > 0 && info.ModTime().Before(cutoff) {
			_ = os.Remove(p)
			return nil
		}
		sz := info.Size()
		totalBytes += sz
		kept = append(kept, archivedFile{path: p, size: sz, modTime: info.ModTime()})
		return nil
	})

	if maxMB > 0 {
		limitBytes := int64(maxMB) * 1024 * 1024
		if totalBytes > limitBytes {
			sort.Slice(kept, func(i, j int) bool {
				return kept[i].modTime.Before(kept[j].modTime)
			})
			for _, f := range kept {
				if totalBytes <= limitBytes {
					break
				}
				if err := os.Remove(f.path); err == nil {
					totalBytes -= f.size
				}
			}
		}
	}

	// Prune empty subdirectories inside .gensversions
	var dirs []string
	_ = filepath.WalkDir(vRoot, func(p string, d os.DirEntry, walkErr error) error {
		if walkErr == nil && d.IsDir() && p != vRoot {
			dirs = append(dirs, p)
		}
		return nil
	})
	for i := len(dirs) - 1; i >= 0; i-- {
		_ = os.Remove(dirs[i]) // succeeds only if empty
	}
}

func collectVersionMatches(dir, stem, ext string) []os.DirEntry {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	prefix := stem + "~"
	var matches []os.DirEntry
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), prefix) && strings.HasSuffix(e.Name(), ext) {
			matches = append(matches, e)
		}
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Name() < matches[j].Name()
	})
	return matches
}

func pruneSimpleVersions(dir, stem, ext string, keep int) {
	matches := collectVersionMatches(dir, stem, ext)
	if len(matches) <= keep {
		return
	}
	for i := 0; i < len(matches)-keep; i++ {
		_ = os.Remove(filepath.Join(dir, matches[i].Name()))
	}
}

func pruneStaggeredVersions(dir, stem, ext string) {
	matches := collectVersionMatches(dir, stem, ext)
	if len(matches) <= 1 {
		return
	}
	now := time.Now().UTC()
	keptBuckets := make(map[string]bool)
	for i := len(matches) - 1; i >= 0; i-- {
		info, err := matches[i].Info()
		if err != nil {
			continue
		}
		age := now.Sub(info.ModTime().UTC())
		if age > 90*24*time.Hour {
			_ = os.Remove(filepath.Join(dir, matches[i].Name()))
			continue
		}
		var bucket string
		switch {
		case age <= time.Hour:
			bucket = fmt.Sprintf("m-%d", i)
		case age <= 24*time.Hour:
			bucket = "h-" + info.ModTime().UTC().Format("2006010215")
		case age <= 30*24*time.Hour:
			bucket = "d-" + info.ModTime().UTC().Format("20060102")
		default:
			year, week := info.ModTime().UTC().ISOWeek()
			bucket = fmt.Sprintf("w-%d-%02d", year, week)
		}
		if keptBuckets[bucket] {
			_ = os.Remove(filepath.Join(dir, matches[i].Name()))
		} else {
			keptBuckets[bucket] = true
		}
	}
}

func moveFileFallback(src, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		_ = in.Close()
		return err
	}
	_, copyErr := io.Copy(out, in)
	syncErr := out.Sync()
	closeOutErr := out.Close()
	_ = in.Close()
	if copyErr != nil || syncErr != nil || closeOutErr != nil {
		_ = os.Remove(dst)
		if copyErr != nil {
			return copyErr
		}
		if syncErr != nil {
			return syncErr
		}
		return closeOutErr
	}
	return os.Remove(src)
}
