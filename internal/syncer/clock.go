package syncer

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
)

type VersionRelation int

const (
	RelEqual VersionRelation = iota
	RelNewer                 // a strictly dominates b
	RelOlder                 // b strictly dominates a
	RelConflict              // concurrent edits
)

func CloneClock(vc protocol.VectorClock) protocol.VectorClock {
	out := make(protocol.VectorClock, len(vc))
	for k, v := range vc {
		out[k] = v
	}
	return out
}

func MergeClocks(a, b protocol.VectorClock) protocol.VectorClock {
	out := CloneClock(a)
	for k, v := range b {
		if v > out[k] {
			out[k] = v
		}
	}
	return out
}

func BumpClock(vc protocol.VectorClock, deviceID string) protocol.VectorClock {
	out := CloneClock(vc)
	out[deviceID] = out[deviceID] + 1
	return out
}

// CompareClocks compares vector clocks a and b.
func CompareClocks(a, b protocol.VectorClock) VersionRelation {
	aGreater := false
	bGreater := false

	seen := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}

	for k := range seen {
		va := a[k]
		vb := b[k]
		if va > vb {
			aGreater = true
		} else if vb > va {
			bGreater = true
		}
	}

	switch {
	case !aGreater && !bGreater:
		return RelEqual
	case aGreater && !bGreater:
		return RelNewer
	case !aGreater && bGreater:
		return RelOlder
	default:
		return RelConflict
	}
}

func ConflictRelPath(relPath, deviceID string, mtimeNs int64) string {
	dir := path.Dir(relPath)
	base := path.Base(relPath)
	ext := path.Ext(base)
	stem := strings.TrimSuffix(base, ext)

	t := time.Unix(0, mtimeNs).UTC()
	if mtimeNs <= 0 {
		t = time.Now().UTC()
	}
	shortID := strings.ReplaceAll(deviceID, "-", "")
	if len(shortID) > 6 {
		shortID = shortID[:6]
	}
	conflictName := fmt.Sprintf("%s.sync-conflict-%s-%s%s", stem, t.Format("20060102-150405"), shortID, ext)
	if dir == "." || dir == "" {
		return conflictName
	}
	return path.Join(dir, conflictName)
}
