package syncer

import (
	"bytes"
	"context"
	"encoding/json"
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/WilliamBossard/Gens-Relay/internal/chunker"
	"github.com/WilliamBossard/Gens-Relay/internal/config"
	"github.com/WilliamBossard/Gens-Relay/internal/p2p"
	"github.com/WilliamBossard/Gens-Relay/internal/store"
	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
	"github.com/quic-go/quic-go"
)

func TestIgnoreMatcherAndVectorClock(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, ".gensignore"), []byte("*.tmp\nbuild/\n!keep.tmp\n"), 0644)

	m := LoadIgnoreMatcher(dir, "*.log")
	if !m.Match("foo.tmp", false) {
		t.Fatal("expected *.tmp to be ignored")
	}
	if m.Match("keep.tmp", false) {
		t.Fatal("expected !keep.tmp negation to keep file")
	}
	if !m.Match("build/app.exe", false) {
		t.Fatal("expected build/app.exe to be ignored")
	}
	if !m.Match("server.log", false) {
		t.Fatal("expected *.log from config to be ignored")
	}
	if !m.Match(".gensversions/old.txt", false) {
		t.Fatal("expected internal .gensversions to always be ignored")
	}
	if !IsInternalPath(dir, filepath.Join(dir, ".gensversions", "sub", "old~2026.txt")) {
		t.Fatal("expected nested .gensversions file to be recognized as internal path")
	}
	if !IsInternalName(".gensfolder") {
		t.Fatal("expected .gensfolder to be internal")
	}

	vcA := protocol.VectorClock{"A": 2, "B": 1}
	vcB := protocol.VectorClock{"A": 1, "B": 1}
	if CompareClocks(vcA, vcB) != RelNewer {
		t.Fatal("expected vcA newer than vcB")
	}
	vcC := protocol.VectorClock{"A": 1, "B": 2}
	if CompareClocks(vcA, vcC) != RelConflict {
		t.Fatal("expected conflict between vcA and vcC")
	}
}

func TestVersionerTrashcanSimpleAndPrune(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "docs")
	_ = os.MkdirAll(sub, 0755)
	_ = os.WriteFile(filepath.Join(sub, "note.txt"), []byte("v1"), 0644)

	if err := ArchiveFile(dir, "docs/note.txt", "trashcan", 5); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(sub, "note.txt")); !os.IsNotExist(err) {
		t.Fatal("expected original file moved")
	}
	data, err := os.ReadFile(filepath.Join(dir, ".gensversions", "docs", "note.txt"))
	if err != nil || string(data) != "v1" {
		t.Fatalf("expected file in .gensversions, got %q err=%v", string(data), err)
	}

	// Age-based pruning removes old trashcan files
	oldFile := filepath.Join(dir, ".gensversions", "docs", "note.txt")
	past := time.Now().Add(-40 * 24 * time.Hour)
	_ = os.Chtimes(oldFile, past, past)
	PruneTrashcan(dir, 30, 0)
	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Fatalf("expected old trashcan file pruned")
	}

	// Directory archiving in trashcan mode must preserve nested files in .gensversions
	tree := filepath.Join(dir, "folder_del", "inner")
	_ = os.MkdirAll(tree, 0755)
	_ = os.WriteFile(filepath.Join(tree, "keepme.txt"), []byte("saved"), 0644)
	if err := ArchiveFile(dir, "folder_del", "trashcan", 5); err != nil {
		t.Fatal(err)
	}
	archived, err := os.ReadFile(filepath.Join(dir, ".gensversions", "folder_del", "inner", "keepme.txt"))
	if err != nil || string(archived) != "saved" {
		t.Fatalf("expected nested file archived on dir delete, got %q err=%v", string(archived), err)
	}
}

func TestScanFolderMountGuardIgnoredPurgeAndIncrementalScanPaths(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("hello"), 0644)
	_ = os.WriteFile(filepath.Join(dir, "temp.tmp"), []byte("junk"), 0644)

	res, err := ScanFolder(dir, "NODE-A", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, FolderMarkerName)); err != nil {
		t.Fatalf("expected %s marker created: %v", FolderMarkerName, err)
	}

	prev := make(map[string]protocol.FileMetadata)
	for _, f := range res.Changed {
		prev[f.RelPath] = f
	}

	// Incremental ScanPaths detects new/modified/deleted files without full walk
	_ = os.MkdirAll(filepath.Join(dir, "sub"), 0755)
	_ = os.WriteFile(filepath.Join(dir, "sub", "inc.txt"), []byte("incremental-ok"), 0644)
	incRes, err := ScanPaths(dir, "NODE-A", "", []string{"sub/inc.txt"}, prev)
	if err != nil {
		t.Fatal(err)
	}
	if len(incRes.Changed) != 1 || incRes.Changed[0].RelPath != "sub/inc.txt" || incRes.FileCount != 3 {
		t.Fatalf("unexpected ScanPaths result: %+v", incRes)
	}
	prev["sub/inc.txt"] = incRes.Changed[0]

	// Case collision detection helper
	collisions := DetectCaseCollisions(map[string]protocol.FileMetadata{
		"Docs/Note.txt": {RelPath: "Docs/Note.txt"},
		"docs/note.txt": {RelPath: "docs/note.txt"},
	})
	if len(collisions) != 2 {
		t.Fatalf("expected 2 case-colliding entries, got %+v", collisions)
	}

	// Free disk check
	avail, err := AvailableDiskBytes(dir)
	if err != nil || avail == 0 {
		t.Fatalf("AvailableDiskBytes failed: avail=%d err=%v", avail, err)
	}

	// Simulate adding *.tmp to .gensignore -> temp.tmp must be reported in IgnoredRelPaths, not Deleted
	res2, err := ScanFolder(dir, "NODE-A", "*.tmp", prev)
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.IgnoredRelPaths) != 1 || res2.IgnoredRelPaths[0] != "temp.tmp" {
		t.Fatalf("expected temp.tmp in IgnoredRelPaths, got %+v", res2.IgnoredRelPaths)
	}
	for _, ch := range res2.Changed {
		if ch.RelPath == "temp.tmp" && ch.Deleted {
			t.Fatal("newly ignored file must not be marked Deleted")
		}
	}

	// Simulate unmounted drive: remove all entries including .gensfolder while prev has live files
	_ = os.RemoveAll(filepath.Join(dir, "sub"))
	_ = os.Remove(filepath.Join(dir, "keep.txt"))
	_ = os.Remove(filepath.Join(dir, "temp.tmp"))
	_ = os.Remove(filepath.Join(dir, FolderMarkerName))
	if _, err := ScanFolder(dir, "NODE-A", "", prev); err == nil {
		t.Fatal("expected ScanFolder to abort when .gensfolder marker is missing in empty directory")
	}
}

func TestFolderInvitationsAndRevertReceiveOnlyExtras(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	cfg, _ := config.LoadOrCreateDaemonConfig(dir)
	_ = cfg.Update(func(c *config.DaemonConfig) {
		c.QUICPort = 0
		c.EnableLANDisc = false
		c.EnableUPnP = false
	})
	id, _ := protocol.LoadOrCreateIdentity(dir)
	st, _ := store.Open(dir)
	defer st.Close()
	pMgr := p2p.NewManager(id, cfg, st, p2p.Handlers{})
	sMgr := NewManager(id.DeviceID, st, pMgr, nil)

	_ = st.UpsertDevice(store.DeviceRecord{DeviceID: "PEER-1", Name: "SenderPeer"})

	// Incoming shared folder invitation
	invRaw, _ := json.Marshal(protocol.FolderInviteMsg{
		FolderID:      "photos",
		Label:         "Photos Famille",
		SuggestedMode: "receive_only",
		FileCount:     12,
		TotalBytes:    4096,
	})
	sMgr.HandleFolderInvite("PEER-1", invRaw)
	invs, err := st.ListFolderInvitations()
	if err != nil || len(invs) != 1 || invs[0].Label != "Photos Famille" {
		t.Fatalf("expected folder invitation saved, got %+v err=%v", invs, err)
	}

	targetDir := filepath.Join(t.TempDir(), "photos-local")
	if err := sMgr.AcceptInvitation("photos", "PEER-1", targetDir, protocol.ModeReceiveOnly); err != nil {
		t.Fatal(err)
	}
	invsAfter, _ := st.ListFolderInvitations()
	if len(invsAfter) != 0 {
		t.Fatalf("expected invitation removed after accept")
	}

	// Extra file created locally in receive_only must be archived & removed on RevertLocalChanges
	extraFile := filepath.Join(targetDir, "rogue.txt")
	_ = os.WriteFile(extraFile, []byte("rogue"), 0644)
	_ = st.UpsertLocalFiles("photos", []protocol.FileMetadata{
		{RelPath: "rogue.txt", Size: 5, Version: protocol.VectorClock{id.DeviceID: 1}},
	})
	if err := sMgr.RevertLocalChanges(ctx, "photos"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(extraFile); !os.IsNotExist(err) {
		t.Fatalf("expected extra receive_only file archived on RevertLocalChanges")
	}
	archived := filepath.Join(targetDir, ".gensversions", "rogue.txt")
	if _, err := os.Stat(archived); err != nil {
		t.Fatalf("expected reverted extra file preserved in .gensversions: %v", err)
	}
}

func TestEndToEndP2PSyncBidirectionalAndUnidirectional(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	nodeA := newTestNode(t, ctx, "WinNode")
	nodeB := newTestNode(t, ctx, "NASNode")

	if err := nodeA.store.UpsertDevice(store.DeviceRecord{DeviceID: nodeB.id.DeviceID, Name: "NAS"}); err != nil {
		t.Fatal(err)
	}
	if err := nodeB.store.UpsertDevice(store.DeviceRecord{DeviceID: nodeA.id.DeviceID, Name: "Win"}); err != nil {
		t.Fatal(err)
	}

	dirA := filepath.Join(t.TempDir(), "win-sync")
	dirB := filepath.Join(t.TempDir(), "nas-sync")
	_ = os.MkdirAll(dirA, 0755)
	_ = os.MkdirAll(dirB, 0755)

	if err := nodeA.store.UpsertFolder(store.FolderRecord{
		FolderID:  "shared-docs",
		Label:     "Shared Docs",
		LocalPath: dirA,
		Mode:      protocol.ModeSendOnly,
		Peers:     []string{nodeB.id.DeviceID},
	}); err != nil {
		t.Fatal(err)
	}
	if err := nodeB.store.UpsertFolder(store.FolderRecord{
		FolderID:   "shared-docs",
		Label:      "Shared Docs",
		LocalPath:  dirB,
		Mode:       protocol.ModeReceiveOnly,
		Versioning: "trashcan",
		Peers:      []string{nodeA.id.DeviceID},
	}); err != nil {
		t.Fatal(err)
	}

	bigData := bytes.Repeat([]byte("GENS-SYNC-BLOCK-"), 80000)
	_ = os.MkdirAll(filepath.Join(dirA, "sub"), 0755)
	_ = os.WriteFile(filepath.Join(dirA, "sub", "archive.bin"), bigData, 0644)

	nodeA.sync.Start(ctx)
	nodeB.sync.Start(ctx)

	nodeA.p2p.SetPeerEndpoint(nodeB.id.DeviceID, "NAS", fmt.Sprintf("127.0.0.1:%d", nodeB.p2p.QUICPort()))

	targetFile := filepath.Join(dirB, "sub", "archive.bin")
	waitUntil(t, 12*time.Second, func() bool {
		got, err := os.ReadFile(targetFile)
		return err == nil && bytes.Equal(got, bigData)
	}, "expected bigData synced from A to B")

	// Delete file on A and verify B moves it into .gensversions (trashcan)
	_ = os.Remove(filepath.Join(dirA, "sub", "archive.bin"))
	nodeA.sync.TriggerScan("shared-docs")

	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		_, errOrig := os.Stat(targetFile)
		_, errTrash := os.Stat(filepath.Join(dirB, ".gensversions", "sub", "archive.bin"))
		if os.IsNotExist(errOrig) && errTrash == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	_, errOrig := os.Stat(targetFile)
	_, errTrash := os.Stat(filepath.Join(dirB, ".gensversions", "sub", "archive.bin"))
	aLoc, _ := nodeA.store.GetLocalIndex("shared-docs")
	bRem, _ := nodeB.store.GetRemoteIndex("shared-docs", nodeA.id.DeviceID)
	bLoc, _ := nodeB.store.GetLocalIndex("shared-docs")
	t.Fatalf("timeout waiting for trashcan deletion: errOrig=%v errTrash=%v aLoc=%+v bRem=%+v bLoc=%+v",
		errOrig, errTrash, aLoc["sub/archive.bin"], bRem["sub/archive.bin"], bLoc["sub/archive.bin"])
}

func TestEndToEndBidirectionalConflictResolution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	nodeA := newTestNode(t, ctx, "WinNode")
	nodeB := newTestNode(t, ctx, "UbuntuNode")

	_ = nodeA.store.UpsertDevice(store.DeviceRecord{DeviceID: nodeB.id.DeviceID, Name: "Ubuntu"})
	_ = nodeB.store.UpsertDevice(store.DeviceRecord{DeviceID: nodeA.id.DeviceID, Name: "Win"})

	dirA := filepath.Join(t.TempDir(), "a")
	dirB := filepath.Join(t.TempDir(), "b")
	_ = os.MkdirAll(dirA, 0755)
	_ = os.MkdirAll(dirB, 0755)

	_ = nodeA.store.UpsertFolder(store.FolderRecord{
		FolderID:  "bidir",
		Label:     "Bidir",
		LocalPath: dirA,
		Mode:      protocol.ModeSendReceive,
		Peers:     []string{nodeB.id.DeviceID},
	})
	_ = nodeB.store.UpsertFolder(store.FolderRecord{
		FolderID:  "bidir",
		Label:     "Bidir",
		LocalPath: dirB,
		Mode:      protocol.ModeSendReceive,
		Peers:     []string{nodeA.id.DeviceID},
	})

	_ = os.WriteFile(filepath.Join(dirA, "notes.txt"), []byte("version-from-A"), 0644)
	oldTime := time.Now().Add(-1 * time.Hour)
	_ = os.Chtimes(filepath.Join(dirA, "notes.txt"), oldTime, oldTime)

	_ = os.WriteFile(filepath.Join(dirB, "notes.txt"), []byte("version-from-B-newer"), 0644)

	nodeA.sync.Start(ctx)
	nodeB.sync.Start(ctx)

	waitUntil(t, 5*time.Second, func() bool {
		idxA, _ := nodeA.store.GetLocalIndex("bidir")
		idxB, _ := nodeB.store.GetLocalIndex("bidir")
		return len(idxA) == 1 && len(idxB) == 1
	}, "initial scans on both nodes")

	nodeA.p2p.SetPeerEndpoint(nodeB.id.DeviceID, "Ubuntu", fmt.Sprintf("127.0.0.1:%d", nodeB.p2p.QUICPort()))

	waitUntil(t, 12*time.Second, func() bool {
		aData, errA := os.ReadFile(filepath.Join(dirA, "notes.txt"))
		bData, errB := os.ReadFile(filepath.Join(dirB, "notes.txt"))
		if errA != nil || errB != nil {
			return false
		}
		if string(aData) != "version-from-B-newer" || string(bData) != "version-from-B-newer" {
			return false
		}
		matches, _ := filepath.Glob(filepath.Join(dirA, "notes.sync-conflict-*.txt"))
		return len(matches) == 1
	}, "expected conflict resolution keeping both versions")
}

type testNode struct {
	id    *protocol.Identity
	store *store.Store
	p2p   *p2p.Manager
	sync  *Manager
}

// TestRenameReuseMovesLocalFile verifies that a file renamed on A (delete old
// + create new, same content) converges on B WITHOUT any data download and by
// MOVING the local file (rename-reuse) instead of copying it, regardless of
// the versioning policy.
func TestRenameReuseMovesLocalFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// Capture the default slog logger to prove the rename-reuse path ran on B.
	oldLogger := slog.Default()
	var logBuf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
	t.Cleanup(func() {
		slog.SetDefault(oldLogger)
		if t.Failed() {
			t.Logf("captured logs:\n%s", logBuf.String())
		}
	})

	nodeA := newTestNode(t, ctx, "RenA")
	nodeB := newTestNode(t, ctx, "RenB")

	dirA := t.TempDir()
	dirB := t.TempDir()
	const folderID = "rename"
	_ = nodeA.store.UpsertDevice(store.DeviceRecord{DeviceID: nodeB.id.DeviceID, Name: "RenB"})
	_ = nodeB.store.UpsertDevice(store.DeviceRecord{DeviceID: nodeA.id.DeviceID, Name: "RenA"})
	_ = nodeA.store.UpsertFolder(store.FolderRecord{
		FolderID:  folderID,
		Label:     "Rename",
		LocalPath: dirA,
		Mode:      protocol.ModeSendReceive,
		Peers:     []string{nodeB.id.DeviceID},
	})
	_ = nodeB.store.UpsertFolder(store.FolderRecord{
		FolderID:  folderID,
		Label:     "Rename",
		LocalPath: dirB,
		Mode:      protocol.ModeSendReceive,
		Peers:     []string{nodeA.id.DeviceID},
	})

	// Deterministic 1 MiB payload.
	content := make([]byte, 1024*1024)
	for i := range content {
		content[i] = byte(i % 251)
	}
	if err := os.WriteFile(filepath.Join(dirA, "old.bin"), content, 0644); err != nil {
		t.Fatal(err)
	}

	nodeA.sync.Start(ctx)
	nodeB.sync.Start(ctx)

	// Establish the direct QUIC link (test harness has no LAN discovery).
	nodeA.p2p.SetPeerEndpoint(nodeB.id.DeviceID, "RenB", fmt.Sprintf("127.0.0.1:%d", nodeB.p2p.QUICPort()))

	waitUntil(t, 15*time.Second, func() bool {
		_, err := os.Stat(filepath.Join(dirB, "old.bin"))
		return err == nil
	}, "initial sync delivers old.bin to B")

	rxBefore, _ := nodeB.sync.TransferCounters()

	// A renames old.bin -> new.bin. B must converge with new.bin (same
	// content), old.bin gone, and zero data transferred for it.
	if err := os.Rename(filepath.Join(dirA, "old.bin"), filepath.Join(dirA, "new.bin")); err != nil {
		t.Fatal(err)
	}

	waitUntil(t, 20*time.Second, func() bool {
		_, nErr := os.Stat(filepath.Join(dirB, "new.bin"))
		_, oErr := os.Stat(filepath.Join(dirB, "old.bin"))
		if nErr != nil || !os.IsNotExist(oErr) {
			return false
		}
		b, rErr := os.ReadFile(filepath.Join(dirB, "new.bin"))
		return rErr == nil && bytes.Equal(b, content)
	}, "rename converges on B (new.bin same content, old.bin gone)")

	rxAfter, _ := nodeB.sync.TransferCounters()
	if rxAfter != rxBefore {
		t.Fatalf("rename must not download data: B received %d extra bytes", rxAfter-rxBefore)
	}
	if !bytes.Contains(logBuf.Bytes(), []byte("rename-reuse from=")) {
		t.Fatal("expected the rename-reuse optimization to run (no successful 'rename-reuse' log line)")
	}
}

// TestPartialTmpResumesWithoutRefetch verifies resumable transfers: when a
// previous pull left a partial .gens.tmp.resume file behind, the next pull
// reuses the blocks already present in it (verified by BLAKE3) and only
// fetches the missing ones — instead of re-downloading the whole file.
func TestPartialTmpResumesWithoutRefetch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	nodeA := newTestNode(t, ctx, "WinNode")
	nodeB := newTestNode(t, ctx, "NASNode")

	if err := nodeA.store.UpsertDevice(store.DeviceRecord{DeviceID: nodeB.id.DeviceID, Name: "NAS"}); err != nil {
		t.Fatal(err)
	}
	if err := nodeB.store.UpsertDevice(store.DeviceRecord{DeviceID: nodeA.id.DeviceID, Name: "Win"}); err != nil {
		t.Fatal(err)
	}

	dirA := filepath.Join(t.TempDir(), "win-sync")
	dirB := filepath.Join(t.TempDir(), "nas-sync")
	_ = os.MkdirAll(dirA, 0755)
	_ = os.MkdirAll(dirB, 0755)

	if err := nodeA.store.UpsertFolder(store.FolderRecord{
		FolderID:  "resume-fold",
		Label:     "Resume",
		LocalPath: dirA,
		Mode:      protocol.ModeSendOnly,
		Peers:     []string{nodeB.id.DeviceID},
	}); err != nil {
		t.Fatal(err)
	}
	if err := nodeB.store.UpsertFolder(store.FolderRecord{
		FolderID:   "resume-fold",
		Label:      "Resume",
		LocalPath:  dirB,
		Mode:       protocol.ModeReceiveOnly,
		Peers:      []string{nodeA.id.DeviceID},
	}); err != nil {
		t.Fatal(err)
	}

	// 10 distinct incompressible 256 KiB blocks (2.5 MiB file → adaptive
	// block size 256 KiB). Random bytes so wire size ≈ raw size (Zstd cannot
	// cheat the transfer assertions).
	const blocks = 10
	const blkSize = 256 * 1024
	content := make([]byte, blocks*blkSize)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	fileA := filepath.Join(dirA, "big.bin")
	if err := os.WriteFile(fileA, content, 0644); err != nil {
		t.Fatal(err)
	}
	srcBlocks, _, _, err := chunker.HashFileBlocks(fileA, 0)
	if err != nil || len(srcBlocks) != blocks {
		t.Fatalf("expected %d blocks, got %d (err=%v)", blocks, len(srcBlocks), err)
	}

	// Simulate a previously interrupted transfer on B: half the blocks are
	// already in the deterministic resume tmp (note the leading dot, which
	// matches the name used by pullFileContent), the rest are zero.
	tmpB := filepath.Join(dirB, ".big.bin.gens.tmp.resume")
	tmpFile, err := os.OpenFile(tmpB, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if err := tmpFile.Truncate(int64(blocks) * blkSize); err != nil {
		tmpFile.Close()
		t.Fatal(err)
	}
	const seeded = 5
	for _, b := range srcBlocks[:seeded] {
		if _, err := tmpFile.WriteAt(content[b.Offset:b.Offset+int64(b.Size)], b.Offset); err != nil {
			tmpFile.Close()
			t.Fatal(err)
		}
	}
	tmpFile.Close()

	rxBefore, _ := nodeB.sync.TransferCounters()

	nodeA.sync.Start(ctx)
	nodeB.sync.Start(ctx)
	nodeA.p2p.SetPeerEndpoint(nodeB.id.DeviceID, "NAS", fmt.Sprintf("127.0.0.1:%d", nodeB.p2p.QUICPort()))

	targetFile := filepath.Join(dirB, "big.bin")
	waitUntil(t, 15*time.Second, func() bool {
		got, err := os.ReadFile(targetFile)
		return err == nil && bytes.Equal(got, content)
	}, "resumed transfer completes with the correct content")

	// The resume tmp must have been consumed (renamed into place).
	if _, err := os.Stat(tmpB); !os.IsNotExist(err) {
		entries, _ := os.ReadDir(dirB)
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("expected the resume tmp to be gone after success, stat err=%v dirB=%v", err, names)
	}

	// Only the unseeded blocks may have been transferred: at least the 5
	// missing blocks (incompressible → wire ≈ raw), and strictly less than
	// the full file (already-present blocks must not be re-downloaded).
	rxAfter, _ := nodeB.sync.TransferCounters()
	delta := int64(rxAfter - rxBefore)
	if delta < int64(blocks-seeded)*blkSize {
		t.Fatalf("expected at least %d fetched bytes (missing blocks), got %d", (blocks-seeded)*blkSize, delta)
	}
	if delta > int64(blocks)*blkSize*105/100 {
		t.Fatalf("resume must not re-download already-present blocks: fetched %d for a %d-byte file", delta, len(content))
	}
}

func newTestNode(t *testing.T, ctx context.Context, name string) *testNode {
	t.Helper()
	dataDir := t.TempDir()
	cfg, err := config.LoadOrCreateDaemonConfig(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	_ = cfg.Update(func(c *config.DaemonConfig) {
		c.NodeName = name
		c.QUICPort = 0
		c.EnableLANDisc = false
		c.EnableUPnP = false
		c.RelayURL = ""
	})
	id, err := protocol.LoadOrCreateIdentity(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	var sMgr *Manager
	pMgr := p2p.NewManager(id, cfg, st, p2p.Handlers{
		OnPeerConnected: func(c context.Context, peerID string) {
			if sMgr != nil {
				sMgr.OnPeerConnected(c, peerID)
			}
		},
		OnControlMessage: func(c context.Context, peerID string, env protocol.ControlEnvelope) {
			if sMgr != nil {
				sMgr.HandleControlMessage(c, peerID, env)
			}
		},
		OnDataStream: func(c context.Context, peerID string, kind protocol.DataRequestKind, stream *quic.Stream) {
			if sMgr != nil {
				sMgr.HandleDataStream(c, peerID, kind, stream)
			}
		},
	})
	sMgr = NewManager(id.DeviceID, st, pMgr, nil)
	if err := pMgr.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer shutCancel()
		sMgr.Shutdown(shutCtx)
		pMgr.Shutdown(shutCtx)
	})
	return &testNode{id: id, store: st, p2p: pMgr, sync: sMgr}
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", msg)
}
