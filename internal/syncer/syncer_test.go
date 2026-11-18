package syncer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestVersionerTrashcanAndSimple(t *testing.T) {
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

func TestScanFolderMountGuardAndIgnoredPurge(t *testing.T) {
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
	_ = os.Remove(filepath.Join(dir, "keep.txt"))
	_ = os.Remove(filepath.Join(dir, "temp.tmp"))
	_ = os.Remove(filepath.Join(dir, FolderMarkerName))
	if _, err := ScanFolder(dir, "NODE-A", "", prev); err == nil {
		t.Fatal("expected ScanFolder to abort when .gensfolder marker is missing in empty directory")
	}
}

func TestEndToEndP2PSyncBidirectionalAndUnidirectional(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	nodeA := newTestNode(t, ctx, "WinNode")
	nodeB := newTestNode(t, ctx, "NASNode")

	// Pair A <-> B
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

	// Configure FolderID "shared-docs": SendOnly on A -> ReceiveOnly on B
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

	// Write a 1.2 MiB file (multi-block) on A
	bigData := bytes.Repeat([]byte("GENS-SYNC-BLOCK-"), 80000)
	_ = os.MkdirAll(filepath.Join(dirA, "sub"), 0755)
	_ = os.WriteFile(filepath.Join(dirA, "sub", "archive.bin"), bigData, 0644)

	nodeA.sync.Start(ctx)
	nodeB.sync.Start(ctx)

	// Connect A -> B over localhost QUIC mTLS
	nodeA.p2p.SetPeerEndpoint(nodeB.id.DeviceID, "NAS", fmt.Sprintf("127.0.0.1:%d", nodeB.p2p.QUICPort()))

	targetFile := filepath.Join(dirB, "sub", "archive.bin")
	waitUntil(t, 12*time.Second, func() bool {
		got, err := os.ReadFile(targetFile)
		return err == nil && bytes.Equal(got, bigData)
	}, "expected bigData synced from A to B")

	// Delete file on A and verify B moves it into .gensversions (trashcan)
	_ = os.Remove(filepath.Join(dirA, "sub", "archive.bin"))
	nodeA.sync.TriggerScan("shared-docs")

	waitUntil(t, 12*time.Second, func() bool {
		_, errOrig := os.Stat(targetFile)
		_, errTrash := os.Stat(filepath.Join(dirB, ".gensversions", "sub", "archive.bin"))
		return os.IsNotExist(errOrig) && errTrash == nil
	}, "expected deletion on A to trashcan file on B")
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

	// Both create the same file while disconnected with slightly different mtimes
	_ = os.WriteFile(filepath.Join(dirA, "notes.txt"), []byte("version-from-A"), 0644)
	oldTime := time.Now().Add(-1 * time.Hour)
	_ = os.Chtimes(filepath.Join(dirA, "notes.txt"), oldTime, oldTime)

	_ = os.WriteFile(filepath.Join(dirB, "notes.txt"), []byte("version-from-B-newer"), 0644)

	nodeA.sync.Start(ctx)
	nodeB.sync.Start(ctx)

	// Wait for initial local scans before connecting
	waitUntil(t, 5*time.Second, func() bool {
		idxA, _ := nodeA.store.GetLocalIndex("bidir")
		idxB, _ := nodeB.store.GetLocalIndex("bidir")
		return len(idxA) == 1 && len(idxB) == 1
	}, "initial scans on both nodes")

	nodeA.p2p.SetPeerEndpoint(nodeB.id.DeviceID, "Ubuntu", fmt.Sprintf("127.0.0.1:%d", nodeB.p2p.QUICPort()))

	// Newer version (B) must win at notes.txt on both nodes, and A's loser must be saved as *.sync-conflict-*
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
