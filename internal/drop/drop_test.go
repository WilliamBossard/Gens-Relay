package drop

import (
	"bytes"
	"context"
	"fmt"
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

func TestDropEndToEndFolderTransfer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	sender := newDropNode(t, ctx, "Sender")
	receiver := newDropNode(t, ctx, "Receiver")

	_ = sender.store.UpsertDevice(store.DeviceRecord{DeviceID: receiver.id.DeviceID, Name: "Receiver"})
	_ = receiver.store.UpsertDevice(store.DeviceRecord{DeviceID: sender.id.DeviceID, Name: "Sender"})

	sender.p2p.SetPeerEndpoint(receiver.id.DeviceID, "Receiver", fmt.Sprintf("127.0.0.1:%d", receiver.p2p.QUICPort()))

	deadline := time.Now().Add(5 * time.Second)
	for !sender.p2p.IsConnected(receiver.id.DeviceID) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !sender.p2p.IsConnected(receiver.id.DeviceID) {
		t.Fatal("peers failed to connect via QUIC mTLS")
	}

	// Prepare folder to send via Drop
	srcRoot := t.TempDir()
	sendFolder := filepath.Join(srcRoot, "VacationPack")
	_ = os.MkdirAll(filepath.Join(sendFolder, "nested"), 0755)
	payload := bytes.Repeat([]byte("DROP-TRANSFER-DATA-"), 60000) // ~1.14 MB (multi-chunk)
	_ = os.WriteFile(filepath.Join(sendFolder, "nested", "clip.dat"), payload, 0644)
	_ = os.WriteFile(filepath.Join(sendFolder, "readme.txt"), []byte("hello drop"), 0644)

	rec, err := sender.drop.SendPaths(ctx, receiver.id.DeviceID, []string{sendFolder})
	if err != nil {
		t.Fatalf("SendPaths failed: %v", err)
	}

	// Wait for receiver to receive pending offer
	var offer *store.DropTransferRecord
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r, err := receiver.store.GetDropTransfer(rec.TransferID); err == nil {
			offer = r
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if offer == nil || offer.Status != "pending_accept" {
		t.Fatalf("expected pending_accept offer on receiver, got %+v", offer)
	}

	customDest := filepath.Join(t.TempDir(), "custom-inbox")
	if err := receiver.drop.RespondOffer(rec.TransferID, true, customDest); err != nil {
		t.Fatalf("RespondOffer failed: %v", err)
	}

	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		r, err := receiver.store.GetDropTransfer(rec.TransferID)
		if err == nil && r.Status == "completed" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	finalRecv, err := receiver.store.GetDropTransfer(rec.TransferID)
	if err != nil || finalRecv.Status != "completed" {
		t.Fatalf("expected completed transfer, got %+v err=%v", finalRecv, err)
	}

	gotClip, err := os.ReadFile(filepath.Join(customDest, "VacationPack", "nested", "clip.dat"))
	if err != nil || !bytes.Equal(gotClip, payload) {
		t.Fatalf("received clip mismatch: err=%v len=%d", err, len(gotClip))
	}
	gotTxt, err := os.ReadFile(filepath.Join(customDest, "VacationPack", "readme.txt"))
	if err != nil || string(gotTxt) != "hello drop" {
		t.Fatalf("received readme mismatch: %q err=%v", string(gotTxt), err)
	}
}

func TestDropPauseAndResume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	sender := newDropNode(t, ctx, "Sender")
	receiver := newDropNode(t, ctx, "Receiver")

	_ = sender.store.UpsertDevice(store.DeviceRecord{DeviceID: receiver.id.DeviceID, Name: "Receiver"})
	_ = receiver.store.UpsertDevice(store.DeviceRecord{
		DeviceID:       sender.id.DeviceID,
		Name:           "Sender",
		AutoAcceptDrop: true,
	})

	sender.p2p.SetPeerEndpoint(receiver.id.DeviceID, "Receiver", fmt.Sprintf("127.0.0.1:%d", receiver.p2p.QUICPort()))

	deadline := time.Now().Add(5 * time.Second)
	for !sender.p2p.IsConnected(receiver.id.DeviceID) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	// Create 4 MiB file (8 chunks of 512 KiB)
	srcFile := filepath.Join(t.TempDir(), "large.bin")
	payload := bytes.Repeat([]byte("RESUME-TEST-1234"), 262144)
	_ = os.WriteFile(srcFile, payload, 0644)

	rec, err := sender.drop.SendPaths(ctx, receiver.id.DeviceID, []string{srcFile})
	if err != nil {
		t.Fatal(err)
	}

	// Immediately pause then resume (even if already transferring or completed)
	_ = sender.drop.PauseTransfer(rec.TransferID)
	_ = sender.drop.ResumeTransfer(ctx, rec.TransferID)

	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		r, err := receiver.store.GetDropTransfer(rec.TransferID)
		if err == nil && r.Status == "completed" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	finalRecv, err := receiver.store.GetDropTransfer(rec.TransferID)
	if err != nil || finalRecv.Status != "completed" {
		t.Fatalf("expected completed transfer after resume, got %+v err=%v", finalRecv, err)
	}
}

type dropTestNode struct {
	id    *protocol.Identity
	store *store.Store
	p2p   *p2p.Manager
	drop  *Manager
}

func newDropNode(t *testing.T, ctx context.Context, name string) *dropTestNode {
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

	var bMgr *Manager
	pMgr := p2p.NewManager(id, cfg, st, p2p.Handlers{
		OnControlMessage: func(c context.Context, peerID string, env protocol.ControlEnvelope) {
			if bMgr == nil {
				return
			}
			switch env.Type {
			case protocol.CtrlDropOffer:
				bMgr.HandleOffer(c, peerID, env.Payload)
			case protocol.CtrlDropResponse:
				bMgr.HandleResponse(c, peerID, env.Payload)
			case protocol.CtrlDropComplete:
				bMgr.HandleComplete(peerID, env.Payload)
			case protocol.CtrlDropPause:
				bMgr.HandlePause(peerID, env.Payload)
			case protocol.CtrlDropResume:
				bMgr.HandleResume(peerID, env.Payload)
			case protocol.CtrlDropCancel:
				bMgr.HandleCancel(peerID, env.Payload)
			}
		},
		OnDataStream: func(c context.Context, peerID string, kind protocol.DataRequestKind, stream *quic.Stream) {
			if kind == protocol.DataReqDropChunk && bMgr != nil {
				bMgr.ServeChunkStream(c, peerID, stream)
			}
		},
	})
	bMgr = NewManager(cfg, st, pMgr, nil)
	if err := pMgr.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return &dropTestNode{id: id, store: st, p2p: pMgr, drop: bMgr}
}

func TestDropOfflineCollectCollisionDeduplication(t *testing.T) {
	root := t.TempDir()
	d1 := filepath.Join(root, "dir1")
	d2 := filepath.Join(root, "dir2")
	_ = os.MkdirAll(filepath.Join(d1, "assets"), 0755)
	_ = os.MkdirAll(filepath.Join(d2, "assets"), 0755)

	f1 := filepath.Join(d1, "report.txt")
	f2 := filepath.Join(d2, "report.txt")
	_ = os.WriteFile(f1, []byte("first"), 0644)
	_ = os.WriteFile(f2, []byte("second"), 0644)
	_ = os.WriteFile(filepath.Join(d1, "assets", "a.txt"), []byte("a1"), 0644)
	_ = os.WriteFile(filepath.Join(d2, "assets", "b.txt"), []byte("b2"), 0644)

	entries, srcMap, total, count, err := collectPathsForDrop([]string{
		f1, f2,
		filepath.Join(d1, "assets"),
		filepath.Join(d2, "assets"),
	})
	if err != nil {
		t.Fatalf("collectPathsForDrop failed: %v", err)
	}
	if count != 4 || total != int64(len("first")+len("second")+len("a1")+len("b2")) {
		t.Fatalf("unexpected count=%d total=%d", count, total)
	}
	if srcMap["report.txt"] != f1 {
		t.Fatalf("expected report.txt -> %s, got %q", f1, srcMap["report.txt"])
	}
	if srcMap["report (1).txt"] != f2 {
		t.Fatalf("expected deduplicated report (1).txt -> %s, got %q", f2, srcMap["report (1).txt"])
	}
	if _, ok := srcMap["assets/a.txt"]; !ok {
		t.Fatalf("expected assets/a.txt in srcMap, got %+v", srcMap)
	}
	if _, ok := srcMap["assets (1)/b.txt"]; !ok {
		t.Fatalf("expected assets (1)/b.txt in srcMap, got %+v", srcMap)
	}
	_ = entries
}

func TestDropOfflineQueuePrehashedAndFlushOnPause(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	cfg, err := config.LoadOrCreateDaemonConfig(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	id, err := protocol.LoadOrCreateIdentity(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	pMgr := p2p.NewManager(id, cfg, st, p2p.Handlers{})
	mgr := NewManager(cfg, st, pMgr, nil)
	mgr.SetRootContext(ctx)

	offlinePeer := "GENS-0000-0000-0000-0000-0000-0000-0001"
	_ = st.UpsertDevice(store.DeviceRecord{DeviceID: offlinePeer, Name: "OfflinePeer"})

	// 1. Offline file queuing via SendPaths -> preparing -> queued_offline
	srcFile := filepath.Join(t.TempDir(), "offline.bin")
	_ = os.WriteFile(srcFile, bytes.Repeat([]byte("OFFLINE"), 1024), 0644)
	rec, err := mgr.SendPaths(ctx, offlinePeer, []string{srcFile})
	if err != nil {
		t.Fatalf("SendPaths offline failed: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r, gErr := st.GetDropTransfer(rec.TransferID)
		if gErr == nil && r.Status == "queued_offline" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	queued, err := st.GetDropTransfer(rec.TransferID)
	if err != nil || queued.Status != "queued_offline" {
		t.Fatalf("expected queued_offline status, got %+v err=%v", queued, err)
	}

	// 2. Prehashed staged paths -> stored with StagingDir, cleaned on CancelTransfer
	stageDir := filepath.Join(dataDir, "drop_staging", "stage-test")
	_ = os.MkdirAll(stageDir, 0755)
	stageFile := filepath.Join(stageDir, "up.txt")
	content := []byte("staged-prehashed-payload")
	_ = os.WriteFile(stageFile, content, 0644)
	blocks, fileHash, sz, err := chunker.HashReaderAdaptive(bytes.NewReader(content), chunker.AdaptiveBlockSize(int64(len(content))))
	if err != nil {
		t.Fatal(err)
	}
	stagedRec, err := mgr.SendPrehashedStagedPaths(ctx, offlinePeer, stageDir, []StagedFile{{
		RelPath:   "up.txt",
		LocalPath: stageFile,
		Size:      sz,
		BlockSize: chunker.AdaptiveBlockSize(sz),
		Blocks:    blocks,
		FileHash:  fileHash,
	}})
	if err != nil {
		t.Fatalf("SendPrehashedStagedPaths failed: %v", err)
	}
	if stagedRec.Status != "queued_offline" || stagedRec.StagingDir != stageDir {
		t.Fatalf("unexpected staged record: %+v", stagedRec)
	}
	activeDirs, err := st.ListActiveDropStagingDirs()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := activeDirs[stageDir]; !ok {
		t.Fatalf("expected active staging dir %s to be listed in store", stageDir)
	}
	if err := mgr.CancelTransfer(stagedRec.TransferID); err != nil {
		t.Fatalf("CancelTransfer failed: %v", err)
	}
	if _, statErr := os.Stat(stageDir); !os.IsNotExist(statErr) {
		t.Fatalf("expected staging dir %s removed on cancel, stat err=%v", stageDir, statErr)
	}

	// 3. Flush receive progress (<16 block batch) cleanly on PauseTransfer
	recvID := "recv-pause-flush"
	destDir := filepath.Join(t.TempDir(), "inbox")
	_ = os.MkdirAll(destDir, 0755)
	_ = st.UpsertDropTransfer(store.DropTransferRecord{
		TransferID:      recvID,
		Direction:       "receive",
		Kind:            "files",
		PeerID:          offlinePeer,
		Title:           "test",
		DestDir:         destDir,
		TotalSize:       4096,
		Status:          "transferring",
		Files:           []protocol.DropFileEntry{{RelPath: "f.bin", Size: 4096}},
		CompletedBlocks: map[string][]int{},
	})
	if _, ok := mgr.getActiveRecvDestDir(recvID, offlinePeer); !ok {
		t.Fatal("expected active recv buffer")
	}
	mgr.recordRecvBlock(recvID, "f.bin", 0, 1024)
	mgr.recordRecvBlock(recvID, "f.bin", 1, 1024)
	if err := mgr.PauseTransfer(recvID); err != nil {
		t.Fatalf("PauseTransfer failed: %v", err)
	}
	pausedRec, err := st.GetDropTransfer(recvID)
	if err != nil {
		t.Fatal(err)
	}
	if pausedRec.Status != "paused" || len(pausedRec.CompletedBlocks["f.bin"]) != 2 || pausedRec.TransferredBytes != 2048 {
		t.Fatalf("expected flushed 2 blocks & 2048 bytes on pause, got status=%s blocks=%v bytes=%d",
			pausedRec.Status, pausedRec.CompletedBlocks, pausedRec.TransferredBytes)
	}
}
