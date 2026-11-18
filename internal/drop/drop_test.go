package drop

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
