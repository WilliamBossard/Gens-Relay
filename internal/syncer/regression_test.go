package syncer

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/WilliamBossard/Gens-Relay/internal/config"
	"github.com/WilliamBossard/Gens-Relay/internal/store"
	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
	"github.com/quic-go/quic-go"
)

// fakeP2P is a P2PSender that can inject failures, for regression tests of
// invite/index retry behaviour.
type fakeP2P struct {
	mu        sync.Mutex
	connected bool
	failSend  error
	sent      []fakeSentMsg
}

type fakeSentMsg struct {
	peerID  string
	msgType string
	payload any
}

func (f *fakeP2P) IsConnected(string) bool { return f.connected }

func (f *fakeP2P) SendControl(peerID, msgType string, payload any) error {
	f.mu.Lock()
	f.sent = append(f.sent, fakeSentMsg{peerID, msgType, payload})
	err := f.failSend
	f.mu.Unlock()
	return err
}

func (f *fakeP2P) OpenTrackedDataStream(ctx context.Context, peerID string, kind protocol.DataRequestKind) (*quic.Stream, func(), error) {
	return nil, func() {}, errors.New("fakeP2P: no data streams")
}

func (f *fakeP2P) CompressionEnabled() bool { return false }

func (f *fakeP2P) LimitedReadFull(ctx context.Context, r io.Reader, p []byte) (int, error) { return io.ReadFull(r, p) }

func (f *fakeP2P) LimitedWrite(ctx context.Context, w io.Writer, p []byte) (int, error) { return w.Write(p) }

func (f *fakeP2P) count(msgType string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, s := range f.sent {
		if s.msgType == msgType {
			n++
		}
	}
	return n
}

// TestInviteRetriedAfterSendFailure guards the B2 fix: when the invite send
// fails, the suppression marker must be cleared so the next OnPeerConnected
// re-invites. Previously a failed invite was lost until a manual folder update.
func TestInviteRetriedAfterSendFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	if _, err := config.LoadOrCreateDaemonConfig(dir); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	peerID := protocol.NormalizeDeviceID("BEEFCAFE")
	if err := st.UpsertDevice(store.DeviceRecord{DeviceID: peerID, Name: "Peer"}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertFolder(store.FolderRecord{
		FolderID:  "inv",
		Label:     "Inv",
		LocalPath: filepath.Join(dir, "folder"),
		Mode:      protocol.ModeSendReceive,
		Peers:     []string{peerID},
	}); err != nil {
		t.Fatal(err)
	}

	fake := &fakeP2P{connected: true, failSend: errors.New("simulated control send failure")}
	m := NewManager(protocol.NormalizeDeviceID("0BADF00D"), st, fake, nil)

	// First connection: the invite is attempted and fails.
	m.OnPeerConnected(ctx, peerID)
	if n := fake.count(protocol.CtrlFolderInvite); n != 1 {
		t.Fatalf("expected 1 invite attempt, got %d", n)
	}

	// The marker must have been cleared on failure, so the next connection
	// retries the invite.
	fake.failSend = nil
	m.OnPeerConnected(ctx, peerID)
	if n := fake.count(protocol.CtrlFolderInvite); n != 2 {
		t.Fatalf("expected the failed invite to be retried (2 attempts), got %d", n)
	}
}
