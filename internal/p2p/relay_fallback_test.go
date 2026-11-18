package p2p

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WilliamBossard/Gens-Relay/internal/config"
	"github.com/WilliamBossard/Gens-Relay/internal/relay"
	"github.com/WilliamBossard/Gens-Relay/internal/store"
	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
)

func TestBlindRelayQUICmTLSFallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Start relay-server with blind E2EE relay enabled
	rSrv := relay.NewServer(relay.Options{EnableBlindRelay: true})
	ts := httptest.NewServer(rSrv.Handler())
	defer ts.Close()
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	var gotCtrl atomic.Bool
	nodeA := newRelayTestNode(t, ctx, "NodeA", wsURL, nil)
	nodeB := newRelayTestNode(t, ctx, "NodeB", wsURL, func(env protocol.ControlEnvelope) {
		if env.Type == protocol.CtrlFolderIndex {
			gotCtrl.Store(true)
		}
	})
	nodeA.p2p.forceRelayOnly = true
	nodeB.p2p.forceRelayOnly = true

	_ = nodeA.store.UpsertDevice(store.DeviceRecord{DeviceID: nodeB.id.DeviceID, Name: "NodeB"})
	_ = nodeB.store.UpsertDevice(store.DeviceRecord{DeviceID: nodeA.id.DeviceID, Name: "NodeA"})

	// Wait for both nodes to register on relay-server
	deadline := time.Now().Add(8 * time.Second)
	for (!nodeA.p2p.RelayConnected() || !nodeB.p2p.RelayConnected()) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !nodeA.p2p.RelayConnected() || !nodeB.p2p.RelayConnected() {
		t.Fatal("nodes failed to register on relay-server")
	}

	// Clear direct LAN/WAN hints so nodeA MUST use blind E2EE relay fallback!
	_ = nodeA.p2p.QueryRelayPeers(ctx)
	_ = nodeB.p2p.QueryRelayPeers(ctx)

	time.Sleep(300 * time.Millisecond)
	nodeA.p2p.mu.Lock()
	h := nodeA.p2p.hints[nodeB.id.DeviceID]
	h.LANAddrs = nil
	h.ObservedAddr = ""
	h.Online = true
	nodeA.p2p.hints[nodeB.id.DeviceID] = h
	nodeA.p2p.mu.Unlock()

	go nodeA.p2p.tryConnectPeer(ctx, nodeB.id.DeviceID)

	deadline = time.Now().Add(10 * time.Second)
	for !nodeA.p2p.IsConnected(nodeB.id.DeviceID) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !nodeA.p2p.IsConnected(nodeB.id.DeviceID) {
		t.Fatal("failed to establish QUIC mTLS connection over blind relay")
	}

	// Verify route is reported as "relay"
	var route string
	for _, st := range nodeA.p2p.PeerStatuses() {
		if st.DeviceID == nodeB.id.DeviceID {
			route = st.Route
		}
	}
	if route != "relay" {
		t.Fatalf("expected route 'relay', got %q", route)
	}

	// Send control envelope through encrypted QUIC over blind relay
	if err := nodeA.p2p.SendControl(nodeB.id.DeviceID, protocol.CtrlFolderIndex, protocol.FolderIndexMsg{FolderID: "test"}); err != nil {
		t.Fatalf("SendControl over blind relay failed: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for !gotCtrl.Load() && time.Now().Before(deadline) {
		time.Sleep(30 * time.Millisecond)
	}
	if !gotCtrl.Load() {
		t.Fatal("nodeB did not receive control message over blind relay")
	}
}

type relayTestNode struct {
	id    *protocol.Identity
	store *store.Store
	p2p   *Manager
}

func newRelayTestNode(t *testing.T, ctx context.Context, name, wsURL string, onCtrl func(protocol.ControlEnvelope)) *relayTestNode {
	t.Helper()
	dir := t.TempDir()
	cfg, err := config.LoadOrCreateDaemonConfig(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = cfg.Update(func(c *config.DaemonConfig) {
		c.NodeName = name
		c.QUICPort = 0
		c.EnableLANDisc = false
		c.EnableRelayFall = true
		c.RelayURL = wsURL
	})
	id, err := protocol.LoadOrCreateIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	mgr := NewManager(id, cfg, st, Handlers{
		OnControlMessage: func(_ context.Context, _ string, env protocol.ControlEnvelope) {
			if onCtrl != nil {
				onCtrl(env)
			}
		},
	})
	if err := mgr.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return &relayTestNode{id: id, store: st, p2p: mgr}
}
