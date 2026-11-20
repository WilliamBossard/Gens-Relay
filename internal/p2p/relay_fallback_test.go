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

func TestBlindRelayQUICmTLSFallbackWS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rSrv := relay.NewServer(relay.Options{EnableBlindRelay: true})
	ts := httptest.NewServer(rSrv.Handler())
	defer ts.Close()
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	var gotCtrl atomic.Bool
	nodeA := newRelayTestNode(t, ctx, "NodeA", []string{wsURL}, nil)
	nodeB := newRelayTestNode(t, ctx, "NodeB", []string{wsURL}, func(env protocol.ControlEnvelope) {
		if env.Type == protocol.CtrlFolderIndex {
			gotCtrl.Store(true)
		}
	})
	nodeA.p2p.forceRelayOnly = true
	nodeB.p2p.forceRelayOnly = true
	nodeA.p2p.forceWSRelay = true
	nodeB.p2p.forceWSRelay = true

	runRelayFallbackCheck(t, ctx, nodeA, nodeB, &gotCtrl)
	if mode := nodeA.p2p.RelayTransportMode(); mode != "ws" {
		t.Fatalf("expected ws relay mode, got %q", mode)
	}
}

func TestNativeUDPBlindRelayAndDisconnectPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rSrv := relay.NewServer(relay.Options{EnableBlindRelay: true})
	if err := rSrv.StartUDP(ctx, "127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rSrv.Handler())
	defer ts.Close()
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	var gotCtrl atomic.Bool
	nodeA := newRelayTestNode(t, ctx, "NodeA", []string{wsURL}, nil)
	nodeB := newRelayTestNode(t, ctx, "NodeB", []string{wsURL}, func(env protocol.ControlEnvelope) {
		if env.Type == protocol.CtrlFolderIndex {
			gotCtrl.Store(true)
		}
	})
	nodeA.p2p.forceRelayOnly = true
	nodeB.p2p.forceRelayOnly = true

	_ = nodeA.store.UpsertDevice(store.DeviceRecord{DeviceID: nodeB.id.DeviceID, Name: "NodeB"})
	_ = nodeB.store.UpsertDevice(store.DeviceRecord{DeviceID: nodeA.id.DeviceID, Name: "NodeA"})

	// Wait until both nodes register AND confirm native UDP relay probe
	deadline := time.Now().Add(8 * time.Second)
	for (nodeA.p2p.RelayTransportMode() != "udp" || nodeB.p2p.RelayTransportMode() != "udp") && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if nodeA.p2p.RelayTransportMode() != "udp" || nodeB.p2p.RelayTransportMode() != "udp" {
		t.Fatalf("expected native UDP relay ready on both nodes, got A=%s B=%s", nodeA.p2p.RelayTransportMode(), nodeB.p2p.RelayTransportMode())
	}

	runRelayFallbackCheck(t, ctx, nodeA, nodeB, &gotCtrl)

	// Verify immediate DisconnectPeer revocation
	nodeA.p2p.DisconnectPeer(nodeB.id.DeviceID)
	if nodeA.p2p.IsConnected(nodeB.id.DeviceID) {
		t.Fatal("expected DisconnectPeer to close session immediately")
	}
}

func TestMultiRelayFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rSrv := relay.NewServer(relay.Options{})
	ts := httptest.NewServer(rSrv.Handler())
	defer ts.Close()
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	// First URL is dead port, second URL is live relay
	node := newRelayTestNode(t, ctx, "FailoverNode", []string{"ws://127.0.0.1:1/ws", wsURL}, nil)
	deadline := time.Now().Add(8 * time.Second)
	for !node.p2p.RelayConnected() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !node.p2p.RelayConnected() {
		t.Fatal("expected failover to second relay URL")
	}
	if node.p2p.ActiveRelayURL() != wsURL {
		t.Fatalf("expected active relay %q, got %q", wsURL, node.p2p.ActiveRelayURL())
	}
}

func runRelayFallbackCheck(t *testing.T, ctx context.Context, nodeA, nodeB *relayTestNode, gotCtrl *atomic.Bool) {
	t.Helper()
	_ = nodeA.store.UpsertDevice(store.DeviceRecord{DeviceID: nodeB.id.DeviceID, Name: "NodeB"})
	_ = nodeB.store.UpsertDevice(store.DeviceRecord{DeviceID: nodeA.id.DeviceID, Name: "NodeA"})

	deadline := time.Now().Add(8 * time.Second)
	for (!nodeA.p2p.RelayConnected() || !nodeB.p2p.RelayConnected()) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !nodeA.p2p.RelayConnected() || !nodeB.p2p.RelayConnected() {
		t.Fatal("nodes failed to register on relay-server")
	}

	_ = nodeA.p2p.QueryRelayPeers(ctx)
	_ = nodeB.p2p.QueryRelayPeers(ctx)

	time.Sleep(250 * time.Millisecond)
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

	var route string
	for _, st := range nodeA.p2p.PeerStatuses() {
		if st.DeviceID == nodeB.id.DeviceID {
			route = st.Route
		}
	}
	if route != "relay" {
		t.Fatalf("expected route 'relay', got %q", route)
	}

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

func newRelayTestNode(t *testing.T, ctx context.Context, name string, wsURLs []string, onCtrl func(protocol.ControlEnvelope)) *relayTestNode {
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
		c.EnableUPnP = false
		c.EnableRelayFall = true
		c.RelayURLs = wsURLs
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
