package p2p

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/WilliamBossard/Gens-Relay/internal/config"
	"github.com/WilliamBossard/Gens-Relay/internal/store"
	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
	"github.com/quic-go/quic-go"
)

// TestControlFailurePreservesDataStreams guards the B1 fix: a failed control
// stream write must invalidate ONLY the control stream. The QUIC connection
// and data streams must survive, and control must re-establish on a fresh
// stream. Previously any control write failure closed the whole connection.
func TestControlFailurePreservesDataStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nodeA := newDirectTestNode(t, ctx, "CtrlA")
	nodeB := newDirectTestNode(t, ctx, "CtrlB")

	// Mutually allow each other and exchange direct endpoints.
	_ = nodeA.store.UpsertDevice(store.DeviceRecord{DeviceID: nodeB.id.DeviceID, Name: "CtrlB"})
	_ = nodeB.store.UpsertDevice(store.DeviceRecord{DeviceID: nodeA.id.DeviceID, Name: "CtrlA"})
	nodeA.p2p.SetPeerEndpoint(nodeB.id.DeviceID, "CtrlB", fmt.Sprintf("127.0.0.1:%d", nodeB.p2p.QUICPort()))
	nodeB.p2p.SetPeerEndpoint(nodeA.id.DeviceID, "CtrlA", fmt.Sprintf("127.0.0.1:%d", nodeA.p2p.QUICPort()))

	deadline := time.Now().Add(15 * time.Second)
	for {
		okA := nodeA.p2p.IsConnected(nodeB.id.DeviceID)
		okB := nodeB.p2p.IsConnected(nodeA.id.DeviceID)
		if okA && okB {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("direct QUIC connection not established: A=%v B=%v", okA, okB)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Baseline: control and data streams both work.
	if err := nodeA.p2p.SendControl(nodeB.id.DeviceID, protocol.CtrlHello, protocol.HelloMsg{DeviceID: nodeA.id.DeviceID}); err != nil {
		t.Fatalf("baseline SendControl failed: %v", err)
	}
	if !waitCtrlType(t, nodeB, protocol.CtrlHello) {
		t.Fatal("baseline: hello not received")
	}
	exchangeData(t, nodeA, nodeB, "before")

	// Simulate a control stream failure: A closes its registered control
	// stream. quic-go then rejects the next write with ErrStreamClosed, and
	// B's reader sees EOF and drops its pointer — exactly the recovery path
	// the B1 fix must handle without tearing down the connection.
	{
		nodeA.p2p.mu.Lock()
		sess := nodeA.p2p.sessions[nodeB.id.DeviceID]
		if sess == nil || sess.ctrlStream == nil {
			t.Fatal("A has no registered control stream after baseline exchange")
		}
		ctrl := sess.ctrlStream
		nodeA.p2p.mu.Unlock()
		_ = ctrl.Close()
	}
	// Give B's reader time to observe the stream reset and drop its pointer.
	time.Sleep(250 * time.Millisecond)

	err := nodeA.p2p.SendControl(nodeB.id.DeviceID, protocol.CtrlHello, protocol.HelloMsg{DeviceID: nodeA.id.DeviceID})
	if err == nil {
		t.Fatal("expected SendControl to fail on the dead control stream")
	}

	// Regression check 1: A's QUIC connection must still be alive
	// (previously CloseWithError(3, ...) tore it down).
	nodeA.p2p.mu.RLock()
	sess := nodeA.p2p.sessions[nodeB.id.DeviceID]
	var connErr error
	if sess == nil {
		connErr = fmt.Errorf("session vanished")
	} else {
		connErr = sess.conn.Context().Err()
	}
	nodeA.p2p.mu.RUnlock()
	if connErr != nil {
		t.Fatalf("QUIC connection was torn down by a control stream failure: %v", connErr)
	}

	// Regression check 2: control re-establishes on a fresh stream and B
	// receives it.
	if err := nodeA.p2p.SendControl(nodeB.id.DeviceID, protocol.CtrlHello, protocol.HelloMsg{DeviceID: nodeA.id.DeviceID}); err != nil {
		t.Fatalf("SendControl on the fresh control stream failed: %v", err)
	}
	if !waitCtrlType(t, nodeB, protocol.CtrlHello) {
		t.Fatal("B did not receive the hello sent over the new control stream")
	}

	// Regression check 3: data streams keep working after the failure.
	exchangeData(t, nodeA, nodeB, "after")
}

// --- helpers ---

type directNode struct {
	id    *protocol.Identity
	store *store.Store
	p2p   *Manager

	mu   sync.Mutex
	ctl  []string        // received control message types, in order
	data map[string]bool // received data stream payloads
}

func newDirectTestNode(t *testing.T, ctx context.Context, name string) *directNode {
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
		c.RelayURL = ""
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

	n := &directNode{id: id, store: st, data: make(map[string]bool)}
	n.p2p = NewManager(id, cfg, st, Handlers{
		OnControlMessage: func(_ context.Context, _ string, env protocol.ControlEnvelope) {
			n.mu.Lock()
			n.ctl = append(n.ctl, env.Type)
			n.mu.Unlock()
		},
		OnDataStream: func(_ context.Context, _ string, _ protocol.DataRequestKind, stream *quic.Stream) {
			if b, err := io.ReadAll(stream); err == nil {
				n.mu.Lock()
				n.data[string(b)] = true
				n.mu.Unlock()
			}
		},
	})
	if err := n.p2p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer shutCancel()
		n.p2p.Shutdown(shutCtx)
	})
	return n
}

func (n *directNode) hasData(payload string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.data[payload]
}

func waitCtrlType(t *testing.T, n *directNode, msgType string) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		found := false
		for _, c := range n.ctl {
			if c == msgType {
				found = true
				break
			}
		}
		n.mu.Unlock()
		if found {
			return true
		}
		time.Sleep(30 * time.Millisecond)
	}
	return false
}

func exchangeData(t *testing.T, sender, recv *directNode, tag string) {
	t.Helper()
	payload := "data-" + tag
	stream, err := sender.p2p.OpenDataStream(context.Background(), recv.id.DeviceID, protocol.DataReqSyncBlock)
	if err != nil {
		t.Fatalf("OpenDataStream(%s) failed: %v", tag, err)
	}
	if _, err := stream.Write([]byte(payload)); err != nil {
		t.Fatalf("data write(%s) failed: %v", tag, err)
	}
	_ = stream.Close()

	deadline := time.Now().Add(5 * time.Second)
	for !recv.hasData(payload) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !recv.hasData(payload) {
		t.Fatalf("receiver never got data payload %q", payload)
	}
}

