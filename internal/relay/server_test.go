package relay

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
	"github.com/coder/websocket"
)

func TestRelaySignalingAndPairing(t *testing.T) {
	srv := NewServer(Options{AuthToken: "net-secret", EnableBlindRelay: false})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	idA, err := protocol.LoadOrCreateIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	idB, err := protocol.LoadOrCreateIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsA := connectAndAuth(t, ctx, wsURL, idA, "WinPC", "net-secret", 22001)
	defer wsA.Close(websocket.StatusNormalClosure, "")

	wsB := connectAndAuth(t, ctx, wsURL, idB, "TrueNAS", "net-secret", 22002)
	defer wsB.Close(websocket.StatusNormalClosure, "")

	// A creates pair code
	if err := writeWSJSON(ctx, wsA, protocol.SignalingMessage{Type: protocol.SigCreatePairCode}); err != nil {
		t.Fatal(err)
	}
	codeMsg, err := readWSJSON(ctx, wsA)
	if err != nil || codeMsg.Type != protocol.SigPairCodeCreated || codeMsg.PairCode == "" {
		t.Fatalf("unexpected pair code response: %+v err=%v", codeMsg, err)
	}

	// B redeems pair code
	if err := writeWSJSON(ctx, wsB, protocol.SignalingMessage{
		Type:     protocol.SigRedeemPairCode,
		PairCode: codeMsg.PairCode,
	}); err != nil {
		t.Fatal(err)
	}
	resMsg, err := readWSJSON(ctx, wsB)
	if err != nil || resMsg.Type != protocol.SigPairResolved || resMsg.Peer == nil {
		t.Fatalf("unexpected pair resolve: %+v err=%v", resMsg, err)
	}
	if resMsg.Peer.DeviceID != idA.DeviceID || resMsg.Peer.QUICPort != 22001 {
		t.Fatalf("resolved wrong peer: %+v", resMsg.Peer)
	}

	// Check /healthz reports stores_files: false
	resp, err := ts.Client().Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var health map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatal(err)
	}
	if health["stores_files"] != false {
		t.Fatalf("expected stores_files=false, got %v", health["stores_files"])
	}
}

func connectAndAuth(t *testing.T, ctx context.Context, wsURL string, id *protocol.Identity, name, token string, quicPort int) *websocket.Conn {
	t.Helper()
	ws, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	ch, err := readWSJSON(ctx, ws)
	if err != nil || ch.Type != protocol.SigChallenge {
		t.Fatalf("expected challenge: %+v err=%v", ch, err)
	}
	nonce, _ := base64.StdEncoding.DecodeString(ch.Challenge)
	sig := id.SignChallenge(nonce)
	if err := writeWSJSON(ctx, ws, protocol.SignalingMessage{
		Type:         protocol.SigRegister,
		DeviceID:     id.DeviceID,
		Name:         name,
		PubKeyB64:    base64.StdEncoding.EncodeToString(id.PubKeyDER),
		SignatureB64: base64.StdEncoding.EncodeToString(sig),
		AuthToken:    token,
		QUICPort:     quicPort,
	}); err != nil {
		t.Fatal(err)
	}
	ack, err := readWSJSON(ctx, ws)
	if err != nil || ack.Type != protocol.SigRegistered {
		t.Fatalf("expected registered: %+v err=%v", ack, err)
	}
	return ws
}

func TestRelayPairRequestAndAccept(t *testing.T) {
	srv := NewServer(Options{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	idA, _ := protocol.LoadOrCreateIdentity(t.TempDir())
	idB, _ := protocol.LoadOrCreateIdentity(t.TempDir())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsA := connectAndAuth(t, ctx, wsURL, idA, "WinPC", "", 22001)
	defer wsA.Close(websocket.StatusNormalClosure, "")
	wsB := connectAndAuth(t, ctx, wsURL, idB, "UbuntuVM", "", 22002)
	defer wsB.Close(websocket.StatusNormalClosure, "")

	// A sends friend/pair request to B
	if err := writeWSJSON(ctx, wsA, protocol.SignalingMessage{
		Type:     protocol.SigPairRequest,
		TargetID: idB.DeviceID,
		Message:  "Salut c'est mon PC",
	}); err != nil {
		t.Fatal(err)
	}

	inc, err := readWSJSON(ctx, wsB)
	if err != nil || inc.Type != protocol.SigPairIncoming || inc.SenderID != idA.DeviceID {
		t.Fatalf("expected pair_incoming on B, got %+v err=%v", inc, err)
	}
	if inc.Message != "Salut c'est mon PC" {
		t.Fatalf("unexpected message: %q", inc.Message)
	}

	// B accepts
	if err := writeWSJSON(ctx, wsB, protocol.SignalingMessage{
		Type:     protocol.SigPairAccept,
		TargetID: idA.DeviceID,
	}); err != nil {
		t.Fatal(err)
	}

	acc, err := readWSJSON(ctx, wsA)
	if err != nil || acc.Type != protocol.SigPairAccepted || acc.SenderID != idB.DeviceID {
		t.Fatalf("expected pair_accepted on A, got %+v err=%v", acc, err)
	}
}
