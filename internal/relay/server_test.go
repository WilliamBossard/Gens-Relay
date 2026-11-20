package relay

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
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
	authProof := ""
	if token != "" {
		authProof = protocol.ComputeAuthProof(token, nonce)
	}
	if err := writeWSJSON(ctx, ws, protocol.SignalingMessage{
		Type:         protocol.SigRegister,
		DeviceID:     id.DeviceID,
		Name:         name,
		PubKeyB64:    base64.StdEncoding.EncodeToString(id.PubKeyDER),
		SignatureB64: base64.StdEncoding.EncodeToString(sig),
		AuthProofB64: authProof,
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

func TestRelayControlPriorityOverRelayQueue(t *testing.T) {
	srv := NewServer(Options{EnableBlindRelay: true})
	c := newClientConn("DEV-1", "Node", nil, 22000, "1.2.3.4", "1.2.3.4:22000", "st", [16]byte{}, nil)

	// Completely fill the best-effort relayCh
	for i := 0; i < cap(c.relayCh); i++ {
		if err := c.sendBinary(context.Background(), []byte{0x01, 1, 'A', 9}, nil); err != nil {
			t.Fatalf("unexpected error before saturation: %v", err)
		}
	}
	if err := c.sendBinary(context.Background(), []byte{0x01, 1, 'A', 9}, nil); err == nil {
		t.Fatal("expected relayCh saturation error")
	}

	// Critical signaling message must still enqueue into ctrlCh without dropping
	if err := c.send(context.Background(), protocol.SignalingMessage{Type: protocol.SigPunchNotify, SenderID: "DEV-2"}); err != nil {
		t.Fatalf("expected ctrlCh to accept signaling even when relayCh is saturated: %v", err)
	}
	_ = srv
}

func TestRelayStrictTrustProxyAndAdminSetupWizard(t *testing.T) {
	rc, err := LoadOrCreateConfig(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(Options{Config: rc})
	handler := srv.AdminHandler()

	// TrustProxy=false must ignore X-Forwarded-For even from private proxy IPs
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.RemoteAddr = "10.0.0.5:4444"
	req.Header.Set("X-Forwarded-For", "198.51.100.99")
	if got := srv.extractRemoteIP(req); got != "10.0.0.5" {
		t.Fatalf("expected strict TrustProxy=false to return 10.0.0.5, got %q", got)
	}
	_ = rc.Update(func(c *RelayConfig) { c.TrustProxy = true })
	if got := srv.extractRemoteIP(req); got != "198.51.100.99" {
		t.Fatalf("expected TrustProxy=true to extract XFF IP, got %q", got)
	}

	// Before setup completes, non-loopback API requests return 403 setup_required
	remoteReq := httptest.NewRequest(http.MethodGet, "/api/v1/clients", nil)
	remoteReq.RemoteAddr = "203.0.113.10:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, remoteReq)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 setup_required before setup, got %d", rec.Code)
	}

	// Setup from public IP without setup_token must be rejected
	badSetup, _ := json.Marshal(map[string]any{"admin_user": "root", "admin_password": "password123"})
	setupReq := httptest.NewRequest(http.MethodPost, "/api/v1/setup", bytes.NewReader(badSetup))
	setupReq.RemoteAddr = "203.0.113.10:1234"
	recSetup := httptest.NewRecorder()
	handler.ServeHTTP(recSetup, setupReq)
	if recSetup.Code != http.StatusForbidden {
		t.Fatalf("expected 403 without setup_token from public IP, got %d", recSetup.Code)
	}

	// Valid setup with setup_token succeeds
	goodSetup, _ := json.Marshal(map[string]any{
		"setup_token":        srv.SetupToken(),
		"admin_user":         "admin",
		"admin_password":     "supersecret",
		"enable_blind_relay": true,
	})
	setupReq2 := httptest.NewRequest(http.MethodPost, "/api/v1/setup", bytes.NewReader(goodSetup))
	setupReq2.RemoteAddr = "203.0.113.10:1234"
	recSetup2 := httptest.NewRecorder()
	handler.ServeHTTP(recSetup2, setupReq2)
	if recSetup2.Code != http.StatusOK {
		t.Fatalf("expected 200 on setup completion, got %d (%s)", recSetup2.Code, recSetup2.Body.String())
	}

	// 5 failed Basic Auth attempts trigger 429 rate-limit before bcrypt
	for i := 0; i < 5; i++ {
		badAuth := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
		badAuth.RemoteAddr = "203.0.113.77:9999"
		badAuth.SetBasicAuth("admin", "wrong")
		r := httptest.NewRecorder()
		handler.ServeHTTP(r, badAuth)
		if r.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d expected 401, got %d", i, r.Code)
		}
	}
	locked := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	locked.RemoteAddr = "203.0.113.77:9999"
	locked.SetBasicAuth("admin", "supersecret")
	rLocked := httptest.NewRecorder()
	handler.ServeHTTP(rLocked, locked)
	if rLocked.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after 5 failed auth attempts, got %d", rLocked.Code)
	}
}
