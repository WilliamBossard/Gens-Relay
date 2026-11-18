package relay

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRelayConfigBanAuthAndAdminHandlers(t *testing.T) {
	dir := t.TempDir()
	rc, err := LoadOrCreateConfig(dir, Options{
		EnableBlindRelay: true,
		AdminUser:        "ops",
		AdminPassword:    "pass123",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !rc.VerifyAdminAuth("ops", "pass123") || rc.VerifyAdminAuth("ops", "bad") {
		t.Fatal("VerifyAdminAuth mismatch")
	}

	srv := NewServer(Options{Config: rc})
	handler := srv.AdminHandler()

	// Unauthenticated remote request must fail with 401
	req := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	req.RemoteAddr = "198.51.100.10:12345"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without basic auth, got %d", rec.Code)
	}

	// Authenticated request succeeds
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	req2.RemoteAddr = "198.51.100.10:12345"
	req2.SetBasicAuth("ops", "pass123")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid auth, got %d", rec2.Code)
	}
	var st map[string]any
	_ = json.Unmarshal(rec2.Body.Bytes(), &st)
	if st["blind_relay_enabled"] != true {
		t.Fatalf("expected blind_relay_enabled=true, got %+v", st)
	}

	// Ban & Unban via Admin API
	banBody, _ := json.Marshal(map[string]string{"device_id": "AAAA-BBBB", "reason": "abuse"})
	reqBan := httptest.NewRequest(http.MethodPost, "/api/v1/bans", bytes.NewReader(banBody))
	reqBan.SetBasicAuth("ops", "pass123")
	recBan := httptest.NewRecorder()
	handler.ServeHTTP(recBan, reqBan)
	if recBan.Code != http.StatusOK || !rc.IsBanned("AAAA-BBBB") {
		t.Fatalf("expected device banned, code=%d", recBan.Code)
	}

	reqUnban := httptest.NewRequest(http.MethodDelete, "/api/v1/bans/AAAA-BBBB", nil)
	reqUnban.SetBasicAuth("ops", "pass123")
	recUnban := httptest.NewRecorder()
	handler.ServeHTTP(recUnban, reqUnban)
	if recUnban.Code != http.StatusOK || rc.IsBanned("AAAA-BBBB") {
		t.Fatalf("expected device unbanned, code=%d", recUnban.Code)
	}
}
