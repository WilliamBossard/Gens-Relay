package webapi

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/WilliamBossard/Gens-Relay/internal/config"
	"github.com/WilliamBossard/Gens-Relay/internal/drop"
	"github.com/WilliamBossard/Gens-Relay/internal/logs"
	"github.com/WilliamBossard/Gens-Relay/internal/p2p"
	"github.com/WilliamBossard/Gens-Relay/internal/store"
	"github.com/WilliamBossard/Gens-Relay/internal/syncer"
	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
	"github.com/quic-go/quic-go"
)

func TestWebAPILogsConflictsVersionsAndTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dir := t.TempDir()
	cfg, err := config.LoadOrCreateDaemonConfig(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = cfg.Update(func(c *config.DaemonConfig) {
		c.QUICPort = 0
		c.EnableLANDisc = false
	})
	id, err := protocol.LoadOrCreateIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	logBuf := logs.NewRingBuffer(100, nil)
	_, _ = logBuf.Write([]byte("2026/11/18 [p2p] test log line\n"))

	pMgr := p2p.NewManager(id, cfg, st, p2p.Handlers{})
	if err := pMgr.Start(ctx); err != nil {
		t.Fatal(err)
	}
	sMgr := syncer.NewManager(id.DeviceID, st, pMgr, nil)
	dMgr := drop.NewManager(cfg, st, pMgr, nil)
	apiSrv := NewServer(id, cfg, st, pMgr, sMgr, dMgr, logBuf)

	// Setup folder with a conflict and a versioned file
	folderDir := filepath.Join(dir, "sync")
	_ = os.MkdirAll(filepath.Join(folderDir, ".gensversions", "sub"), 0755)
	_ = os.WriteFile(filepath.Join(folderDir, "doc.txt"), []byte("current"), 0644)
	_ = os.WriteFile(filepath.Join(folderDir, "doc.sync-conflict-20261118-120000-ABC123.txt"), []byte("from-conflict"), 0644)
	_ = os.WriteFile(filepath.Join(folderDir, ".gensversions", "sub", "old~20261118-120000.txt"), []byte("archived"), 0644)

	_ = st.UpsertFolder(store.FolderRecord{
		FolderID:  "f1",
		Label:     "F1",
		LocalPath: folderDir,
		Mode:      protocol.ModeSendReceive,
	})

	ts := httptest.NewServer(apiSrv.Handler())
	defer ts.Close()

	// 1. Logs endpoint
	resp, err := ts.Client().Get(ts.URL + "/api/v1/logs")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("GET /logs failed: err=%v status=%v", err, resp)
	}
	var logEntries []logs.Entry
	_ = json.NewDecoder(resp.Body).Decode(&logEntries)
	resp.Body.Close()
	if len(logEntries) != 1 || logEntries[0].Message != "2026/11/18 [p2p] test log line" {
		t.Fatalf("unexpected logs: %+v", logEntries)
	}

	// 2. Conflicts list & resolve (keep_conflict)
	resp, err = ts.Client().Get(ts.URL + "/api/v1/folders/f1/conflicts")
	if err != nil {
		t.Fatal(err)
	}
	var conflicts []syncer.ConflictItem
	_ = json.NewDecoder(resp.Body).Decode(&conflicts)
	resp.Body.Close()
	if len(conflicts) != 1 || conflicts[0].OriginalPath != "doc.txt" {
		t.Fatalf("unexpected conflicts: %+v", conflicts)
	}

	resolveBody, _ := json.Marshal(map[string]string{
		"rel_path": conflicts[0].RelPath,
		"action":   "keep_conflict",
	})
	resp, err = ts.Client().Post(ts.URL+"/api/v1/folders/f1/conflicts/resolve", "application/json", bytes.NewReader(resolveBody))
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("resolve conflict failed: %v", err)
	}
	resp.Body.Close()
	gotDoc, _ := os.ReadFile(filepath.Join(folderDir, "doc.txt"))
	if string(gotDoc) != "from-conflict" {
		t.Fatalf("expected conflict content restored to doc.txt, got %q", string(gotDoc))
	}

	// 3. Versions list & restore
	resp, err = ts.Client().Get(ts.URL + "/api/v1/folders/f1/versions")
	if err != nil {
		t.Fatal(err)
	}
	var versions []syncer.VersionItem
	_ = json.NewDecoder(resp.Body).Decode(&versions)
	resp.Body.Close()
	if len(versions) < 1 {
		t.Fatalf("expected archived versions, got %+v", versions)
	}

	restoreBody, _ := json.Marshal(map[string]string{"archive_path": "sub/old~20261118-120000.txt"})
	resp, err = ts.Client().Post(ts.URL+"/api/v1/folders/f1/versions/restore", "application/json", bytes.NewReader(restoreBody))
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("restore version failed: %v", err)
	}
	resp.Body.Close()
	gotRestored, _ := os.ReadFile(filepath.Join(folderDir, "sub", "old.txt"))
	if string(gotRestored) != "archived" {
		t.Fatalf("expected restored version 'archived', got %q", string(gotRestored))
	}

	// 4. Web UI HTTPS certificate generation & TLS serving
	certPath, keyPath, err := EnsureWebCert(dir)
	if err != nil {
		t.Fatalf("EnsureWebCert failed: %v", err)
	}
	tlsSrv := httptest.NewUnstartedServer(apiSrv.Handler())
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	tlsSrv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	tlsSrv.StartTLS()
	defer tlsSrv.Close()

	httpsClient := tlsSrv.Client()
	resp, err = httpsClient.Get(tlsSrv.URL + "/api/v1/status")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("HTTPS /api/v1/status failed: %v", err)
	}
	resp.Body.Close()
}

func TestQuickDropFolderMultipartUploadEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Sender + Receiver over localhost QUIC mTLS
	senderDir := t.TempDir()
	recvDir := t.TempDir()

	sCfg, _ := config.LoadOrCreateDaemonConfig(senderDir)
	rCfg, _ := config.LoadOrCreateDaemonConfig(recvDir)
	_ = sCfg.Update(func(c *config.DaemonConfig) { c.QUICPort = 0; c.EnableLANDisc = false })
	_ = rCfg.Update(func(c *config.DaemonConfig) { c.QUICPort = 0; c.EnableLANDisc = false })

	sID, _ := protocol.LoadOrCreateIdentity(senderDir)
	rID, _ := protocol.LoadOrCreateIdentity(recvDir)
	sStore, _ := store.Open(senderDir)
	rStore, _ := store.Open(recvDir)
	defer sStore.Close()
	defer rStore.Close()

	_ = sStore.UpsertDevice(store.DeviceRecord{DeviceID: rID.DeviceID, Name: "Recv"})
	_ = rStore.UpsertDevice(store.DeviceRecord{DeviceID: sID.DeviceID, Name: "Send", AutoAcceptDrop: true})

	var sDrop, rDrop *drop.Manager
	sP2P := p2p.NewManager(sID, sCfg, sStore, p2p.Handlers{
		OnControlMessage: func(c context.Context, pid string, env protocol.ControlEnvelope) {
			if env.Type == protocol.CtrlDropResponse {
				sDrop.HandleResponse(c, pid, env.Payload)
			}
		},
	})
	rP2P := p2p.NewManager(rID, rCfg, rStore, p2p.Handlers{
		OnControlMessage: func(c context.Context, pid string, env protocol.ControlEnvelope) {
			switch env.Type {
			case protocol.CtrlDropOffer:
				rDrop.HandleOffer(c, pid, env.Payload)
			case protocol.CtrlDropComplete:
				rDrop.HandleComplete(pid, env.Payload)
			}
		},
		OnDataStream: func(c context.Context, pid string, kind protocol.DataRequestKind, st *quic.Stream) {
			if kind == protocol.DataReqDropChunk {
				rDrop.ServeChunkStream(c, pid, st)
			}
		},
	})
	if err := sP2P.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := rP2P.Start(ctx); err != nil {
		t.Fatal(err)
	}
	sDrop = drop.NewManager(sCfg, sStore, sP2P, nil)
	rDrop = drop.NewManager(rCfg, rStore, rP2P, nil)

	sP2P.SetPeerEndpoint(rID.DeviceID, "Recv", fmt.Sprintf("127.0.0.1:%d", rP2P.QUICPort()))
	for i := 0; i < 60 && !sP2P.IsConnected(rID.DeviceID); i++ {
		time.Sleep(50 * time.Millisecond)
	}
	if !sP2P.IsConnected(rID.DeviceID) {
		t.Fatal("failed to connect sender -> receiver")
	}

	apiSrv := NewServer(sID, sCfg, sStore, sP2P, syncer.NewManager(sID.DeviceID, sStore, sP2P, nil), sDrop, nil)
	ts := httptest.NewServer(apiSrv.Handler())
	defer ts.Close()

	// Build multipart upload with nested folder path
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	_ = mw.WriteField("peer_id", rID.DeviceID)
	_ = mw.WriteField("rel_path", "Photos/2026/note.txt")
	fw, _ := mw.CreateFormFile("files", "note.txt")
	_, _ = fw.Write([]byte("nested-folder-upload-ok"))
	_ = mw.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/drop/upload", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := ts.Client().Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("upload failed: err=%v resp=%v", err, resp)
	}
	var rec store.DropTransferRecord
	_ = json.NewDecoder(resp.Body).Decode(&rec)
	resp.Body.Close()

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		got, err := rStore.GetDropTransfer(rec.TransferID)
		if err == nil && got.Status == "completed" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	outBytes, err := os.ReadFile(filepath.Join(rCfg.Snapshot().DropInboxDir, "Photos", "2026", "note.txt"))
	if err != nil || string(outBytes) != "nested-folder-upload-ok" {
		log.Printf("inbox dir=%s", rCfg.Snapshot().DropInboxDir)
		t.Fatalf("expected nested folder file on receiver, got %q err=%v", string(outBytes), err)
	}
}

func TestWebAPISetupWizardRateLimitInvitationsAndOfflineUpload(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.LoadOrCreateDaemonConfig(dir)
	if err != nil {
		t.Fatal(err)
	}
	id, err := protocol.LoadOrCreateIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	pMgr := p2p.NewManager(id, cfg, st, p2p.Handlers{})
	sMgr := syncer.NewManager(id.DeviceID, st, pMgr, nil)
	dMgr := drop.NewManager(cfg, st, pMgr, nil)
	apiSrv := NewServer(id, cfg, st, pMgr, sMgr, dMgr, nil)
	h := apiSrv.Handler()

	// 1. Loopback works without password prior to setup
	req := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	req.RemoteAddr = "127.0.0.1:41000"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected loopback status 200 before setup, got %d (%s)", rec.Code, rec.Body.String())
	}

	// 2. Non-loopback API requests return 403 setup_required before password set
	req = httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	req.RemoteAddr = "198.51.100.42:55000"
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "setup_required") {
		t.Fatalf("expected 403 setup_required for remote request, got %d (%s)", rec.Code, rec.Body.String())
	}

	// Check /api/v1/setup/status
	req = httptest.NewRequest(http.MethodGet, "/api/v1/setup/status", nil)
	req.RemoteAddr = "198.51.100.42:55000"
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"needs_setup":true`) {
		t.Fatalf("expected setup status needs_setup=true, got %d (%s)", rec.Code, rec.Body.String())
	}

	// Non-loopback cannot call /api/v1/open-path even after setup; check before & after
	inboxDir := filepath.Join(dir, "inbox")
	setupPayload := fmt.Sprintf(`{"web_user":"admin","web_password":"secret-password-99","node_name":"TrueNASBox","drop_inbox_dir":%q}`, inboxDir)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/setup", strings.NewReader(setupPayload))
	req.RemoteAddr = "198.51.100.42:55000"
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/v1/setup failed: %d (%s)", rec.Code, rec.Body.String())
	}

	// 3. Re-running setup returns 409 Conflict
	req = httptest.NewRequest(http.MethodPost, "/api/v1/setup", strings.NewReader(setupPayload))
	req.RemoteAddr = "127.0.0.1:41000"
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 on repeat setup, got %d", rec.Code)
	}

	// 4. Rate limit Basic Auth (5 failures -> 429 on 6th)
	for i := 0; i < 5; i++ {
		req = httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
		req.RemoteAddr = "198.51.100.42:55000"
		req.SetBasicAuth("admin", "wrong-pass")
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d: expected 401, got %d", i+1, rec.Code)
		}
	}
	req = httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	req.RemoteAddr = "198.51.100.42:55000"
	req.SetBasicAuth("admin", "secret-password-99")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 rate limit on 6th request, got %d", rec.Code)
	}

	// Valid auth from another IP works
	req = httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	req.RemoteAddr = "127.0.0.1:41000"
	req.SetBasicAuth("admin", "secret-password-99")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid auth, got %d (%s)", rec.Code, rec.Body.String())
	}

	// 5. Loopback restriction on /api/v1/open-path
	req = httptest.NewRequest(http.MethodPost, "/api/v1/open-path", strings.NewReader(`{"path":".","reveal":false}`))
	req.RemoteAddr = "198.51.100.99:55000"
	req.SetBasicAuth("admin", "secret-password-99")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-loopback open-path, got %d", rec.Code)
	}

	// 6. Companion apps endpoint
	req = httptest.NewRequest(http.MethodGet, "/api/v1/companion-apps", nil)
	req.RemoteAddr = "127.0.0.1:41000"
	req.SetBasicAuth("admin", "secret-password-99")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"desktop_installed"`) {
		t.Fatalf("unexpected companion-apps response: %d %s", rec.Code, rec.Body.String())
	}

	// 7. Folder invitations CRUD via API
	senderID := "GENS-0000-0000-0000-0000-0000-0000-0002"
	_ = st.UpsertDevice(store.DeviceRecord{DeviceID: senderID, Name: "PeerTwo"})
	_ = st.UpsertFolderInvitation(store.FolderInvitationRecord{
		FolderID:   "shared-docs",
		Label:      "Shared Docs",
		SenderID:   senderID,
		SenderName: "PeerTwo",
	})
	req = httptest.NewRequest(http.MethodGet, "/api/v1/folder-invitations", nil)
	req.RemoteAddr = "127.0.0.1:41000"
	req.SetBasicAuth("admin", "secret-password-99")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"shared-docs"`) {
		t.Fatalf("expected shared-docs invitation, got %d %s", rec.Code, rec.Body.String())
	}
	acceptDir := filepath.Join(dir, "accepted-share")
	req = httptest.NewRequest(http.MethodPost, "/api/v1/folder-invitations/shared-docs/accept",
		strings.NewReader(fmt.Sprintf(`{"sender_id":%q,"local_path":%q,"mode":"send_receive"}`, senderID, acceptDir)))
	req.RemoteAddr = "127.0.0.1:41000"
	req.SetBasicAuth("admin", "secret-password-99")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("accept invitation failed: %d %s", rec.Code, rec.Body.String())
	}
	fRec, err := st.GetFolder("shared-docs")
	if err != nil || fRec.LocalPath != acceptDir {
		t.Fatalf("expected accepted folder in store, got %+v err=%v", fRec, err)
	}

	// 8. Inline prehashed multipart upload to offline peer -> queued_offline
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	_ = mw.WriteField("peer_id", senderID)
	_ = mw.WriteField("rel_path", "Inline/stream.txt")
	fw, _ := mw.CreateFormFile("files", "stream.txt")
	_, _ = fw.Write([]byte("inline-stream-hashed-payload"))
	_ = mw.Close()

	req = httptest.NewRequest(http.MethodPost, "/api/v1/drop/upload", &body)
	req.RemoteAddr = "127.0.0.1:41000"
	req.SetBasicAuth("admin", "secret-password-99")
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload failed: %d %s", rec.Code, rec.Body.String())
	}
	var upRec store.DropTransferRecord
	if err := json.Unmarshal(rec.Body.Bytes(), &upRec); err != nil {
		t.Fatal(err)
	}
	if upRec.Status != "queued_offline" || len(upRec.Files) != 1 || upRec.Files[0].FileHash == "" {
		t.Fatalf("unexpected inline prehashed upload result: %+v", upRec)
	}
}
