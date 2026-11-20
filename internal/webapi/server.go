package webapi

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/WilliamBossard/Gens-Relay/internal/chunker"
	"github.com/WilliamBossard/Gens-Relay/internal/config"
	"github.com/WilliamBossard/Gens-Relay/internal/drop"
	"github.com/WilliamBossard/Gens-Relay/internal/logs"
	"github.com/WilliamBossard/Gens-Relay/internal/p2p"
	"github.com/WilliamBossard/Gens-Relay/internal/store"
	"github.com/WilliamBossard/Gens-Relay/internal/syncer"
	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
	"github.com/WilliamBossard/Gens-Relay/web"
	"github.com/coder/websocket"
)

type wsSub struct {
	conn *websocket.Conn
	send chan []byte
}

type authFailEntry struct {
	count   int
	resetAt time.Time
}

type Server struct {
	id            *protocol.Identity
	cfg           *config.DaemonConfig
	store         *store.Store
	p2p           *p2p.Manager
	syncer        *syncer.Manager
	drop          *drop.Manager
	logs          *logs.RingBuffer
	onShutdown    func()
	shutdownToken string
	rootCtx       context.Context

	authFailMu sync.Mutex
	authFails  map[string]*authFailEntry

	wsMu      sync.Mutex
	wsClients map[*wsSub]struct{}
	eventCh   chan []byte
}

func NewServer(id *protocol.Identity, cfg *config.DaemonConfig, st *store.Store, p2pMgr *p2p.Manager, syncMgr *syncer.Manager, dropMgr *drop.Manager, logBuf *logs.RingBuffer) *Server {
	s := &Server{
		id:        id,
		cfg:       cfg,
		store:     st,
		p2p:       p2pMgr,
		syncer:    syncMgr,
		drop:      dropMgr,
		logs:      logBuf,
		rootCtx:   context.Background(),
		authFails: make(map[string]*authFailEntry),
		wsClients: make(map[*wsSub]struct{}),
		eventCh:   make(chan []byte, 512),
	}
	go s.broadcastLoop()
	return s
}

func (s *Server) SetShutdownFunc(fn func()) {
	s.onShutdown = fn
}

func (s *Server) SetShutdownToken(tok string) {
	s.shutdownToken = tok
}

func (s *Server) SetRootContext(ctx context.Context) {
	s.rootCtx = ctx
}

func (s *Server) broadcastLoop() {
	for payload := range s.eventCh {
		s.wsMu.Lock()
		for sub := range s.wsClients {
			select {
			case sub.send <- payload:
			default:
				// Slow or half-open UI client: drop it immediately without blocking other clients
				delete(s.wsClients, sub)
				close(sub.send)
				_ = sub.conn.CloseNow()
			}
		}
		s.wsMu.Unlock()
	}
}

func (s *Server) BroadcastEvent(event string, data any) {
	payload, err := json.Marshal(map[string]any{
		"event": event,
		"data":  data,
	})
	if err != nil {
		return
	}
	select {
	case s.eventCh <- payload:
	default:
		// Drop event if UI WebSocket queue is saturated so callers (like RingBuffer) never block
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/v1/setup/status", s.handleSetupStatus)
	mux.HandleFunc("POST /api/v1/setup", s.handleSetupComplete)
	mux.HandleFunc("GET /api/v1/status", s.handleStatus)
	mux.HandleFunc("POST /api/v1/config", s.handleUpdateConfig)
	mux.HandleFunc("GET /api/v1/events", s.handleEvents)
	mux.HandleFunc("GET /api/v1/logs", s.handleLogs)
	mux.HandleFunc("GET /api/v1/local-browse", s.handleLocalBrowse)
	mux.HandleFunc("POST /api/v1/open-path", s.handleOpenPath)
	mux.HandleFunc("GET /api/v1/companion-apps", s.handleCompanionApps)
	mux.HandleFunc("POST /api/v1/companion-apps/{app}/launch", s.handleCompanionLaunch)
	mux.HandleFunc("POST /api/v1/companion-apps/{app}/install", s.handleCompanionInstall)

	mux.HandleFunc("GET /api/v1/devices", s.handleListDevices)
	mux.HandleFunc("POST /api/v1/devices", s.handleUpsertDevice)
	mux.HandleFunc("DELETE /api/v1/devices/{id}", s.handleDeleteDevice)
	mux.HandleFunc("GET /api/v1/peers", s.handlePeers)
	mux.HandleFunc("GET /api/v1/peers/nearby", s.handleNearbyPeers)

	mux.HandleFunc("POST /api/v1/pair/create", s.handlePairCreate)
	mux.HandleFunc("POST /api/v1/pair/redeem", s.handlePairRedeem)
	mux.HandleFunc("GET /api/v1/pair/requests", s.handleListPairRequests)
	mux.HandleFunc("POST /api/v1/pair/request", s.handleSendPairRequest)
	mux.HandleFunc("POST /api/v1/pair/requests/{id}/accept", s.handleAcceptPairRequest)
	mux.HandleFunc("POST /api/v1/pair/requests/{id}/reject", s.handleRejectPairRequest)

	mux.HandleFunc("GET /api/v1/folders", s.handleListFolders)
	mux.HandleFunc("POST /api/v1/folders", s.handleUpsertFolder)
	mux.HandleFunc("DELETE /api/v1/folders/{id}", s.handleDeleteFolder)
	mux.HandleFunc("GET /api/v1/folders/status", s.handleFolderStatuses)
	mux.HandleFunc("POST /api/v1/folders/{id}/rescan", s.handleFolderRescan)
	mux.HandleFunc("POST /api/v1/folders/{id}/override", s.handleFolderOverride)
	mux.HandleFunc("POST /api/v1/folders/{id}/revert", s.handleFolderRevert)
	mux.HandleFunc("GET /api/v1/folders/{id}/conflicts", s.handleListConflicts)
	mux.HandleFunc("POST /api/v1/folders/{id}/conflicts/resolve", s.handleResolveConflict)
	mux.HandleFunc("GET /api/v1/folders/{id}/versions", s.handleListVersions)
	mux.HandleFunc("POST /api/v1/folders/{id}/versions/restore", s.handleRestoreVersion)
	mux.HandleFunc("GET /api/v1/folder-invitations", s.handleListFolderInvitations)
	mux.HandleFunc("POST /api/v1/folder-invitations/{id}/accept", s.handleAcceptFolderInvitation)
	mux.HandleFunc("POST /api/v1/folder-invitations/{id}/reject", s.handleRejectFolderInvitation)

	mux.HandleFunc("GET /api/v1/drop", s.handleDropList)
	mux.HandleFunc("POST /api/v1/drop/send", s.handleDropSend)
	mux.HandleFunc("POST /api/v1/drop/send-text", s.handleDropSendText)
	mux.HandleFunc("POST /api/v1/drop/upload", s.handleDropUpload)
	mux.HandleFunc("POST /api/v1/drop/{id}/respond", s.handleDropRespond)
	mux.HandleFunc("POST /api/v1/drop/{id}/pause", s.handleDropPause)
	mux.HandleFunc("POST /api/v1/drop/{id}/resume", s.handleDropResume)
	mux.HandleFunc("POST /api/v1/drop/{id}/cancel", s.handleDropCancel)
	mux.HandleFunc("DELETE /api/v1/drop/{id}", s.handleDropDelete)
	mux.HandleFunc("POST /api/v1/shutdown", s.handleShutdown)
	mux.HandleFunc("GET /metrics", s.handleMetrics)

	distSub, err := fs.Sub(web.DistFS, "dist")
	if err == nil {
		fileServer := http.FileServer(http.FS(distSub))
		mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/api/") {
				writeError(w, http.StatusNotFound, "unknown api endpoint")
				return
			}
			clean := strings.TrimPrefix(r.URL.Path, "/")
			if clean == "" {
				clean = "index.html"
			}
			if _, statErr := fs.Stat(distSub, clean); statErr != nil {
				r2 := r.Clone(r.Context())
				r2.URL.Path = "/"
				fileServer.ServeHTTP(w, r2)
				return
			}
			fileServer.ServeHTTP(w, r)
		}))
	}

	return s.authMiddleware(s.originMiddleware(mux))
}

func isAllowedOrigin(origin, host string) bool {
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	if u.Scheme == "tauri" || u.Hostname() == "tauri.localhost" {
		return true
	}
	reqHost, _, err := net.SplitHostPort(host)
	if err != nil {
		reqHost = host
	}
	origHost := u.Hostname()
	if strings.EqualFold(origHost, reqHost) {
		return true
	}
	isLoop := func(h string) bool {
		return h == "localhost" || h == "127.0.0.1" || h == "::1"
	}
	return isLoop(origHost) && isLoop(reqHost)
}

func (s *Server) originMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodDelete || r.Method == http.MethodPatch {
			if origin := r.Header.Get("Origin"); origin != "" && !isAllowedOrigin(origin, r.Host) {
				writeError(w, http.StatusForbidden, "cross-origin state-changing request forbidden")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func remoteIP(r *http.Request) net.IP {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return net.ParseIP(host)
}

func isLoopbackRequest(r *http.Request) bool {
	ip := remoteIP(r)
	return ip != nil && ip.IsLoopback()
}

func (s *Server) allowAuth(ip string) bool {
	if ip == "" {
		return true
	}
	now := time.Now()
	s.authFailMu.Lock()
	defer s.authFailMu.Unlock()
	ent, ok := s.authFails[ip]
	if !ok || now.After(ent.resetAt) {
		return true
	}
	return ent.count < 5
}

func (s *Server) recordAuthResult(ip string, ok bool) {
	if ip == "" {
		return
	}
	now := time.Now()
	s.authFailMu.Lock()
	defer s.authFailMu.Unlock()
	if len(s.authFails) > 1000 {
		for k, v := range s.authFails {
			if now.After(v.resetAt) {
				delete(s.authFails, k)
			}
		}
	}
	if ok {
		delete(s.authFails, ip)
		return
	}
	ent, exists := s.authFails[ip]
	if !exists || now.After(ent.resetAt) {
		s.authFails[ip] = &authFailEntry{count: 1, resetAt: now.Add(1 * time.Minute)}
		return
	}
	ent.count++
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/shutdown" && s.shutdownToken != "" {
			got := r.Header.Get("X-Gens-Shutdown-Token")
			if subtle.ConstantTimeCompare([]byte(got), []byte(s.shutdownToken)) == 1 {
				next.ServeHTTP(w, r)
				return
			}
		}
		if r.URL.Path == "/api/v1/setup/status" || r.URL.Path == "/api/v1/setup" {
			next.ServeHTTP(w, r)
			return
		}

		snap := s.cfg.Snapshot()
		if snap.WebPassHash == "" {
			// Local apps & localhost browser run passwordless out of the box.
			// Non-loopback clients (Docker/TrueNAS/LAN remote UI) must complete First-Run Setup first.
			if !strings.HasPrefix(r.URL.Path, "/api/") || isLoopbackRequest(r) {
				next.ServeHTTP(w, r)
				return
			}
			writeError(w, http.StatusForbidden, "setup_required")
			return
		}

		ipStr := ""
		if ip := remoteIP(r); ip != nil {
			ipStr = ip.String()
		}
		if !s.allowAuth(ipStr) {
			http.Error(w, "Too many failed authentication attempts", http.StatusTooManyRequests)
			return
		}

		user, pass, ok := r.BasicAuth()
		if !ok || !s.cfg.VerifyWebAuth(user, pass) {
			s.recordAuthResult(ipStr, false)
			w.Header().Set("WWW-Authenticate", `Basic realm="Gens-Relay"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		s.recordAuthResult(ipStr, true)
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleSetupStatus(w http.ResponseWriter, r *http.Request) {
	snap := s.cfg.Snapshot()
	isLoop := isLoopbackRequest(r)
	needsSetup := snap.WebPassHash == ""
	resp := map[string]any{
		"needs_setup": needsSetup,
		"is_loopback": isLoop,
	}
	if needsSetup {
		resp["web_user"] = snap.WebUser
		resp["node_name"] = snap.NodeName
		resp["drop_inbox_dir"] = snap.DropInboxDir
		resp["relay_url"] = snap.RelayURL
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleSetupComplete(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Snapshot().WebPassHash != "" {
		writeError(w, http.StatusConflict, "setup already completed")
		return
	}
	var body struct {
		WebUser      string   `json:"web_user"`
		WebPassword  string   `json:"web_password"`
		NodeName     string   `json:"node_name"`
		RelayURL     string   `json:"relay_url"`
		RelayURLs    []string `json:"relay_urls"`
		DropInboxDir string   `json:"drop_inbox_dir"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(body.WebPassword) < 4 {
		writeError(w, http.StatusBadRequest, "mot de passe trop court (minimum 4 caractères)")
		return
	}
	if err := s.cfg.Update(func(c *config.DaemonConfig) {
		if u := strings.TrimSpace(body.WebUser); u != "" {
			c.WebUser = u
		}
		if n := strings.TrimSpace(body.NodeName); n != "" {
			c.NodeName = n
		}
		if ru := strings.TrimSpace(body.RelayURL); ru != "" {
			c.RelayURL = normalizeRelayURL(ru)
		}
		if len(body.RelayURLs) > 0 {
			c.RelayURLs = normalizeRelayURLList(body.RelayURLs)
		}
		if d := strings.TrimSpace(body.DropInboxDir); d != "" {
			c.DropInboxDir = d
			_ = os.MkdirAll(d, 0755)
		}
	}); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.cfg.SetWebPassword(body.WebPassword); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.p2p.RestartRelay()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); !isAllowedOrigin(origin, r.Host) {
		http.Error(w, "Forbidden origin", http.StatusForbidden)
		return
	}
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	sub := &wsSub{
		conn: ws,
		send: make(chan []byte, 64),
	}
	s.wsMu.Lock()
	s.wsClients[sub] = struct{}{}
	s.wsMu.Unlock()

	defer func() {
		s.wsMu.Lock()
		if _, exists := s.wsClients[sub]; exists {
			delete(s.wsClients, sub)
			close(sub.send)
		}
		s.wsMu.Unlock()
		_ = ws.CloseNow()
	}()

	// Writer loop per UI client
	go func() {
		for payload := range sub.send {
			wCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			err := ws.Write(wCtx, websocket.MessageText, payload)
			cancel()
			if err != nil {
				_ = ws.CloseNow()
				return
			}
		}
	}()

	for {
		if _, _, err := ws.Read(r.Context()); err != nil {
			return
		}
	}
}

func normalizeRelayURL(raw string) string {
	u := strings.TrimSpace(raw)
	if u != "" && !strings.Contains(u, "://") {
		u = "ws://" + u
	}
	return u
}

func normalizeRelayURLList(list []string) []string {
	var out []string
	seen := make(map[string]struct{}, len(list))
	for _, raw := range list {
		u := normalizeRelayURL(raw)
		if u == "" {
			continue
		}
		if _, dup := seen[u]; !dup {
			seen[u] = struct{}{}
			out = append(out, u)
		}
	}
	return out
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	snap := s.cfg.Snapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"device_id":            s.id.DeviceID,
		"node_name":            snap.NodeName,
		"quic_port":            s.p2p.QUICPort(),
		"lan_addrs":            p2p.LocalLANAddrs(s.p2p.QUICPort()),
		"relay_url":            snap.RelayURL,
		"relay_urls":           snap.RelayURLs,
		"active_relay_url":     s.p2p.ActiveRelayURL(),
		"relay_transport_mode": s.p2p.RelayTransportMode(),
		"relay_connected":      s.p2p.RelayConnected(),
		"has_auth_token":       snap.AuthToken != "",
		"has_web_password":     snap.WebPassHash != "",
		"drop_inbox_dir":       snap.DropInboxDir,
		"max_upload_kbps":      snap.MaxUploadKBps,
		"max_download_kbps":    snap.MaxDownloadKBps,
		"enable_upnp":          snap.EnableUPnP,
		"enable_compression":   snap.EnableCompression,
		"web_listen":           snap.WebListen,
		"web_tls":              snap.WebTLS,
		"web_user":             snap.WebUser,
		"is_loopback":          isLoopbackRequest(r),
	})
}

func (s *Server) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	var body struct {
		NodeName          string    `json:"node_name"`
		RelayURL          *string   `json:"relay_url"`
		RelayURLs         *[]string `json:"relay_urls"`
		AuthToken         string    `json:"auth_token"`
		ClearAuthToken    bool      `json:"clear_auth_token"`
		DropInboxDir      string    `json:"drop_inbox_dir"`
		MaxUploadKBps     *int      `json:"max_upload_kbps"`
		MaxDownloadKBps   *int      `json:"max_download_kbps"`
		EnableUPnP        *bool     `json:"enable_upnp"`
		EnableCompression *bool     `json:"enable_compression"`
		WebListen         string    `json:"web_listen"`
		WebTLS            *bool     `json:"web_tls"`
		WebUser           *string   `json:"web_user"`
		WebPassword       string    `json:"web_password"`
		ClearWebPassword  bool      `json:"clear_web_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	prevSnap := s.cfg.Snapshot()
	err := s.cfg.Update(func(c *config.DaemonConfig) {
		if body.NodeName != "" {
			c.NodeName = body.NodeName
		}
		if body.RelayURL != nil {
			c.RelayURL = normalizeRelayURL(*body.RelayURL)
		}
		if body.RelayURLs != nil {
			c.RelayURLs = normalizeRelayURLList(*body.RelayURLs)
		}
		if body.ClearAuthToken {
			c.AuthToken = ""
		} else if body.AuthToken != "" {
			c.AuthToken = body.AuthToken
		}
		if body.DropInboxDir != "" {
			c.DropInboxDir = body.DropInboxDir
			_ = os.MkdirAll(c.DropInboxDir, 0755)
		}
		if body.MaxUploadKBps != nil && *body.MaxUploadKBps >= 0 {
			c.MaxUploadKBps = *body.MaxUploadKBps
		}
		if body.MaxDownloadKBps != nil && *body.MaxDownloadKBps >= 0 {
			c.MaxDownloadKBps = *body.MaxDownloadKBps
		}
		if body.EnableUPnP != nil {
			c.EnableUPnP = *body.EnableUPnP
		}
		if body.EnableCompression != nil {
			c.EnableCompression = *body.EnableCompression
		}
		if body.WebListen != "" {
			c.WebListen = body.WebListen
		}
		if body.WebTLS != nil {
			c.WebTLS = *body.WebTLS
		}
		if body.WebUser != nil {
			c.WebUser = *body.WebUser
		}
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if body.ClearWebPassword {
		if err := s.cfg.SetWebPassword(""); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	} else if body.WebPassword != "" {
		if err := s.cfg.SetWebPassword(body.WebPassword); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	newSnap := s.cfg.Snapshot()
	needsRestart := newSnap.WebListen != prevSnap.WebListen || newSnap.WebTLS != prevSnap.WebTLS || newSnap.EnableUPnP != prevSnap.EnableUPnP
	s.p2p.RefreshRateLimiters()
	s.p2p.RestartRelay()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "needs_restart": needsRestart})
}

func (s *Server) handleLocalBrowse(w http.ResponseWriter, r *http.Request) {
	type entry struct {
		Name  string `json:"name"`
		Path  string `json:"path"`
		IsDir bool   `json:"is_dir"`
		Size  int64  `json:"size"`
	}

	rawPath := strings.TrimSpace(r.URL.Query().Get("path"))
	if runtime.GOOS == "windows" && rawPath == "__drives__" {
		var drives []entry
		for c := 'A'; c <= 'Z'; c++ {
			root := fmt.Sprintf("%c:\\", c)
			if _, err := os.Stat(root); err == nil {
				drives = append(drives, entry{
					Name:  root,
					Path:  root,
					IsDir: true,
				})
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"path":    "__drives__",
			"parent":  "__drives__",
			"entries": drives,
		})
		return
	}

	if rawPath == "" {
		if home, err := os.UserHomeDir(); err == nil {
			rawPath = home
		} else {
			rawPath = "."
		}
	}
	absPath, err := filepath.Abs(rawPath)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	dirEntries, err := os.ReadDir(absPath)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var out []entry
	for _, d := range dirEntries {
		info, err := d.Info()
		if err != nil {
			continue
		}
		out = append(out, entry{
			Name:  d.Name(),
			Path:  filepath.Join(absPath, d.Name()),
			IsDir: d.IsDir(),
			Size:  info.Size(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IsDir != out[j].IsDir {
			return out[i].IsDir
		}
		return strings.ToLower(out[i].Name) < strings.ToLower(out[j].Name)
	})

	parent := filepath.Dir(absPath)
	if runtime.GOOS == "windows" && parent == absPath {
		parent = "__drives__"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"path":    absPath,
		"parent":  parent,
		"entries": out,
	})
}

func (s *Server) handleListDevices(w http.ResponseWriter, _ *http.Request) {
	list, err := s.store.ListDevices()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleUpsertDevice(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DeviceID       string `json:"device_id"`
		Name           string `json:"name"`
		AutoAcceptDrop bool   `json:"auto_accept_drop"`
		Endpoint       string `json:"endpoint"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	id := protocol.NormalizeDeviceID(body.DeviceID)
	if id == "" || id == s.id.DeviceID {
		writeError(w, http.StatusBadRequest, "invalid device_id")
		return
	}
	name := strings.TrimSpace(body.Name)
	if name == "" {
		name = id
	}
	if err := s.store.UpsertDevice(store.DeviceRecord{
		DeviceID:       id,
		Name:           name,
		AutoAcceptDrop: body.AutoAcceptDrop,
	}); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if body.Endpoint != "" {
		s.p2p.SetPeerEndpoint(id, name, strings.TrimSpace(body.Endpoint))
	}
	_ = s.p2p.QueryRelayPeers(r.Context())
	s.BroadcastEvent("peers_updated", s.p2p.PeerStatuses())
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDeleteDevice(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.DeleteDevice(id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.p2p.DisconnectPeer(id)
	s.syncer.ReloadFolders()
	s.BroadcastEvent("peers_updated", s.p2p.PeerStatuses())
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handlePeers(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.p2p.PeerStatuses())
}

func (s *Server) handleNearbyPeers(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.p2p.NearbyPeers())
}

func (s *Server) handleListPairRequests(w http.ResponseWriter, _ *http.Request) {
	list, err := s.store.ListPairRequests()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleSendPairRequest(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DeviceID string `json:"device_id"`
		Name     string `json:"name"`
		Endpoint string `json:"endpoint"`
		Message  string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.p2p.SendPairRequest(r.Context(), body.DeviceID, body.Name, body.Endpoint, body.Message); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleAcceptPairRequest(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if err := s.p2p.AcceptPairRequest(r.Context(), r.PathValue("id"), body.Name); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleRejectPairRequest(w http.ResponseWriter, r *http.Request) {
	if err := s.p2p.RejectPairRequest(r.Context(), r.PathValue("id")); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handlePairCreate(w http.ResponseWriter, r *http.Request) {
	code, exp, err := s.p2p.CreatePairCode(r.Context())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pair_code":      code,
		"expires_in_sec": exp,
	})
}

func (s *Server) handlePairRedeem(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PairCode string `json:"pair_code"`
		Name     string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	hint, err := s.p2p.RedeemPairCode(r.Context(), body.PairCode)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	name := strings.TrimSpace(body.Name)
	if name == "" {
		name = hint.Name
	}
	if name == "" {
		name = hint.DeviceID
	}
	if err := s.store.UpsertDevice(store.DeviceRecord{
		DeviceID: hint.DeviceID,
		Name:     name,
	}); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for _, a := range hint.LANAddrs {
		s.p2p.SetPeerEndpoint(hint.DeviceID, name, a)
	}
	if hint.ObservedAddr != "" {
		s.p2p.SetPeerEndpoint(hint.DeviceID, name, hint.ObservedAddr)
	}
	_ = s.p2p.QueryRelayPeers(r.Context())
	writeJSON(w, http.StatusOK, hint)
}

func (s *Server) handleListFolders(w http.ResponseWriter, _ *http.Request) {
	list, err := s.store.ListFolders()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleUpsertFolder(w http.ResponseWriter, r *http.Request) {
	var body store.FolderRecord
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	body.FolderID = strings.TrimSpace(body.FolderID)
	body.LocalPath = strings.TrimSpace(body.LocalPath)
	if body.FolderID == "" || body.LocalPath == "" {
		writeError(w, http.StatusBadRequest, "folder_id and local_path are required")
		return
	}
	if body.Label == "" {
		body.Label = body.FolderID
	}
	if err := s.store.UpsertFolder(body); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.syncer.ReloadFolders()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDeleteFolder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.DeleteFolder(id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.syncer.ReloadFolders()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleFolderStatuses(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.syncer.Statuses())
}

func (s *Server) handleFolderRescan(w http.ResponseWriter, r *http.Request) {
	s.syncer.TriggerScan(r.PathValue("id"))
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleFolderOverride(w http.ResponseWriter, r *http.Request) {
	if err := s.syncer.OverrideChanges(r.PathValue("id")); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleFolderRevert(w http.ResponseWriter, r *http.Request) {
	if err := s.syncer.RevertLocalChanges(s.rootCtx, r.PathValue("id")); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleLogs(w http.ResponseWriter, _ *http.Request) {
	if s.logs == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	writeJSON(w, http.StatusOK, s.logs.List())
}

func (s *Server) handleListConflicts(w http.ResponseWriter, r *http.Request) {
	list, err := s.syncer.ListConflicts(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if list == nil {
		list = []syncer.ConflictItem{}
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleResolveConflict(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RelPath string `json:"rel_path"`
		Action  string `json:"action"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.syncer.ResolveConflict(r.PathValue("id"), body.RelPath, body.Action); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleListVersions(w http.ResponseWriter, r *http.Request) {
	list, err := s.syncer.ListVersions(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if list == nil {
		list = []syncer.VersionItem{}
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleRestoreVersion(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ArchivePath string `json:"archive_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.syncer.RestoreVersion(r.PathValue("id"), body.ArchivePath); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleListFolderInvitations(w http.ResponseWriter, _ *http.Request) {
	list, err := s.store.ListFolderInvitations()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if list == nil {
		list = []store.FolderInvitationRecord{}
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleAcceptFolderInvitation(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SenderID  string `json:"sender_id"`
		LocalPath string `json:"local_path"`
		Mode      string `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var mode protocol.FolderMode
	switch body.Mode {
	case "send_only", "sendonly":
		mode = protocol.ModeSendOnly
	case "receive_only", "receiveonly":
		mode = protocol.ModeReceiveOnly
	default:
		mode = protocol.ModeSendReceive
	}
	if err := s.syncer.AcceptInvitation(r.PathValue("id"), body.SenderID, body.LocalPath, mode); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleRejectFolderInvitation(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SenderID string `json:"sender_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.syncer.RejectInvitation(r.PathValue("id"), body.SenderID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleOpenPath(w http.ResponseWriter, r *http.Request) {
	if !isLoopbackRequest(r) {
		writeError(w, http.StatusForbidden, "open-path allowed only from loopback")
		return
	}
	var body struct {
		Path   string `json:"path"`
		Reveal bool   `json:"reveal"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	cleanPath := filepath.Clean(strings.TrimSpace(body.Path))
	if cleanPath == "" || cleanPath == "." {
		writeError(w, http.StatusBadRequest, "path is required")
		return
	}
	info, err := os.Stat(cleanPath)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("chemin introuvable : %v", err))
		return
	}

	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		if body.Reveal && !info.IsDir() {
			cmd = exec.Command("explorer.exe", "/select,"+cleanPath)
		} else if !info.IsDir() {
			cmd = exec.Command("explorer.exe", filepath.Dir(cleanPath))
		} else {
			cmd = exec.Command("explorer.exe", cleanPath)
		}
	case "darwin":
		if body.Reveal && !info.IsDir() {
			cmd = exec.Command("open", "-R", cleanPath)
		} else {
			cmd = exec.Command("open", cleanPath)
		}
	default:
		target := cleanPath
		if !info.IsDir() {
			target = filepath.Dir(cleanPath)
		}
		cmd = exec.Command("xdg-open", target)
	}
	if err := cmd.Start(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type companionPaths struct {
	DesktopPath   string
	DropPath      string
	InstallerPath string
}

func detectCompanionPaths() companionPaths {
	var candidates []string
	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		candidates = append(candidates, exeDir, filepath.Dir(exeDir))
	}
	if runtime.GOOS == "windows" {
		if pf := os.Getenv("ProgramFiles"); pf != "" {
			candidates = append(candidates, filepath.Join(pf, "Gens-Relay"))
		}
		if local := os.Getenv("LOCALAPPDATA"); local != "" {
			candidates = append(candidates, filepath.Join(local, "Programs", "Gens-Relay"))
		}
	}

	desktopName := "gens-relay-desktop"
	dropName := "gens-drop-desktop"
	if runtime.GOOS == "windows" {
		desktopName += ".exe"
		dropName += ".exe"
	}

	findFile := func(relNames ...string) string {
		for _, base := range candidates {
			for _, rel := range relNames {
				p := filepath.Join(base, rel)
				if info, err := os.Stat(p); err == nil && !info.IsDir() {
					return p
				}
			}
		}
		return ""
	}

	return companionPaths{
		DesktopPath:   findFile(desktopName),
		DropPath:      findFile(dropName),
		InstallerPath: findFile(filepath.Join("setup", "gens-relay-setup.exe"), "gens-relay-setup.exe"),
	}
}

func (s *Server) handleCompanionApps(w http.ResponseWriter, r *http.Request) {
	paths := detectCompanionPaths()
	writeJSON(w, http.StatusOK, map[string]any{
		"desktop_installed":   paths.DesktopPath != "",
		"drop_installed":      paths.DropPath != "",
		"installer_available": paths.InstallerPath != "",
		"desktop_path":        paths.DesktopPath,
		"drop_path":           paths.DropPath,
		"installer_path":      paths.InstallerPath,
		"is_loopback":         isLoopbackRequest(r),
	})
}

func (s *Server) handleCompanionLaunch(w http.ResponseWriter, r *http.Request) {
	if !isLoopbackRequest(r) {
		writeError(w, http.StatusForbidden, "companion launch allowed only from loopback")
		return
	}
	paths := detectCompanionPaths()
	var target string
	switch r.PathValue("app") {
	case "desktop":
		target = paths.DesktopPath
	case "drop":
		target = paths.DropPath
	default:
		writeError(w, http.StatusBadRequest, "unknown companion app")
		return
	}
	if target == "" {
		writeError(w, http.StatusNotFound, "application compagnon non installée sur ce PC")
		return
	}
	if err := exec.Command(target).Start(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleCompanionInstall(w http.ResponseWriter, r *http.Request) {
	if !isLoopbackRequest(r) {
		writeError(w, http.StatusForbidden, "companion install allowed only from loopback")
		return
	}
	paths := detectCompanionPaths()
	if paths.InstallerPath == "" {
		writeError(w, http.StatusNotFound, "installateur introuvable localement — télécharge gens-relay-setup.exe depuis les Releases GitHub")
		return
	}
	if err := exec.Command(paths.InstallerPath).Start(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDropList(w http.ResponseWriter, _ *http.Request) {
	list, err := s.drop.ListTransfers()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleDropSend(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PeerID string   `json:"peer_id"`
		Paths  []string `json:"paths"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	rec, err := s.drop.SendPaths(s.rootCtx, body.PeerID, body.Paths)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) handleDropSendText(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PeerID string `json:"peer_id"`
		Text   string `json:"text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	rec, err := s.drop.SendText(s.rootCtx, body.PeerID, body.Text)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func safeUploadJoin(root, rel string) (string, error) {
	cleanRel := filepath.Clean(filepath.FromSlash(rel))
	if cleanRel == "." || filepath.IsAbs(cleanRel) || strings.HasPrefix(cleanRel, "..") {
		return "", fmt.Errorf("unsafe path: %s", rel)
	}
	full := filepath.Join(root, cleanRel)
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	absFull, err := filepath.Abs(full)
	if err != nil {
		return "", err
	}
	if absFull != absRoot && !strings.HasPrefix(absFull, absRoot+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes staging dir: %s", rel)
	}
	return full, nil
}

// streamingBlockHasher computes BLAKE3 block hashes and Merkle FileHash on the fly
// during io.Copy so browser multipart uploads never require a second disk read pass.
type streamingBlockHasher struct {
	blockSize int
	bufPtr    *[]byte
	filled    int
	offset    int64
	blocks    []protocol.BlockInfo
}

func newStreamingBlockHasher(blockSize int) *streamingBlockHasher {
	if blockSize <= 0 {
		blockSize = chunker.DefaultBlockSize
	}
	return &streamingBlockHasher{
		blockSize: blockSize,
		bufPtr:    chunker.GetBufferSize(blockSize),
	}
}

func (h *streamingBlockHasher) Write(p []byte) (int, error) {
	total := len(p)
	buf := (*h.bufPtr)[:h.blockSize]
	for len(p) > 0 {
		n := copy(buf[h.filled:], p)
		h.filled += n
		p = p[n:]
		if h.filled == h.blockSize {
			sum := chunker.HashBytes(buf[:h.blockSize])
			h.blocks = append(h.blocks, protocol.BlockInfo{
				Offset: h.offset,
				Size:   h.blockSize,
				Hash:   sum,
			})
			h.offset += int64(h.blockSize)
			h.filled = 0
		}
	}
	return total, nil
}

func (h *streamingBlockHasher) Finish() ([]protocol.BlockInfo, string, int64) {
	if h.filled > 0 {
		buf := (*h.bufPtr)[:h.filled]
		h.blocks = append(h.blocks, protocol.BlockInfo{
			Offset: h.offset,
			Size:   h.filled,
			Hash:   chunker.HashBytes(buf),
		})
		h.offset += int64(h.filled)
		h.filled = 0
	}
	if h.bufPtr != nil {
		chunker.PutBufferSized(h.bufPtr)
		h.bufPtr = nil
	}
	return h.blocks, chunker.ComputeMerkleFileHash(h.blocks), h.offset
}

func (s *Server) handleDropUpload(w http.ResponseWriter, r *http.Request) {
	mr, err := r.MultipartReader()
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid multipart upload: "+err.Error())
		return
	}

	var rnd [6]byte
	_, _ = rand.Read(rnd[:])
	stageBase := filepath.Join(s.cfg.DataDir(), "drop_staging")
	if inbox := s.cfg.Snapshot().DropInboxDir; inbox != "" {
		stageBase = filepath.Join(inbox, ".gens.staging")
	}
	stagingDir := filepath.Join(stageBase, hex.EncodeToString(rnd[:]))
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	cleanupOnErr := true
	defer func() {
		if cleanupOnErr {
			_ = os.RemoveAll(stagingDir)
		}
	}()

	var peerID string
	var nextRelPath string
	var stagedFiles []drop.StagedFile

	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if part.FormName() == "peer_id" {
			b, _ := io.ReadAll(io.LimitReader(part, 256))
			peerID = strings.TrimSpace(string(b))
			_ = part.Close()
			continue
		}
		if part.FormName() == "rel_path" {
			b, _ := io.ReadAll(io.LimitReader(part, 1024))
			nextRelPath = strings.TrimSpace(string(b))
			_ = part.Close()
			continue
		}
		if part.FormName() == "files" && part.FileName() != "" {
			rel := nextRelPath
			nextRelPath = ""
			if rel == "" {
				rel = part.FileName()
			}
			rel = filepath.ToSlash(filepath.Clean(strings.ReplaceAll(rel, "\\", "/")))
			rel = strings.TrimPrefix(rel, "/")
			dstPath, jErr := safeUploadJoin(stagingDir, rel)
			if jErr != nil {
				_ = part.Close()
				continue
			}

			if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
				_ = part.Close()
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			f, err := os.Create(dstPath)
			if err != nil {
				_ = part.Close()
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			hasher := newStreamingBlockHasher(chunker.DefaultBlockSize)
			if _, err := io.Copy(io.MultiWriter(f, hasher), part); err != nil {
				_, _, _ = hasher.Finish()
				_ = f.Close()
				_ = part.Close()
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			blocks, fileHash, sz := hasher.Finish()
			_ = f.Close()
			_ = part.Close()

			stagedFiles = append(stagedFiles, drop.StagedFile{
				RelPath:   rel,
				LocalPath: dstPath,
				Size:      sz,
				BlockSize: chunker.DefaultBlockSize,
				Blocks:    blocks,
				FileHash:  fileHash,
			})
			continue
		}
		_ = part.Close()
	}

	if peerID == "" || len(stagedFiles) == 0 {
		writeError(w, http.StatusBadRequest, "peer_id et au moins un fichier sont requis")
		return
	}

	rec, err := s.drop.SendPrehashedStagedPaths(s.rootCtx, peerID, stagingDir, stagedFiles)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	cleanupOnErr = false
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) handleDropRespond(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Accept  bool   `json:"accept"`
		DestDir string `json:"dest_dir"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.drop.RespondOffer(r.PathValue("id"), body.Accept, body.DestDir); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDropPause(w http.ResponseWriter, r *http.Request) {
	if err := s.drop.PauseTransfer(r.PathValue("id")); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDropResume(w http.ResponseWriter, r *http.Request) {
	if err := s.drop.ResumeTransfer(s.rootCtx, r.PathValue("id")); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDropCancel(w http.ResponseWriter, r *http.Request) {
	if err := s.drop.CancelTransfer(r.PathValue("id")); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDropDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.drop.DeleteTransfer(r.PathValue("id")); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleShutdown(w http.ResponseWriter, r *http.Request) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		writeError(w, http.StatusForbidden, "shutdown allowed only from loopback")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	if s.onShutdown != nil {
		go func() {
			time.Sleep(50 * time.Millisecond)
			s.onShutdown()
		}()
	}
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	peers := s.p2p.ConnectedPeerIDs()
	folders := s.syncer.Statuses()
	syncRx, syncTx := s.syncer.TransferCounters()
	dropRx, dropTx := s.drop.TransferCounters()
	var localFiles int
	var localBytes int64
	var needBytes int64
	for _, f := range folders {
		localFiles += f.LocalFiles
		localBytes += f.LocalBytes
		needBytes += f.NeedBytes
	}
	relayUp := 0
	if s.p2p.RelayConnected() {
		relayUp = 1
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w,
		"gens_daemon_connected_peers %d\ngens_daemon_relay_connected %d\ngens_daemon_folders %d\ngens_daemon_local_files %d\ngens_daemon_local_bytes %d\ngens_daemon_need_bytes %d\ngens_daemon_sync_rx_bytes_total %d\ngens_daemon_sync_tx_bytes_total %d\ngens_daemon_drop_rx_bytes_total %d\ngens_daemon_drop_tx_bytes_total %d\n",
		len(peers), relayUp, len(folders), localFiles, localBytes, needBytes, syncRx, syncTx, dropRx, dropTx,
	)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": msg})
}
