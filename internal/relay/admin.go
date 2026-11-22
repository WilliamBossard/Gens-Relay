package relay

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/WilliamBossard/Gens-Relay/internal/logs"
	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
	relayweb "github.com/WilliamBossard/Gens-Relay/relay-web"
	"github.com/coder/websocket"
)

type adminWsSub struct {
	conn *websocket.Conn
	send chan []byte
}

func (s *Server) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/setup/status", s.handleAdminSetupStatus)
	mux.HandleFunc("POST /api/v1/setup", s.handleAdminSetupComplete)
	mux.HandleFunc("GET /api/v1/status", s.handleAdminStatus)
	mux.HandleFunc("GET /api/v1/clients", s.handleAdminClients)
	mux.HandleFunc("POST /api/v1/clients/{id}/kick", s.handleAdminKick)
	mux.HandleFunc("GET /api/v1/bans", s.handleAdminListBans)
	mux.HandleFunc("POST /api/v1/bans", s.handleAdminAddBan)
	mux.HandleFunc("DELETE /api/v1/bans/{id}", s.handleAdminRemoveBan)
	mux.HandleFunc("GET /api/v1/pair-codes", s.handleAdminPairCodes)
	mux.HandleFunc("DELETE /api/v1/pair-codes/{code}", s.handleAdminRevokeCode)
	mux.HandleFunc("GET /api/v1/pending", s.handleAdminPending)
	mux.HandleFunc("GET /api/v1/config", s.handleAdminGetConfig)
	mux.HandleFunc("POST /api/v1/config", s.handleAdminUpdateConfig)
	mux.HandleFunc("GET /api/v1/logs", s.handleAdminLogs)
	mux.HandleFunc("GET /api/v1/events", s.handleAdminEvents)
	mux.HandleFunc("POST /api/v1/shutdown", s.handleAdminShutdown)

	distSub, err := fs.Sub(relayweb.DistFS, "dist")
	if err == nil {
		fileServer := http.FileServer(http.FS(distSub))
		mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/api/") {
				writeAdminError(w, http.StatusNotFound, "unknown admin api endpoint")
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

	return s.adminSecurityHeaders(s.adminAuthMiddleware(s.adminOriginMiddleware(mux)))
}

func (s *Server) adminSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

func remoteHostIP(r *http.Request) net.IP {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return net.ParseIP(host)
}

func isAdminLoopback(r *http.Request) bool {
	ip := remoteHostIP(r)
	return ip != nil && ip.IsLoopback()
}

func isAdminLocalNet(r *http.Request) bool {
	ip := remoteHostIP(r)
	return ip != nil && (ip.IsLoopback() || ip.IsPrivate())
}

func isAllowedAdminOrigin(origin, host string) bool {
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

func (s *Server) adminOriginMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodDelete || r.Method == http.MethodPatch {
			if origin := r.Header.Get("Origin"); origin != "" {
				if !isAllowedAdminOrigin(origin, r.Host) {
					writeAdminError(w, http.StatusForbidden, "cross-origin request forbidden")
					return
				}
			} else if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
				// Browser request with no Origin but declared cross-site: reject (CSRF defense-in-depth).
				writeAdminError(w, http.StatusForbidden, "cross-origin request forbidden")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) adminAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/shutdown" && s.opts.ShutdownToken != "" {
			got := r.Header.Get("X-Gens-Shutdown-Token")
			if subtle.ConstantTimeCompare([]byte(got), []byte(s.opts.ShutdownToken)) == 1 {
				next.ServeHTTP(w, r)
				return
			}
		}
		if r.URL.Path == "/api/v1/setup/status" || r.URL.Path == "/api/v1/setup" {
			next.ServeHTTP(w, r)
			return
		}

		snap := s.cfg.Snapshot()
		if snap.AdminPassHash == "" {
			// Allow static UI assets so First-Run Setup Wizard renders in browser,
			// and allow loopback admin requests; block remote API calls until setup completes.
			if !strings.HasPrefix(r.URL.Path, "/api/") || isAdminLoopback(r) {
				next.ServeHTTP(w, r)
				return
			}
			writeAdminError(w, http.StatusForbidden, "setup_required")
			return
		}

		ipStr := ""
		if ip := remoteHostIP(r); ip != nil {
			ipStr = ip.String()
		}
		if !s.allowAdminAuth(ipStr) {
			http.Error(w, "Too many failed authentication attempts", http.StatusTooManyRequests)
			return
		}

		user, pass, ok := r.BasicAuth()
		if !ok || !s.cfg.VerifyAdminAuth(user, pass) {
			s.recordAdminAuthResult(ipStr, false)
			w.Header().Set("WWW-Authenticate", `Basic realm="Gens-Relay-Admin"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		s.recordAdminAuthResult(ipStr, true)
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleAdminSetupStatus(w http.ResponseWriter, r *http.Request) {
	snap := s.cfg.Snapshot()
	local := isAdminLocalNet(r)
	writeAdminJSON(w, http.StatusOK, map[string]any{
		"needs_setup":          snap.AdminPassHash == "",
		"is_local":             local,
		"requires_setup_token": !local,
		"admin_user":           snap.AdminUser,
	})
}

func (s *Server) handleAdminSetupComplete(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Snapshot().AdminPassHash != "" {
		writeAdminError(w, http.StatusConflict, "setup already completed")
		return
	}
	var body struct {
		SetupToken       string `json:"setup_token"`
		AdminUser        string `json:"admin_user"`
		AdminPassword    string `json:"admin_password"`
		AuthToken        string `json:"auth_token"`
		EnableBlindRelay bool   `json:"enable_blind_relay"`
		MaxRelayKBps     int    `json:"max_relay_kbps"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !isAdminLocalNet(r) {
		tok := strings.ToUpper(strings.TrimSpace(body.SetupToken))
		if subtle.ConstantTimeCompare([]byte(tok), []byte(s.setupToken)) != 1 {
			writeAdminError(w, http.StatusForbidden, "invalid setup_token for remote setup")
			return
		}
	}
	if len(body.AdminPassword) < 4 {
		writeAdminError(w, http.StatusBadRequest, "admin password must be at least 4 characters")
		return
	}
	if err := s.cfg.Update(func(rc *RelayConfig) {
		if strings.TrimSpace(body.AdminUser) != "" {
			rc.AdminUser = strings.TrimSpace(body.AdminUser)
		}
		rc.AuthToken = body.AuthToken
		rc.EnableBlindRelay = body.EnableBlindRelay
		if body.MaxRelayKBps >= 0 {
			rc.MaxRelayKBps = body.MaxRelayKBps
		}
	}); err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.cfg.SetAdminPassword(body.AdminPassword); err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.refreshLimiter()
	writeAdminJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) hasAdminClients() bool {
	s.adminWsMu.Lock()
	n := len(s.adminWsClients)
	s.adminWsMu.Unlock()
	return n > 0
}

func (s *Server) adminBroadcastLoop() {
	for payload := range s.adminEventCh {
		s.adminWsMu.Lock()
		for sub := range s.adminWsClients {
			select {
			case sub.send <- payload:
			default:
				delete(s.adminWsClients, sub)
				close(sub.send)
				_ = sub.conn.CloseNow()
			}
		}
		s.adminWsMu.Unlock()
	}
}

func (s *Server) broadcastAdminEvent(event string, data any) {
	if !s.hasAdminClients() {
		return
	}
	payload, err := json.Marshal(map[string]any{
		"event": event,
		"data":  data,
	})
	if err != nil {
		return
	}
	select {
	case s.adminEventCh <- payload:
	default:
	}
}

func (s *Server) BroadcastLog(e logs.Entry) {
	s.broadcastAdminEvent("log_entry", e)
}

func (s *Server) broadcastAdminClients() {
	if !s.hasAdminClients() {
		return
	}
	s.broadcastAdminEvent("clients_updated", s.ClientsSnapshot())
}

func (s *Server) StatusSnapshot() map[string]any {
	snap := s.cfg.Snapshot()
	s.mu.RLock()
	onlineCount := len(s.clients)
	pairCodeCount := len(s.pairCodes)
	pendingCount := len(s.pendingSignaling)
	s.mu.RUnlock()

	maxClients := snap.MaxClients
	if maxClients <= 0 {
		maxClients = 2000
	}

	udpPort := atomic.LoadInt32(&s.stunPort)
	return map[string]any{
		"status":                "ok",
		"uptime_sec":            int64(time.Since(s.startedAt).Seconds()),
		"listen_addr":           snap.ListenAddr,
		"admin_listen":          snap.AdminListen,
		"stun_port":             udpPort,
		"udp_relay_port":        udpPort,
		"online_devices":        onlineCount,
		"active_pair_codes":     pairCodeCount,
		"pending_queues":        pendingCount,
		"blind_relay_enabled":   snap.EnableBlindRelay,
		"stores_files":          false,
		"has_auth_token":        snap.AuthToken != "",
		"has_admin_password":    snap.AdminPassHash != "",
		"trust_proxy":           snap.TrustProxy,
		"max_relay_kbps":        snap.MaxRelayKBps,
		"max_clients":           maxClients,
		"relay_packets_total":   s.relayPacketsTotal.Load(),
		"relay_bytes_total":     s.relayBytesTotal.Load(),
		"udp_relay_bytes_total": s.udpRelayBytesTotal.Load(),
		"ws_relay_bytes_total":  s.wsRelayBytesTotal.Load(),
		"relay_bps":             s.relayBps.Load(),
	}
}

type AdminClientInfo struct {
	DeviceID      string   `json:"device_id"`
	Name          string   `json:"name"`
	ObservedAddr  string   `json:"observed_addr"`
	LANAddrs      []string `json:"lan_addrs"`
	QUICPort      int      `json:"quic_port"`
	ConnectedAt   int64    `json:"connected_at"`
	RelayRxBytes  uint64   `json:"relay_rx_bytes"`
	RelayTxBytes  uint64   `json:"relay_tx_bytes"`
	WatchingCount int      `json:"watching_count"`
	UDPRelay      bool     `json:"udp_relay"`
}

func (s *Server) ClientsSnapshot() []AdminClientInfo {
	s.mu.RLock()
	out := make([]AdminClientInfo, 0, len(s.clients))
	for _, c := range s.clients {
		c.mu.RLock()
		obs := c.observedAddr
		watchN := len(c.watchIDs)
		hasUDP := c.udpRelayAddr != nil
		c.mu.RUnlock()
		out = append(out, AdminClientInfo{
			DeviceID:      c.deviceID,
			Name:          c.name,
			ObservedAddr:  obs,
			LANAddrs:      c.lanAddrs,
			QUICPort:      c.quicPort,
			ConnectedAt:   c.connectedAt,
			RelayRxBytes:  c.rxBytes.Load(),
			RelayTxBytes:  c.txBytes.Load(),
			WatchingCount: watchN,
			UDPRelay:      hasUDP,
		})
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

func (s *Server) handleAdminStatus(w http.ResponseWriter, _ *http.Request) {
	writeAdminJSON(w, http.StatusOK, s.StatusSnapshot())
}

func (s *Server) handleAdminClients(w http.ResponseWriter, _ *http.Request) {
	writeAdminJSON(w, http.StatusOK, s.ClientsSnapshot())
}

func (s *Server) handleAdminKick(w http.ResponseWriter, r *http.Request) {
	id := protocol.NormalizeDeviceID(r.PathValue("id"))
	s.mu.RLock()
	c, ok := s.clients[id]
	s.mu.RUnlock()
	if !ok {
		writeAdminError(w, http.StatusNotFound, "device not connected")
		return
	}
	c.closed.Store(true)
	_ = c.ws.Close(websocket.StatusPolicyViolation, "kicked by administrator")
	writeAdminJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleAdminListBans(w http.ResponseWriter, _ *http.Request) {
	snap := s.cfg.Snapshot()
	type banItem struct {
		DeviceID string `json:"device_id"`
		Reason   string `json:"reason"`
	}
	out := make([]banItem, 0, len(snap.BannedDevices))
	for id, reason := range snap.BannedDevices {
		out = append(out, banItem{DeviceID: id, Reason: reason})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DeviceID < out[j].DeviceID })
	writeAdminJSON(w, http.StatusOK, out)
}

func (s *Server) handleAdminAddBan(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DeviceID string `json:"device_id"`
		Reason   string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}
	id := protocol.NormalizeDeviceID(body.DeviceID)
	if id == "" {
		writeAdminError(w, http.StatusBadRequest, "invalid device_id")
		return
	}
	if err := s.cfg.BanDevice(id, body.Reason); err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.mu.RLock()
	c, online := s.clients[id]
	s.mu.RUnlock()
	if online {
		c.closed.Store(true)
		_ = c.ws.Close(websocket.StatusPolicyViolation, "banned by administrator")
	}
	writeAdminJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleAdminRemoveBan(w http.ResponseWriter, r *http.Request) {
	id := protocol.NormalizeDeviceID(r.PathValue("id"))
	if err := s.cfg.UnbanDevice(id); err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeAdminJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleAdminPairCodes(w http.ResponseWriter, _ *http.Request) {
	type codeItem struct {
		Code      string `json:"code"`
		DeviceID  string `json:"device_id"`
		Name      string `json:"name"`
		ExpiresAt int64  `json:"expires_at"`
	}
	now := time.Now()
	s.mu.RLock()
	out := make([]codeItem, 0, len(s.pairCodes))
	for code, ent := range s.pairCodes {
		if now.After(ent.expiresAt) {
			continue
		}
		out = append(out, codeItem{
			Code:      code,
			DeviceID:  ent.hint.DeviceID,
			Name:      ent.hint.Name,
			ExpiresAt: ent.expiresAt.Unix(),
		})
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ExpiresAt < out[j].ExpiresAt })
	writeAdminJSON(w, http.StatusOK, out)
}

func (s *Server) handleAdminRevokeCode(w http.ResponseWriter, r *http.Request) {
	code := strings.TrimSpace(r.PathValue("code"))
	s.mu.Lock()
	delete(s.pairCodes, code)
	s.mu.Unlock()
	writeAdminJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleAdminPending(w http.ResponseWriter, _ *http.Request) {
	type pendingInfo struct {
		TargetID string `json:"target_id"`
		Count    int    `json:"count"`
		OldestAt int64  `json:"oldest_at"`
	}
	s.mu.RLock()
	out := make([]pendingInfo, 0, len(s.pendingSignaling))
	for tid, q := range s.pendingSignaling {
		if len(q) == 0 {
			continue
		}
		out = append(out, pendingInfo{
			TargetID: tid,
			Count:    len(q),
			OldestAt: q[0].createdAt.Unix(),
		})
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].TargetID < out[j].TargetID })
	writeAdminJSON(w, http.StatusOK, out)
}

func (s *Server) handleAdminGetConfig(w http.ResponseWriter, _ *http.Request) {
	snap := s.cfg.Snapshot()
	writeAdminJSON(w, http.StatusOK, map[string]any{
		"listen_addr":        snap.ListenAddr,
		"admin_listen":       snap.AdminListen,
		"enable_blind_relay": snap.EnableBlindRelay,
		"trust_proxy":        snap.TrustProxy,
		"max_relay_kbps":     snap.MaxRelayKBps,
		"max_clients":        snap.MaxClients,
		"admin_user":         snap.AdminUser,
		"has_auth_token":     snap.AuthToken != "",
		"has_admin_password": snap.AdminPassHash != "",
	})
}

func (s *Server) handleAdminUpdateConfig(w http.ResponseWriter, r *http.Request) {
	var body struct {
		AuthToken          string  `json:"auth_token"`
		ClearAuthToken     bool    `json:"clear_auth_token"`
		EnableBlindRelay   *bool   `json:"enable_blind_relay"`
		TrustProxy         *bool   `json:"trust_proxy"`
		MaxRelayKBps       *int    `json:"max_relay_kbps"`
		MaxClients         *int    `json:"max_clients"`
		AdminUser          *string `json:"admin_user"`
		AdminPassword      string  `json:"admin_password"`
		ClearAdminPassword bool    `json:"clear_admin_password"`
		CurrentPassword    string  `json:"current_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.cfg.Update(func(rc *RelayConfig) {
		if body.ClearAuthToken {
			rc.AuthToken = ""
		} else if body.AuthToken != "" {
			rc.AuthToken = body.AuthToken
		}
		if body.EnableBlindRelay != nil {
			rc.EnableBlindRelay = *body.EnableBlindRelay
		}
		if body.TrustProxy != nil {
			rc.TrustProxy = *body.TrustProxy
		}
		if body.MaxRelayKBps != nil && *body.MaxRelayKBps >= 0 {
			rc.MaxRelayKBps = *body.MaxRelayKBps
		}
		if body.MaxClients != nil && *body.MaxClients >= 10 {
			rc.MaxClients = *body.MaxClients
		}
		if body.AdminUser != nil {
			rc.AdminUser = *body.AdminUser
		}
	}); err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if body.ClearAdminPassword {
		// Effacer le mot de passe admin est une action sensible : exiger la
		// ré-authentification explicite (current_password) avant d'effacer.
		if body.CurrentPassword == "" {
			writeAdminError(w, http.StatusBadRequest, "current_password requis pour effacer le mot de passe admin")
			return
		}
		cSnap := s.cfg.Snapshot()
		if cSnap.AdminPassHash != "" && !s.cfg.VerifyAdminAuth(cSnap.AdminUser, body.CurrentPassword) {
			writeAdminError(w, http.StatusUnauthorized, "mot de passe admin actuel incorrect")
			return
		}
		_ = s.cfg.SetAdminPassword("")
	} else if body.AdminPassword != "" {
		if err := s.cfg.SetAdminPassword(body.AdminPassword); err != nil {
			writeAdminError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	s.refreshLimiter()
	s.broadcastAdminEvent("status_updated", s.StatusSnapshot())
	writeAdminJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleAdminLogs(w http.ResponseWriter, _ *http.Request) {
	if s.logs == nil {
		writeAdminJSON(w, http.StatusOK, []any{})
		return
	}
	writeAdminJSON(w, http.StatusOK, s.logs.List())
}

func (s *Server) handleAdminEvents(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); !isAllowedAdminOrigin(origin, r.Host) {
		http.Error(w, "Forbidden origin", http.StatusForbidden)
		return
	}
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	sub := &adminWsSub{
		conn: ws,
		send: make(chan []byte, 64),
	}
	s.adminWsMu.Lock()
	s.adminWsClients[sub] = struct{}{}
	s.adminWsMu.Unlock()

	defer func() {
		s.adminWsMu.Lock()
		if _, exists := s.adminWsClients[sub]; exists {
			delete(s.adminWsClients, sub)
			close(sub.send)
		}
		s.adminWsMu.Unlock()
		_ = ws.CloseNow()
	}()

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

func (s *Server) handleAdminShutdown(w http.ResponseWriter, r *http.Request) {
	if !isAdminLoopback(r) {
		writeAdminError(w, http.StatusForbidden, "shutdown allowed only from loopback")
		return
	}
	writeAdminJSON(w, http.StatusOK, map[string]any{"ok": true})
	if s.opts.OnShutdown != nil {
		go func() {
			time.Sleep(50 * time.Millisecond)
			s.opts.OnShutdown()
		}()
	}
}

func writeAdminJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeAdminError(w http.ResponseWriter, code int, msg string) {
	writeAdminJSON(w, code, map[string]any{"error": msg})
}
