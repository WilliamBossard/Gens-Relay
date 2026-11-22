package relay

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WilliamBossard/Gens-Relay/internal/logs"
	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
	"github.com/coder/websocket"
	"golang.org/x/time/rate"
)

type Options struct {
	ListenAddr       string
	AdminListen      string
	AuthToken        string
	EnableBlindRelay bool
	TrustProxy       bool
	MaxRelayKBps     int
	AdminUser        string
	AdminPassword    string
	Config           *RelayConfig
	Logs             *logs.RingBuffer
	ShutdownToken    string
	OnShutdown       func()
}

type pairEntry struct {
	hint      protocol.PeerHint
	expiresAt time.Time
}

type pendingEntry struct {
	msg       protocol.SignalingMessage
	createdAt time.Time
}

type wsOutFrame struct {
	typ     websocket.MessageType
	data    []byte
	poolPtr *[]byte
}

var relayFramePool = sync.Pool{
	New: func() any {
		b := make([]byte, 4096+300)
		return &b
	},
}

type clientConn struct {
	deviceID      string
	name          string
	lanAddrs      []string
	quicPort      int
	observedIP    string
	observedAddr  string
	stunToken     string
	udpToken      [16]byte
	udpRelayAddr  *net.UDPAddr
	connectedAt   int64
	watchIDs      map[string]struct{}
	ws            *websocket.Conn
	ctrlCh        chan wsOutFrame // priority signaling channel
	relayCh       chan wsOutFrame // droppable best-effort binary relay channel
	closed        atomic.Bool
	rxBytes       atomic.Uint64
	txBytes       atomic.Uint64
	redeemLimiter *rate.Limiter
	relayLimiter  *rate.Limiter
	relayLimVer   uint64
	mu            sync.RWMutex
}

func newClientConn(deviceID, name string, lanAddrs []string, quicPort int, observedIP, observedAddr, stunToken string, udpToken [16]byte, ws *websocket.Conn) *clientConn {
	return &clientConn{
		deviceID:      deviceID,
		name:          name,
		lanAddrs:      lanAddrs,
		quicPort:      quicPort,
		observedIP:    observedIP,
		observedAddr:  observedAddr,
		stunToken:     stunToken,
		udpToken:      udpToken,
		connectedAt:   time.Now().Unix(),
		watchIDs:      make(map[string]struct{}),
		ws:            ws,
		ctrlCh:        make(chan wsOutFrame, 128),
		relayCh:       make(chan wsOutFrame, 256),
		redeemLimiter: rate.NewLimiter(rate.Every(12*time.Second), 5),
	}
}

func (c *clientConn) writeFrame(ctx context.Context, frame wsOutFrame) bool {
	wCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := c.ws.Write(wCtx, frame.typ, frame.data)
	cancel()
	if frame.poolPtr != nil && cap(*frame.poolPtr) <= 8192 {
		relayFramePool.Put(frame.poolPtr)
	}
	if err != nil {
		_ = c.ws.CloseNow()
		return false
	}
	return true
}

func (c *clientConn) startWriter(ctx context.Context) {
	go func() {
		for {
			// Always prioritize signaling control frames over best-effort relay data
			select {
			case <-ctx.Done():
				return
			case frame := <-c.ctrlCh:
				if !c.writeFrame(ctx, frame) {
					return
				}
				continue
			default:
			}

			select {
			case <-ctx.Done():
				return
			case frame := <-c.ctrlCh:
				if !c.writeFrame(ctx, frame) {
					return
				}
			case frame := <-c.relayCh:
				if !c.writeFrame(ctx, frame) {
					return
				}
			}
		}
	}()
}

func (c *clientConn) send(_ context.Context, msg protocol.SignalingMessage) error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	select {
	case c.ctrlCh <- wsOutFrame{typ: websocket.MessageText, data: data}:
		return nil
	default:
		return fmt.Errorf("client control queue saturated")
	}
}

func (c *clientConn) sendBinary(_ context.Context, data []byte, poolPtr *[]byte) error {
	if c.closed.Load() {
		if poolPtr != nil && cap(*poolPtr) <= 8192 {
			relayFramePool.Put(poolPtr)
		}
		return net.ErrClosed
	}
	select {
	case c.relayCh <- wsOutFrame{typ: websocket.MessageBinary, data: data, poolPtr: poolPtr}:
		return nil
	default:
		if poolPtr != nil && cap(*poolPtr) <= 8192 {
			relayFramePool.Put(poolPtr)
		}
		return fmt.Errorf("client relay queue saturated")
	}
}

type authFailEntry struct {
	count   int
	resetAt time.Time
}

// Server implements the zero-file-transit signaling, STUN, and UDP/WS Blind Relay server.
type Server struct {
	opts       Options
	cfg        *RelayConfig
	logs       *logs.RingBuffer
	startedAt  time.Time
	stunPort   int32
	setupToken string

	mu               sync.RWMutex
	clients          map[string]*clientConn
	udpTokens        map[[16]byte]*clientConn
	pairCodes        map[string]pairEntry
	pendingSignaling map[string][]pendingEntry
	udpConn          *net.UDPConn

	limiterMu    sync.RWMutex
	relayLimiter *rate.Limiter
	relayLimKBps int
	relayLimVer  atomic.Uint64

	ipLimMu    sync.Mutex
	ipLimiters map[string]*ipRateEntry
	authFailMu sync.Mutex
	authFails  map[string]*authFailEntry

	adminWsMu      sync.Mutex
	adminWsClients map[*adminWsSub]struct{}
	adminEventCh   chan []byte
	stopOnce       sync.Once

	relayPacketsTotal  atomic.Uint64
	relayBytesTotal    atomic.Uint64
	udpRelayBytesTotal atomic.Uint64
	wsRelayBytesTotal  atomic.Uint64
	relayBps           atomic.Int64
}

type ipRateEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

func NewServer(opts Options) *Server {
	cfg := opts.Config
	if cfg == nil {
		cfg, _ = LoadOrCreateConfig("", opts)
	}
	var setupTokBytes [6]byte
	_, _ = rand.Read(setupTokBytes[:])
	setupTok := strings.ToUpper(hex.EncodeToString(setupTokBytes[:]))

	s := &Server{
		opts:             opts,
		cfg:              cfg,
		logs:             opts.Logs,
		startedAt:        time.Now(),
		setupToken:       setupTok,
		clients:          make(map[string]*clientConn),
		udpTokens:        make(map[[16]byte]*clientConn),
		pairCodes:        make(map[string]pairEntry),
		pendingSignaling: make(map[string][]pendingEntry),
		ipLimiters:       make(map[string]*ipRateEntry),
		authFails:        make(map[string]*authFailEntry),
		adminWsClients:   make(map[*adminWsSub]struct{}),
		adminEventCh:     make(chan []byte, 256),
	}
	if cfg.Snapshot().AdminPassHash == "" && opts.Config != nil {
		slog.Info("first-run setup active")
	}
	s.refreshLimiter()
	go s.adminBroadcastLoop()
	return s
}

func (s *Server) SetupToken() string {
	return s.setupToken
}

func (s *Server) allowIPConnect(ip string) bool {
	if ip == "" {
		return true
	}
	now := time.Now()
	s.ipLimMu.Lock()
	ent, ok := s.ipLimiters[ip]
	if !ok {
		ent = &ipRateEntry{
			limiter: rate.NewLimiter(rate.Every(3*time.Second), 20),
		}
		s.ipLimiters[ip] = ent
	}
	ent.lastSeen = now
	lim := ent.limiter
	s.ipLimMu.Unlock()
	return lim.Allow()
}

func (s *Server) allowAdminAuth(ip string) bool {
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

func (s *Server) recordAdminAuthResult(ip string, ok bool) {
	if ip == "" {
		return
	}
	now := time.Now()
	s.authFailMu.Lock()
	defer s.authFailMu.Unlock()
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

func (s *Server) refreshLimiter() {
	snap := s.cfg.Snapshot()
	s.limiterMu.Lock()
	s.relayLimKBps = snap.MaxRelayKBps
	if snap.MaxRelayKBps > 0 {
		bps := rate.Limit(snap.MaxRelayKBps * 1024)
		burst := snap.MaxRelayKBps * 1024
		if burst < 64*1024 {
			burst = 64 * 1024
		}
		s.relayLimiter = rate.NewLimiter(bps, burst)
	} else {
		s.relayLimiter = nil
	}
	s.limiterMu.Unlock()
	s.relayLimVer.Add(1)
}

func (s *Server) allowRelayBytes(client *clientConn, n int) bool {
	ver := s.relayLimVer.Load()
	s.limiterMu.RLock()
	kbps := s.relayLimKBps
	globalLim := s.relayLimiter
	s.limiterMu.RUnlock()

	if kbps <= 0 {
		return true
	}

	now := time.Now()
	if globalLim != nil {
		gn := n
		if gn > globalLim.Burst() {
			gn = globalLim.Burst()
		}
		if !globalLim.AllowN(now, gn) {
			return false
		}
	}

	client.mu.Lock()
	if client.relayLimVer != ver || client.relayLimiter == nil {
		bps := rate.Limit(kbps * 1024)
		burst := kbps * 1024
		if burst < 64*1024 {
			burst = 64 * 1024
		}
		client.relayLimiter = rate.NewLimiter(bps, burst)
		client.relayLimVer = ver
	}
	lim := client.relayLimiter
	client.mu.Unlock()

	cn := n
	if cn > lim.Burst() {
		cn = lim.Burst()
	}
	return lim.AllowN(now, cn)
}

// StartBackground launches periodic cleanup and live throughput sampling for the admin UI.
func (s *Server) StartBackground(ctx context.Context) {
	go func() {
		secTicker := time.NewTicker(1 * time.Second)
		pruneTicker := time.NewTicker(5 * time.Minute)
		defer secTicker.Stop()
		defer pruneTicker.Stop()

		prevBytes := s.relayBytesTotal.Load()
		for {
			select {
			case <-ctx.Done():
				return
			case <-secTicker.C:
				cur := s.relayBytesTotal.Load()
				s.relayBps.Store(int64(cur - prevBytes))
				prevBytes = cur
				if s.hasAdminClients() {
					s.broadcastAdminEvent("status_updated", s.StatusSnapshot())
				}
			case <-pruneTicker.C:
				s.pruneExpired()
			}
		}
	}()
}

// Shutdown cleanly terminates all client sessions, admin subscribers, and the UDP socket.
func (s *Server) Shutdown(_ context.Context) {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		clients := make([]*clientConn, 0, len(s.clients))
		for _, c := range s.clients {
			clients = append(clients, c)
		}
		udpConn := s.udpConn
		s.udpConn = nil
		s.mu.Unlock()

		for _, c := range clients {
			c.closed.Store(true)
			_ = c.ws.Close(websocket.StatusGoingAway, "relay server shutting down")
		}
		if udpConn != nil {
			_ = udpConn.Close()
		}

		s.adminWsMu.Lock()
		for sub := range s.adminWsClients {
			delete(s.adminWsClients, sub)
			close(sub.send)
			_ = sub.conn.CloseNow()
		}
		s.adminWsMu.Unlock()
		close(s.adminEventCh)
	})
}

func (s *Server) pruneExpired() {
	now := time.Now()
	cutoff := now.Add(-24 * time.Hour)
	s.mu.Lock()
	for code, ent := range s.pairCodes {
		if now.After(ent.expiresAt) {
			delete(s.pairCodes, code)
		}
	}
	for tid, q := range s.pendingSignaling {
		fresh := q[:0]
		for _, item := range q {
			if item.createdAt.After(cutoff) {
				fresh = append(fresh, item)
			}
		}
		if len(fresh) == 0 {
			delete(s.pendingSignaling, tid)
		} else {
			s.pendingSignaling[tid] = fresh
		}
	}
	s.mu.Unlock()

	ipCutoff := now.Add(-15 * time.Minute)
	s.ipLimMu.Lock()
	for ip, ent := range s.ipLimiters {
		if ent.lastSeen.Before(ipCutoff) {
			delete(s.ipLimiters, ip)
		}
	}
	s.ipLimMu.Unlock()

	s.authFailMu.Lock()
	for ip, ent := range s.authFails {
		if now.After(ent.resetAt) {
			delete(s.authFails, ip)
		}
	}
	s.authFailMu.Unlock()
}

// StartUDP launches the multiplexed UDP STUN + native UDP Blind Relay listener.
func (s *Server) StartUDP(ctx context.Context, addr string) error {
	udpAddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		return err
	}
	_ = conn.SetReadBuffer(8 * 1024 * 1024)
	_ = conn.SetWriteBuffer(8 * 1024 * 1024)
	port := conn.LocalAddr().(*net.UDPAddr).Port
	atomic.StoreInt32(&s.stunPort, int32(port))

	s.mu.Lock()
	s.udpConn = conn
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
	go func() {
		buf := make([]byte, 4096)
		for {
			n, remote, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			pkt := buf[:n]

			// 1. Native UDP Blind Relay frame
			if tok, targetID, payload, ok := protocol.DecodeUDPRelayPacket(pkt); ok {
				s.handleUDPRelayPacket(ctx, conn, remote, tok, targetID, payload)
				continue
			}

			// 2. Authenticated UDP STUN echo
			line := strings.TrimSpace(string(pkt))
			if strings.HasPrefix(line, "GENSSTUN:") {
				rest := strings.TrimPrefix(line, "GENSSTUN:")
				parts := strings.SplitN(rest, ":", 2)
				devID := protocol.NormalizeDeviceID(parts[0])
				stunTok := ""
				if len(parts) > 1 {
					stunTok = parts[1]
				}
				remoteStr := remote.String()
				remoteIP := remote.IP.String()
				updated := false

				s.mu.RLock()
				c, exists := s.clients[devID]
				s.mu.RUnlock()
				if exists && c.stunToken != "" && stunTok == c.stunToken {
					c.mu.Lock()
					if c.observedIP == remoteIP || remote.IP.IsLoopback() {
						if c.observedAddr != remoteStr {
							c.observedAddr = remoteStr
							updated = true
						}
					}
					c.mu.Unlock()
					if updated {
						s.notifyWatchers(devID)
					}
					_, _ = conn.WriteToUDP([]byte("GENSSTUN_OK:"+remoteStr), remote)
				}
			}
		}
	}()
	return nil
}

func (s *Server) handleUDPRelayPacket(ctx context.Context, conn *net.UDPConn, remote *net.UDPAddr, tok [16]byte, targetID string, payload []byte) {
	s.mu.RLock()
	sender, ok := s.udpTokens[tok]
	s.mu.RUnlock()
	if !ok {
		return
	}

	sender.mu.Lock()
	sender.udpRelayAddr = &net.UDPAddr{IP: append([]byte(nil), remote.IP...), Port: remote.Port, Zone: remote.Zone}
	sender.mu.Unlock()

	if targetID == "" {
		// Keepalive / probe echo confirming bidirectional UDP reachability
		pong := protocol.EncodeUDPRelayPacket(nil, tok, "", nil)
		_, _ = conn.WriteToUDP(pong, remote)
		return
	}

	if !s.cfg.Snapshot().EnableBlindRelay || len(payload) == 0 {
		return
	}
	if !s.allowRelayBytes(sender, len(payload)) {
		return
	}

	s.mu.RLock()
	target, online := s.clients[targetID]
	s.mu.RUnlock()
	if !online {
		return
	}

	target.mu.RLock()
	tAddr := target.udpRelayAddr
	tTok := target.udpToken
	target.mu.RUnlock()

	if tAddr != nil {
		bufPtr := relayFramePool.Get().(*[]byte)
		out := protocol.EncodeUDPRelayPacket(*bufPtr, tTok, sender.deviceID, payload)
		_, err := conn.WriteToUDP(out, tAddr)
		if cap(out) <= 8192 {
			*bufPtr = out[:cap(out)]
			relayFramePool.Put(bufPtr)
		}
		if err == nil {
			s.relayPacketsTotal.Add(1)
			s.relayBytesTotal.Add(uint64(len(payload)))
			s.udpRelayBytesTotal.Add(uint64(len(payload)))
			sender.txBytes.Add(uint64(len(payload)))
			target.rxBytes.Add(uint64(len(payload)))
		}
		return
	}

	// Fallback seamlessly to WebSocket binary frame if target lacks an active UDP relay mapping
	senderBytes := []byte(sender.deviceID)
	if len(senderBytes) > 255 {
		return
	}
	need := 2 + len(senderBytes) + len(payload)
	bufPtr := relayFramePool.Get().(*[]byte)
	var wsOut []byte
	var usedPool *[]byte
	if cap(*bufPtr) >= need {
		wsOut = (*bufPtr)[:need]
		usedPool = bufPtr
	} else {
		relayFramePool.Put(bufPtr)
		wsOut = make([]byte, need)
	}
	wsOut[0] = 0x01
	wsOut[1] = byte(len(senderBytes))
	copy(wsOut[2:], senderBytes)
	copy(wsOut[2+len(senderBytes):], payload)

	if err := target.sendBinary(ctx, wsOut, usedPool); err == nil {
		s.relayPacketsTotal.Add(1)
		s.relayBytesTotal.Add(uint64(len(payload)))
		s.wsRelayBytesTotal.Add(uint64(len(payload)))
		sender.txBytes.Add(uint64(len(payload)))
		target.rxBytes.Add(uint64(len(payload)))
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/admin", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/admin/", http.StatusMovedPermanently)
	})
	mux.Handle("/admin/", http.StripPrefix("/admin", s.AdminHandler()))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			s.handleWS(w, r)
			return
		}
		s.handleHealth(w, r)
	})
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	snap := s.cfg.Snapshot()
	s.mu.RLock()
	onlineCount := len(s.clients)
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":              "ok",
		"online_devices":      onlineCount,
		"blind_relay_enabled": snap.EnableBlindRelay,
		"stun_port":           atomic.LoadInt32(&s.stunPort),
		"udp_relay_port":      atomic.LoadInt32(&s.stunPort),
		"stores_files":        false,
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	onlineCount := len(s.clients)
	pairCodeCount := len(s.pairCodes)
	pendingCount := len(s.pendingSignaling)
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w,
		"gens_relay_online_devices %d\ngens_relay_active_pair_codes %d\ngens_relay_pending_queues %d\ngens_relay_blind_packets_total %d\ngens_relay_blind_bytes_total %d\ngens_relay_udp_bytes_total %d\ngens_relay_ws_bytes_total %d\n",
		onlineCount, pairCodeCount, pendingCount, s.relayPacketsTotal.Load(), s.relayBytesTotal.Load(), s.udpRelayBytesTotal.Load(), s.wsRelayBytesTotal.Load(),
	)
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	remoteIP := s.extractRemoteIP(r)
	if !s.allowIPConnect(remoteIP) {
		http.Error(w, "too many connections", http.StatusTooManyRequests)
		return
	}
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	ws.SetReadLimit(256 * 1024)
	defer ws.Close(websocket.StatusNormalClosure, "")

	ctx := r.Context()

	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return
	}
	challengeB64 := base64.StdEncoding.EncodeToString(nonce[:])
	if err := writeWSJSON(ctx, ws, protocol.SignalingMessage{
		Type:      protocol.SigChallenge,
		Challenge: challengeB64,
	}); err != nil {
		return
	}

	authCtx, authCancel := context.WithTimeout(ctx, 10*time.Second)
	regMsg, err := readWSJSON(authCtx, ws)
	authCancel()
	if err != nil || regMsg.Type != protocol.SigRegister {
		_ = writeWSJSON(ctx, ws, protocol.SignalingMessage{Type: protocol.SigError, Error: "expected register message"})
		return
	}

	snap := s.cfg.Snapshot()
	if snap.AuthToken != "" {
		if !protocol.VerifyAuthProof(snap.AuthToken, nonce[:], regMsg.AuthProofB64) {
			_ = writeWSJSON(ctx, ws, protocol.SignalingMessage{Type: protocol.SigError, Error: "invalid auth proof"})
			return
		}
	}

	pubDER, err := base64.StdEncoding.DecodeString(regMsg.PubKeyB64)
	if err != nil {
		_ = writeWSJSON(ctx, ws, protocol.SignalingMessage{Type: protocol.SigError, Error: "invalid pub_key"})
		return
	}
	sigBytes, err := base64.StdEncoding.DecodeString(regMsg.SignatureB64)
	if err != nil {
		_ = writeWSJSON(ctx, ws, protocol.SignalingMessage{Type: protocol.SigError, Error: "invalid signature"})
		return
	}
	deviceID := protocol.NormalizeDeviceID(regMsg.DeviceID)
	if err := protocol.VerifyChallenge(deviceID, pubDER, nonce[:], sigBytes); err != nil {
		_ = writeWSJSON(ctx, ws, protocol.SignalingMessage{Type: protocol.SigError, Error: err.Error()})
		return
	}
	if s.cfg.IsBanned(deviceID) {
		slog.Warn("rejected banned device", "device", deviceID)
		_ = writeWSJSON(ctx, ws, protocol.SignalingMessage{Type: protocol.SigError, Error: "device is banned"})
		return
	}

	observedIP := remoteIP
	observedAddr := ""
	if observedIP != "" && regMsg.QUICPort > 0 {
		observedAddr = net.JoinHostPort(observedIP, fmt.Sprintf("%d", regMsg.QUICPort))
	} else {
		observedAddr = observedIP
	}

	var stunTokBytes [8]byte
	var udpTok [16]byte
	_, _ = rand.Read(stunTokBytes[:])
	_, _ = rand.Read(udpTok[:])
	stunToken := hex.EncodeToString(stunTokBytes[:])

	client := newClientConn(deviceID, regMsg.Name, regMsg.LANAddrs, regMsg.QUICPort, observedIP, observedAddr, stunToken, udpTok, ws)

	maxClients := snap.MaxClients
	if maxClients <= 0 {
		maxClients = 2000
	}

	s.mu.Lock()
	old, existed := s.clients[deviceID]
	if !existed && len(s.clients) >= maxClients {
		s.mu.Unlock()
		_ = writeWSJSON(ctx, ws, protocol.SignalingMessage{Type: protocol.SigError, Error: "relay server at client capacity"})
		return
	}
	if existed {
		old.closed.Store(true)
		delete(s.udpTokens, old.udpToken)
		_ = old.ws.Close(websocket.StatusPolicyViolation, "replaced by new session")
	}
	s.clients[deviceID] = client
	s.udpTokens[udpTok] = client
	s.mu.Unlock()

	connCtx, connCancel := context.WithCancel(ctx)
	defer connCancel()

	client.startWriter(connCtx)
	slog.Info("device connected", "device", deviceID, "name", client.name, "observed", observedAddr)
	s.broadcastAdminClients()

	defer func() {
		client.closed.Store(true)
		s.mu.Lock()
		if cur, ok := s.clients[deviceID]; ok && cur == client {
			delete(s.clients, deviceID)
		}
		delete(s.udpTokens, udpTok)
		s.mu.Unlock()
		s.notifyWatchers(deviceID)
		s.broadcastAdminClients()
		slog.Info("device disconnected", "device", deviceID)
	}()

	udpPort := int(atomic.LoadInt32(&s.stunPort))
	_ = client.send(connCtx, protocol.SignalingMessage{
		Type:          protocol.SigRegistered,
		DeviceID:      deviceID,
		ObservedAddr:  observedAddr,
		STUNPort:      udpPort,
		STUNToken:     stunToken,
		UDPRelayPort:  udpPort,
		UDPRelayToken: hex.EncodeToString(udpTok[:]),
	})
	s.flushPendingSignaling(connCtx, client)
	s.notifyWatchers(deviceID)

	for {
		msgType, data, err := ws.Read(connCtx)
		if err != nil {
			return
		}
		if msgType == websocket.MessageBinary {
			s.handleBinaryRelay(connCtx, client, data)
			continue
		}
		var msg protocol.SignalingMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		s.handleClientMessage(connCtx, client, msg)
	}
}

func (s *Server) handleBinaryRelay(ctx context.Context, client *clientConn, data []byte) {
	if !s.cfg.Snapshot().EnableBlindRelay || len(data) < 3 || data[0] != 0x01 {
		return
	}
	idLen := int(data[1])
	if idLen == 0 || len(data) < 2+idLen {
		return
	}
	targetID := protocol.NormalizeDeviceID(string(data[2 : 2+idLen]))
	payload := data[2+idLen:]
	if !s.allowRelayBytes(client, len(payload)) {
		return
	}

	s.mu.RLock()
	target, ok := s.clients[targetID]
	s.mu.RUnlock()
	if !ok {
		return
	}

	// If target has an active UDP relay mapping, forward directly over UDP!
	target.mu.RLock()
	tAddr := target.udpRelayAddr
	tTok := target.udpToken
	target.mu.RUnlock()
	s.mu.RLock()
	udpConn := s.udpConn
	s.mu.RUnlock()
	if tAddr != nil && udpConn != nil {
		bufPtr := relayFramePool.Get().(*[]byte)
		out := protocol.EncodeUDPRelayPacket(*bufPtr, tTok, client.deviceID, payload)
		_, err := udpConn.WriteToUDP(out, tAddr)
		if cap(out) <= 8192 {
			*bufPtr = out[:cap(out)]
			relayFramePool.Put(bufPtr)
		}
		if err == nil {
			s.relayPacketsTotal.Add(1)
			s.relayBytesTotal.Add(uint64(len(payload)))
			s.udpRelayBytesTotal.Add(uint64(len(payload)))
			client.txBytes.Add(uint64(len(payload)))
			target.rxBytes.Add(uint64(len(payload)))
		}
		return
	}

	senderBytes := []byte(client.deviceID)
	if len(senderBytes) > 255 {
		return
	}
	need := 2 + len(senderBytes) + len(payload)
	bufPtr := relayFramePool.Get().(*[]byte)
	var out []byte
	var usedPool *[]byte
	if cap(*bufPtr) >= need {
		out = (*bufPtr)[:need]
		usedPool = bufPtr
	} else {
		relayFramePool.Put(bufPtr)
		out = make([]byte, need)
	}
	out[0] = 0x01
	out[1] = byte(len(senderBytes))
	copy(out[2:], senderBytes)
	copy(out[2+len(senderBytes):], payload)

	if err := target.sendBinary(ctx, out, usedPool); err == nil {
		s.relayPacketsTotal.Add(1)
		s.relayBytesTotal.Add(uint64(len(payload)))
		s.wsRelayBytesTotal.Add(uint64(len(payload)))
		client.txBytes.Add(uint64(len(payload)))
		target.rxBytes.Add(uint64(len(payload)))
	}
}

func (s *Server) handleClientMessage(ctx context.Context, client *clientConn, msg protocol.SignalingMessage) {
	switch msg.Type {
	case protocol.SigCreatePairCode:
		code, err := s.generatePairCode(client.toHint())
		if err != nil {
			_ = client.send(ctx, protocol.SignalingMessage{Type: protocol.SigError, Error: err.Error()})
			return
		}
		_ = client.send(ctx, protocol.SignalingMessage{
			Type:         protocol.SigPairCodeCreated,
			PairCode:     code,
			ExpiresInSec: 300,
		})

	case protocol.SigRedeemPairCode:
		if client.redeemLimiter != nil && !client.redeemLimiter.Allow() {
			_ = client.send(ctx, protocol.SignalingMessage{
				Type:  protocol.SigError,
				Error: "too many pair code attempts, please wait",
			})
			return
		}
		code := strings.ToUpper(strings.TrimSpace(msg.PairCode))
		s.mu.Lock()
		entry, ok := s.pairCodes[code]
		if ok && time.Now().After(entry.expiresAt) {
			delete(s.pairCodes, code)
			ok = false
		}
		if ok {
			delete(s.pairCodes, code)
		}
		s.mu.Unlock()

		if !ok {
			_ = client.send(ctx, protocol.SignalingMessage{
				Type:  protocol.SigError,
				Error: "invalid or expired pair code",
			})
			return
		}

		hint := entry.hint
		s.mu.RLock()
		if live, online := s.clients[hint.DeviceID]; online {
			hint = live.toHint()
		}
		s.mu.RUnlock()

		_ = client.send(ctx, protocol.SignalingMessage{
			Type: protocol.SigPairResolved,
			Peer: &hint,
		})

		creatorHint := client.toHint()
		s.deliverOrQueue(ctx, hint.DeviceID, protocol.SignalingMessage{
			Type:     protocol.SigPairIncoming,
			SenderID: client.deviceID,
			Name:     client.name,
			Message:  "Appairage par code",
			Peer:     &creatorHint,
		})

	case protocol.SigPairRequest:
		targetID := protocol.NormalizeDeviceID(msg.TargetID)
		if targetID == "" || targetID == client.deviceID {
			return
		}
		senderHint := client.toHint()
		s.deliverOrQueue(ctx, targetID, protocol.SignalingMessage{
			Type:     protocol.SigPairIncoming,
			SenderID: client.deviceID,
			Name:     client.name,
			Message:  msg.Message,
			Peer:     &senderHint,
		})

	case protocol.SigPairAccept:
		targetID := protocol.NormalizeDeviceID(msg.TargetID)
		if targetID == "" {
			return
		}
		acceptorHint := client.toHint()
		s.deliverOrQueue(ctx, targetID, protocol.SignalingMessage{
			Type:     protocol.SigPairAccepted,
			SenderID: client.deviceID,
			Name:     client.name,
			Peer:     &acceptorHint,
		})

	case protocol.SigPairReject:
		targetID := protocol.NormalizeDeviceID(msg.TargetID)
		if targetID == "" {
			return
		}
		s.deliverOrQueue(ctx, targetID, protocol.SignalingMessage{
			Type:     protocol.SigPairRejected,
			SenderID: client.deviceID,
		})

	case protocol.SigQueryPeers:
		s.mu.Lock()
		newWatch := make(map[string]struct{}, len(msg.WatchIDs))
		var hints []protocol.PeerHint
		for _, rawID := range msg.WatchIDs {
			id := protocol.NormalizeDeviceID(rawID)
			if id == "" || id == client.deviceID {
				continue
			}
			newWatch[id] = struct{}{}
			if target, online := s.clients[id]; online {
				hints = append(hints, target.toHint())
			} else {
				hints = append(hints, protocol.PeerHint{
					DeviceID: id,
					Online:   false,
				})
			}
		}
		client.mu.Lock()
		client.watchIDs = newWatch
		client.mu.Unlock()
		s.mu.Unlock()

		_ = client.send(ctx, protocol.SignalingMessage{
			Type:  protocol.SigPeersStatus,
			Peers: hints,
		})

	case protocol.SigPunchRequest:
		targetID := protocol.NormalizeDeviceID(msg.TargetID)
		s.mu.RLock()
		target, ok := s.clients[targetID]
		s.mu.RUnlock()
		if ok {
			requesterHint := client.toHint()
			_ = target.send(ctx, protocol.SignalingMessage{
				Type:     protocol.SigPunchNotify,
				SenderID: client.deviceID,
				Peer:     &requesterHint,
			})
		}

	case protocol.SigBlindRelay:
		if !s.cfg.Snapshot().EnableBlindRelay {
			_ = client.send(ctx, protocol.SignalingMessage{
				Type:  protocol.SigError,
				Error: "blind relay is disabled on this server",
			})
			return
		}
		rawPkt, err := base64.StdEncoding.DecodeString(msg.PayloadB64)
		if err != nil || !s.allowRelayBytes(client, len(rawPkt)) {
			return
		}
		targetID := protocol.NormalizeDeviceID(msg.TargetID)
		s.mu.RLock()
		target, ok := s.clients[targetID]
		s.mu.RUnlock()
		if ok {
			if err := target.send(ctx, protocol.SignalingMessage{
				Type:       protocol.SigBlindRelay,
				SenderID:   client.deviceID,
				PayloadB64: msg.PayloadB64,
			}); err == nil {
				s.relayPacketsTotal.Add(1)
				s.relayBytesTotal.Add(uint64(len(rawPkt)))
				s.wsRelayBytesTotal.Add(uint64(len(rawPkt)))
				client.txBytes.Add(uint64(len(rawPkt)))
				target.rxBytes.Add(uint64(len(rawPkt)))
			}
		}
	}
}

func (s *Server) deliverOrQueue(ctx context.Context, targetID string, msg protocol.SignalingMessage) {
	targetID = protocol.NormalizeDeviceID(targetID)
	now := time.Now()
	s.mu.Lock()
	target, online := s.clients[targetID]
	if !online {
		if _, exists := s.pendingSignaling[targetID]; !exists && len(s.pendingSignaling) >= 1000 {
			s.mu.Unlock()
			return
		}
		q := s.pendingSignaling[targetID]
		fresh := q[:0]
		cutoff := now.Add(-24 * time.Hour)
		for _, item := range q {
			if item.createdAt.After(cutoff) {
				fresh = append(fresh, item)
			}
		}
		if len(fresh) < 64 {
			fresh = append(fresh, pendingEntry{msg: msg, createdAt: now})
		}
		s.pendingSignaling[targetID] = fresh
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	_ = target.send(ctx, msg)
}

func (s *Server) flushPendingSignaling(ctx context.Context, client *clientConn) {
	cutoff := time.Now().Add(-24 * time.Hour)
	s.mu.Lock()
	queued := s.pendingSignaling[client.deviceID]
	delete(s.pendingSignaling, client.deviceID)
	s.mu.Unlock()
	for _, item := range queued {
		if item.createdAt.After(cutoff) {
			_ = client.send(ctx, item.msg)
		}
	}
}

func (s *Server) notifyWatchers(changedID string) {
	s.mu.RLock()
	changedClient, online := s.clients[changedID]
	var hint protocol.PeerHint
	if online {
		hint = changedClient.toHint()
	} else {
		hint = protocol.PeerHint{DeviceID: changedID, Online: false}
	}

	var watchers []*clientConn
	for _, c := range s.clients {
		c.mu.RLock()
		_, watching := c.watchIDs[changedID]
		c.mu.RUnlock()
		if watching {
			watchers = append(watchers, c)
		}
	}
	s.mu.RUnlock()

	for _, w := range watchers {
		_ = w.send(context.Background(), protocol.SignalingMessage{
			Type:  protocol.SigPeersStatus,
			Peers: []protocol.PeerHint{hint},
		})
	}
}

func (s *Server) generatePairCode(hint protocol.PeerHint) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for k, v := range s.pairCodes {
		if now.After(v.expiresAt) {
			delete(s.pairCodes, k)
		}
	}
	if len(s.pairCodes) >= 10000 {
		return "", fmt.Errorf("too many active pair codes")
	}

	const codeAlphabet = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
	alphabetLen := big.NewInt(int64(len(codeAlphabet)))
	for {
		var buf [6]byte
		for i := range buf {
			n, _ := rand.Int(rand.Reader, alphabetLen)
			buf[i] = codeAlphabet[n.Int64()]
		}
		code := fmt.Sprintf("%s-%s", string(buf[:3]), string(buf[3:]))
		if _, exists := s.pairCodes[code]; !exists {
			s.pairCodes[code] = pairEntry{
				hint:      hint,
				expiresAt: now.Add(5 * time.Minute),
			}
			return code, nil
		}
	}
}

func (c *clientConn) toHint() protocol.PeerHint {
	c.mu.RLock()
	obs := c.observedAddr
	c.mu.RUnlock()
	return protocol.PeerHint{
		DeviceID:     c.deviceID,
		Name:         c.name,
		Online:       true,
		LANAddrs:     c.lanAddrs,
		ObservedAddr: obs,
		QUICPort:     c.quicPort,
	}
}

func writeWSJSON(ctx context.Context, ws *websocket.Conn, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	wCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return ws.Write(wCtx, websocket.MessageText, data)
}

func readWSJSON(ctx context.Context, ws *websocket.Conn) (protocol.SignalingMessage, error) {
	var msg protocol.SignalingMessage
	_, data, err := ws.Read(ctx)
	if err != nil {
		return msg, err
	}
	err = json.Unmarshal(data, &msg)
	return msg, err
}

func (s *Server) extractRemoteIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	// Strict TrustProxy: only accept forwarded headers when explicitly enabled
	if s.cfg.Snapshot().TrustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			if len(parts) > 0 {
				if ip := strings.TrimSpace(parts[0]); net.ParseIP(ip) != nil {
					return ip
				}
			}
		}
		if xri := strings.TrimSpace(r.Header.Get("X-Real-IP")); xri != "" && net.ParseIP(xri) != nil {
			return xri
		}
	}
	return host
}
