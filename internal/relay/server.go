package relay

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
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
	typ  websocket.MessageType
	data []byte
}

type clientConn struct {
	deviceID      string
	name          string
	lanAddrs      []string
	quicPort      int
	observedIP    string
	observedAddr  string
	connectedAt   int64
	watchIDs      map[string]struct{}
	ws            *websocket.Conn
	sendCh        chan wsOutFrame
	closed        atomic.Bool
	rxBytes       atomic.Uint64
	txBytes       atomic.Uint64
	redeemLimiter *rate.Limiter
	relayLimiter  *rate.Limiter
	relayLimVer   uint64
	mu            sync.RWMutex
}

func newClientConn(deviceID, name string, lanAddrs []string, quicPort int, observedIP, observedAddr string, ws *websocket.Conn) *clientConn {
	return &clientConn{
		deviceID:      deviceID,
		name:          name,
		lanAddrs:      lanAddrs,
		quicPort:      quicPort,
		observedIP:    observedIP,
		observedAddr:  observedAddr,
		connectedAt:   time.Now().Unix(),
		watchIDs:      make(map[string]struct{}),
		ws:            ws,
		sendCh:        make(chan wsOutFrame, 256),
		redeemLimiter: rate.NewLimiter(rate.Every(12*time.Second), 5),
	}
}

func (c *clientConn) startWriter(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case frame, ok := <-c.sendCh:
				if !ok {
					return
				}
				wCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := c.ws.Write(wCtx, frame.typ, frame.data)
				cancel()
				if err != nil {
					_ = c.ws.CloseNow()
					return
				}
			}
		}
	}()
}

func (c *clientConn) enqueue(typ websocket.MessageType, data []byte) error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	select {
	case c.sendCh <- wsOutFrame{typ: typ, data: data}:
		return nil
	default:
		// Backpressure: drop packet on saturated slow peer without blocking sender
		return fmt.Errorf("client send queue saturated")
	}
}

func (c *clientConn) send(_ context.Context, msg protocol.SignalingMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return c.enqueue(websocket.MessageText, data)
}

func (c *clientConn) sendBinary(_ context.Context, data []byte) error {
	return c.enqueue(websocket.MessageBinary, data)
}

// Server implements the zero-file-transit signaling & discovery server.
type Server struct {
	opts      Options
	cfg       *RelayConfig
	logs      *logs.RingBuffer
	startedAt time.Time
	stunPort  int32

	mu               sync.RWMutex
	clients          map[string]*clientConn
	pairCodes        map[string]pairEntry
	pendingSignaling map[string][]pendingEntry

	limiterMu    sync.RWMutex
	relayLimiter *rate.Limiter
	relayLimKBps int
	relayLimVer  atomic.Uint64

	ipLimMu     sync.Mutex
	ipLimiters  map[string]*ipRateEntry

	adminWsMu      sync.Mutex
	adminWsClients map[*adminWsSub]struct{}
	adminEventCh   chan []byte

	relayPacketsTotal atomic.Uint64
	relayBytesTotal   atomic.Uint64
	relayBps          atomic.Int64
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
	s := &Server{
		opts:             opts,
		cfg:              cfg,
		logs:             opts.Logs,
		startedAt:        time.Now(),
		clients:          make(map[string]*clientConn),
		pairCodes:        make(map[string]pairEntry),
		pendingSignaling: make(map[string][]pendingEntry),
		ipLimiters:       make(map[string]*ipRateEntry),
		adminWsClients:   make(map[*adminWsSub]struct{}),
		adminEventCh:     make(chan []byte, 256),
	}
	s.refreshLimiter()
	go s.adminBroadcastLoop()
	return s
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
	s.limiterMu.RUnlock()

	if kbps <= 0 {
		return true
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

	if n > lim.Burst() {
		n = lim.Burst()
	}
	return lim.AllowN(time.Now(), n)
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
				s.broadcastAdminEvent("status_updated", s.StatusSnapshot())
			case <-pruneTicker.C:
				s.pruneExpired()
			}
		}
	}()
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
}

// StartUDP launches a lightweight UDP STUN/echo listener so peers behind NAT can discover their mapped public UDP port.
func (s *Server) StartUDP(ctx context.Context, addr string) error {
	udpAddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		return err
	}
	port := conn.LocalAddr().(*net.UDPAddr).Port
	atomic.StoreInt32(&s.stunPort, int32(port))

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
	go func() {
		buf := make([]byte, 512)
		for {
			n, remote, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			line := strings.TrimSpace(string(buf[:n]))
			if strings.HasPrefix(line, "GENSSTUN:") {
				devID := protocol.NormalizeDeviceID(strings.TrimPrefix(line, "GENSSTUN:"))
				remoteStr := remote.String()
				remoteIP := remote.IP.String()
				updated := false
				s.mu.RLock()
				c, ok := s.clients[devID]
				s.mu.RUnlock()
				if ok {
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
				}
				_, _ = conn.WriteToUDP([]byte("GENSSTUN_OK:"+remoteStr), remote)
			}
		}
	}()
	return nil
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
		"gens_relay_online_devices %d\ngens_relay_active_pair_codes %d\ngens_relay_pending_queues %d\ngens_relay_blind_packets_total %d\ngens_relay_blind_bytes_total %d\n",
		onlineCount, pairCodeCount, pendingCount, s.relayPacketsTotal.Load(), s.relayBytesTotal.Load(),
	)
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	if !s.allowIPConnect(s.extractRemoteIP(r)) {
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

	// 1. Send random authentication challenge
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

	// 2. Expect register message within 10s auth timeout
	authCtx, authCancel := context.WithTimeout(ctx, 10*time.Second)
	regMsg, err := readWSJSON(authCtx, ws)
	authCancel()
	if err != nil || regMsg.Type != protocol.SigRegister {
		_ = writeWSJSON(ctx, ws, protocol.SignalingMessage{Type: protocol.SigError, Error: "expected register message"})
		return
	}

	snap := s.cfg.Snapshot()
	if snap.AuthToken != "" {
		if subtle.ConstantTimeCompare([]byte(regMsg.AuthToken), []byte(snap.AuthToken)) != 1 {
			_ = writeWSJSON(ctx, ws, protocol.SignalingMessage{Type: protocol.SigError, Error: "invalid auth_token"})
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
		log.Printf("[relay-server] Rejected banned device: %s", deviceID)
		_ = writeWSJSON(ctx, ws, protocol.SignalingMessage{Type: protocol.SigError, Error: "device is banned"})
		return
	}

	observedIP := s.extractRemoteIP(r)
	observedAddr := ""
	if observedIP != "" && regMsg.QUICPort > 0 {
		observedAddr = net.JoinHostPort(observedIP, fmt.Sprintf("%d", regMsg.QUICPort))
	} else {
		observedAddr = observedIP
	}

	client := newClientConn(deviceID, regMsg.Name, regMsg.LANAddrs, regMsg.QUICPort, observedIP, observedAddr, ws)

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
		_ = old.ws.Close(websocket.StatusPolicyViolation, "replaced by new session")
	}
	s.clients[deviceID] = client
	s.mu.Unlock()

	client.startWriter(ctx)
	log.Printf("[relay-server] Device connected: %s (%s) observed=%s", deviceID, client.name, observedAddr)
	s.broadcastAdminClients()

	defer func() {
		client.closed.Store(true)
		s.mu.Lock()
		if cur, ok := s.clients[deviceID]; ok && cur == client {
			delete(s.clients, deviceID)
		}
		s.mu.Unlock()
		s.notifyWatchers(deviceID)
		s.broadcastAdminClients()
		log.Printf("[relay-server] Device disconnected: %s", deviceID)
	}()

	_ = client.send(ctx, protocol.SignalingMessage{
		Type:         protocol.SigRegistered,
		DeviceID:     deviceID,
		ObservedAddr: observedAddr,
		STUNPort:     int(atomic.LoadInt32(&s.stunPort)),
	})
	s.flushPendingSignaling(ctx, client)
	s.notifyWatchers(deviceID)

	for {
		msgType, data, err := ws.Read(ctx)
		if err != nil {
			return
		}
		if msgType == websocket.MessageBinary {
			s.handleBinaryRelay(ctx, client, data)
			continue
		}
		var msg protocol.SignalingMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		s.handleClientMessage(ctx, client, msg)
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

	senderBytes := []byte(client.deviceID)
	if len(senderBytes) > 255 {
		return
	}
	out := make([]byte, 2+len(senderBytes)+len(payload))
	out[0] = 0x01
	out[1] = byte(len(senderBytes))
	copy(out[2:], senderBytes)
	copy(out[2+len(senderBytes):], payload)

	if err := target.sendBinary(ctx, out); err == nil {
		s.relayPacketsTotal.Add(1)
		s.relayBytesTotal.Add(uint64(len(payload)))
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

		// Refresh live hint if creator is still online
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

		// Also notify the pair code creator so both sides add/approve each other
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
		// Prune expired entries (>24h) opportunistically
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
	remoteIP := net.ParseIP(host)
	isTrusted := s.cfg.Snapshot().TrustProxy || (remoteIP != nil && (remoteIP.IsLoopback() || remoteIP.IsPrivate()))
	if isTrusted {
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
