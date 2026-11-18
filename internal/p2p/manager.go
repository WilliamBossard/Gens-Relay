package p2p

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WilliamBossard/Gens-Relay/internal/config"
	"github.com/WilliamBossard/Gens-Relay/internal/store"
	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
	"github.com/coder/websocket"
	"github.com/quic-go/quic-go"
	"golang.org/x/time/rate"
)

type PeerRuntimeStatus struct {
	DeviceID     string   `json:"device_id"`
	Name         string   `json:"name"`
	Connected    bool     `json:"connected"`
	RelayOnline  bool     `json:"relay_online"`
	Route        string   `json:"route"` // "lan" | "wan" | "offline"
	RemoteAddr   string   `json:"remote_addr,omitempty"`
	LANAddrs     []string `json:"lan_addrs,omitempty"`
	ObservedAddr string   `json:"observed_addr,omitempty"`
}

type Handlers struct {
	OnControlMessage func(ctx context.Context, peerID string, env protocol.ControlEnvelope)
	OnDataStream     func(ctx context.Context, peerID string, kind protocol.DataRequestKind, stream *quic.Stream)
	OnPeerConnected  func(ctx context.Context, peerID string)
	OnPeerState      func()
	OnPairRequests   func()
	OnNearbyPeers    func()
}

type peerSession struct {
	deviceID       string
	conn           *quic.Conn
	ctrlStream     *quic.Stream
	ctrlMu         sync.Mutex
	ctrlMsgCh      chan protocol.ControlEnvelope
	ctrlDone       chan struct{}
	ctrlOnce       sync.Once
	route          string
	remoteAddr     string
	connectedAt    time.Time
	connectedFired atomic.Bool
	activeStreams  atomic.Int32
}

type Manager struct {
	id       *protocol.Identity
	cfg      *config.DaemonConfig
	store    *store.Store
	handlers Handlers
	rootCtx  context.Context

	udpConn       *net.UDPConn
	directTr      *quic.Transport
	relayTr       *quic.Transport
	pktConn       *relayPacketConn
	directListen  *quic.Listener
	relayListen   *quic.Listener
	quicPort      int

	mu             sync.RWMutex
	sessions       map[string]*peerSession
	hints          map[string]protocol.PeerHint
	nearbyLAN      map[string]NearbyPeer
	dialing        map[string]bool
	dialNextAt     map[string]time.Time
	dialFailures   map[string]int
	relayWS        *websocket.Conn
	relayMu        sync.Mutex
	relayWriteMu   sync.Mutex
	relayConn      bool
	forceRelayOnly bool

	pairCodeCh     chan protocol.SignalingMessage
	pairResolvedCh chan protocol.SignalingMessage
	relayRestartCh chan struct{}

	upLimiter   *rate.Limiter
	downLimiter *rate.Limiter
}

func NewManager(id *protocol.Identity, cfg *config.DaemonConfig, st *store.Store, h Handlers) *Manager {
	m := &Manager{
		id:             id,
		cfg:            cfg,
		store:          st,
		handlers:       h,
		rootCtx:        context.Background(),
		sessions:       make(map[string]*peerSession),
		hints:          make(map[string]protocol.PeerHint),
		nearbyLAN:      make(map[string]NearbyPeer),
		dialing:        make(map[string]bool),
		dialNextAt:     make(map[string]time.Time),
		dialFailures:   make(map[string]int),
		pairCodeCh:     make(chan protocol.SignalingMessage, 4),
		pairResolvedCh: make(chan protocol.SignalingMessage, 4),
		relayRestartCh: make(chan struct{}, 1),
	}
	m.RefreshRateLimiters()
	return m
}

func (m *Manager) RefreshRateLimiters() {
	snap := m.cfg.Snapshot()
	m.mu.Lock()
	defer m.mu.Unlock()
	if snap.MaxUploadKBps > 0 {
		bps := rate.Limit(snap.MaxUploadKBps * 1024)
		m.upLimiter = rate.NewLimiter(bps, maxInt(snap.MaxUploadKBps*1024, 64*1024))
	} else {
		m.upLimiter = nil
	}
	if snap.MaxDownloadKBps > 0 {
		bps := rate.Limit(snap.MaxDownloadKBps * 1024)
		m.downLimiter = rate.NewLimiter(bps, maxInt(snap.MaxDownloadKBps*1024, 64*1024))
	} else {
		m.downLimiter = nil
	}
}

func (m *Manager) WaitLimit(ctx context.Context, upload bool, n int) error {
	if n <= 0 {
		return nil
	}
	m.mu.RLock()
	lim := m.downLimiter
	if upload {
		lim = m.upLimiter
	}
	m.mu.RUnlock()
	if lim == nil {
		return nil
	}
	burst := lim.Burst()
	for remaining := n; remaining > 0; {
		take := remaining
		if take > burst {
			take = burst
		}
		if err := lim.WaitN(ctx, take); err != nil {
			return err
		}
		remaining -= take
	}
	return nil
}

// LimitedReadFrom reads up to len(p) from r while enforcing download rate limits in 32 KiB chunks.
func (m *Manager) LimitedReadFull(ctx context.Context, r io.Reader, p []byte) (int, error) {
	const chunk = 32 * 1024
	total := 0
	for total < len(p) {
		end := total + chunk
		if end > len(p) {
			end = len(p)
		}
		if err := m.WaitLimit(ctx, false, end-total); err != nil {
			return total, err
		}
		n, err := io.ReadFull(r, p[total:end])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// LimitedWrite writes p to w while enforcing upload rate limits in 32 KiB chunks.
func (m *Manager) LimitedWrite(ctx context.Context, w io.Writer, p []byte) (int, error) {
	const chunk = 32 * 1024
	total := 0
	for total < len(p) {
		end := total + chunk
		if end > len(p) {
			end = len(p)
		}
		if err := m.WaitLimit(ctx, true, end-total); err != nil {
			return total, err
		}
		n, err := w.Write(p[total:end])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (m *Manager) QUICPort() int {
	return m.quicPort
}

func (m *Manager) RelayConnected() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.relayConn
}

func (m *Manager) Start(ctx context.Context) error {
	m.rootCtx = ctx
	port := m.cfg.Snapshot().QUICPort
	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{Port: port})
	if err != nil {
		udpConn, err = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: port})
	}
	if err != nil && port != 0 {
		log.Printf("[p2p] Configured QUIC UDP port :%d unavailable (%v); falling back to ephemeral port", port, err)
		udpConn, err = net.ListenUDP("udp", &net.UDPAddr{Port: 0})
		if err != nil {
			udpConn, err = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
		}
	}
	if err != nil {
		return fmt.Errorf("listen udp: %w", err)
	}
	_ = udpConn.SetReadBuffer(4 * 1024 * 1024)
	_ = udpConn.SetWriteBuffer(4 * 1024 * 1024)

	m.udpConn = udpConn
	m.quicPort = udpConn.LocalAddr().(*net.UDPAddr).Port
	m.directTr = &quic.Transport{Conn: udpConn}

	m.pktConn = newRelayPacketConn(func() net.Addr { return udpConn.LocalAddr() }, m.sendBlindRelayPacket)
	m.relayTr = &quic.Transport{Conn: m.pktConn}

	tlsConf := m.id.ServerTLSConfig(m.store.IsDeviceAllowed)
	quicConf := &quic.Config{
		KeepAlivePeriod: 10 * time.Second,
		MaxIdleTimeout:  45 * time.Second,
	}

	dLn, err := m.directTr.Listen(tlsConf, quicConf)
	if err != nil {
		_ = m.directTr.Close()
		_ = m.pktConn.Close()
		return fmt.Errorf("quic direct listen: %w", err)
	}
	m.directListen = dLn

	rLn, err := m.relayTr.Listen(tlsConf, &quic.Config{
		KeepAlivePeriod:         10 * time.Second,
		MaxIdleTimeout:          45 * time.Second,
		DisablePathMTUDiscovery: true,
	})
	if err != nil {
		_ = dLn.Close()
		_ = m.directTr.Close()
		_ = m.relayTr.Close()
		return fmt.Errorf("quic relay listen: %w", err)
	}
	m.relayListen = rLn

	log.Printf("[p2p] QUIC listening on UDP :%d (DeviceID: %s)", m.quicPort, m.id.DeviceID)

	go m.acceptLoop(ctx, dLn)
	go m.acceptLoop(ctx, rLn)
	m.runLANDiscovery(ctx)
	go m.runRelayClient(ctx)
	go m.reconnectLoop(ctx)

	go func() {
		<-ctx.Done()
		_ = dLn.Close()
		_ = rLn.Close()
		_ = m.directTr.Close()
		_ = m.relayTr.Close()
		_ = m.pktConn.Close()
		_ = udpConn.Close()
	}()
	return nil
}

func (m *Manager) acceptLoop(ctx context.Context, ln *quic.Listener) {
	for {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return
		}
		go m.handleIncomingConn(ctx, conn)
	}
}

func (m *Manager) handleIncomingConn(ctx context.Context, conn *quic.Conn) {
	state := conn.ConnectionState()
	if len(state.TLS.PeerCertificates) == 0 {
		_ = conn.CloseWithError(1, "missing cert")
		return
	}
	peerID := protocol.DeviceIDFromCert(state.TLS.PeerCertificates[0])
	if !m.store.IsDeviceAllowed(peerID) {
		_ = conn.CloseWithError(2, "unauthorized")
		return
	}
	m.registerSession(ctx, peerID, conn, nil, inferRoute(conn.RemoteAddr()), true)
}

func (m *Manager) registerSession(ctx context.Context, peerID string, conn *quic.Conn, ctrl *quic.Stream, route string, isIncoming bool) {
	peerID = protocol.NormalizeDeviceID(peerID)
	now := time.Now()
	sess := &peerSession{
		deviceID:    peerID,
		conn:        conn,
		ctrlStream:  ctrl,
		ctrlMsgCh:   make(chan protocol.ControlEnvelope, 256),
		ctrlDone:    make(chan struct{}),
		route:       route,
		remoteAddr:  conn.RemoteAddr().String(),
		connectedAt: now,
	}

	var drainingOld *peerSession
	m.mu.Lock()
	delete(m.dialNextAt, peerID)
	delete(m.dialFailures, peerID)
	if old, ok := m.sessions[peerID]; ok && old.conn != conn {
		oldAlive := old.conn.Context().Err() == nil
		if oldAlive && old.route != "relay" && route == "relay" {
			// Keep existing direct LAN/WAN session instead of downgrading to relay
			m.mu.Unlock()
			_ = conn.CloseWithError(0, "prefer direct session")
			return
		}
		if oldAlive && route == old.route && now.Sub(old.connectedAt) < 3*time.Second {
			// Simultaneous dial tie-breaker: smaller DeviceID initiates (outgoing wins on smaller local ID)
			keepNew := isIncoming == (peerID < m.id.DeviceID)
			if !keepNew {
				m.mu.Unlock()
				_ = conn.CloseWithError(0, "simultaneous dial tie-breaker")
				return
			}
		}
		if oldAlive && old.route == "relay" && route != "relay" && old.activeStreams.Load() > 0 {
			// Drain in-flight transfers on the relay connection instead of aborting active streams mid-flight
			drainingOld = old
		} else {
			_ = old.conn.CloseWithError(0, "replaced")
		}
	}
	m.sessions[peerID] = sess
	m.mu.Unlock()

	if drainingOld != nil {
		go func(oldSess *peerSession) {
			deadline := time.Now().Add(60 * time.Second)
			for oldSess.activeStreams.Load() > 0 && time.Now().Before(deadline) {
				if oldSess.conn.Context().Err() != nil {
					return
				}
				time.Sleep(250 * time.Millisecond)
			}
			_ = oldSess.conn.CloseWithError(0, "upgraded to direct")
		}(drainingOld)
	}

	_ = m.store.TouchDeviceSeen(peerID)
	log.Printf("[p2p] Connected to %s via %s (%s)", peerID, route, sess.remoteAddr)

	if m.handlers.OnPeerState != nil {
		m.handlers.OnPeerState()
	}

	go m.startControlWorker(ctx, sess)
	go m.acceptStreamsLoop(ctx, sess)

	if ctrl != nil {
		go m.readControlLoop(ctx, sess, ctrl)
		if m.handlers.OnPeerConnected != nil && sess.connectedFired.CompareAndSwap(false, true) {
			go m.handlers.OnPeerConnected(ctx, peerID)
		}
	}
}

func (m *Manager) startControlWorker(ctx context.Context, sess *peerSession) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-sess.ctrlDone:
			return
		case env := <-sess.ctrlMsgCh:
			if m.handlers.OnControlMessage != nil {
				m.handlers.OnControlMessage(ctx, sess.deviceID, env)
			}
		}
	}
}

func (m *Manager) acceptStreamsLoop(ctx context.Context, sess *peerSession) {
	defer func() {
		sess.ctrlOnce.Do(func() { close(sess.ctrlDone) })
		m.mu.Lock()
		if cur, ok := m.sessions[sess.deviceID]; ok && cur == sess {
			delete(m.sessions, sess.deviceID)
		}
		m.mu.Unlock()
		if m.handlers.OnPeerState != nil {
			m.handlers.OnPeerState()
		}
		log.Printf("[p2p] Disconnected from %s", sess.deviceID)
	}()

	for {
		stream, err := sess.conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go m.dispatchStream(ctx, sess, stream)
	}
}

func (m *Manager) dispatchStream(ctx context.Context, sess *peerSession, stream *quic.Stream) {
	var kindBuf [1]byte
	if _, err := io.ReadFull(stream, kindBuf[:]); err != nil {
		_ = stream.Close()
		return
	}
	switch protocol.StreamKind(kindBuf[0]) {
	case protocol.StreamControl:
		sess.ctrlMu.Lock()
		if sess.ctrlStream == nil {
			sess.ctrlStream = stream
		}
		sess.ctrlMu.Unlock()
		if m.handlers.OnPeerConnected != nil && sess.connectedFired.CompareAndSwap(false, true) {
			go m.handlers.OnPeerConnected(ctx, sess.deviceID)
		}
		m.readControlLoop(ctx, sess, stream)

	case protocol.StreamData:
		sess.activeStreams.Add(1)
		defer sess.activeStreams.Add(-1)
		var reqKind [1]byte
		if _, err := io.ReadFull(stream, reqKind[:]); err != nil {
			_ = stream.Close()
			return
		}
		if m.handlers.OnDataStream != nil {
			m.handlers.OnDataStream(ctx, sess.deviceID, protocol.DataRequestKind(reqKind[0]), stream)
		} else {
			_ = stream.Close()
		}

	default:
		_ = stream.Close()
	}
}

func (m *Manager) readControlLoop(ctx context.Context, sess *peerSession, stream *quic.Stream) {
	for {
		var env protocol.ControlEnvelope
		if err := protocol.ReadFrame(stream, &env); err != nil {
			return
		}
		if env.Type == protocol.CtrlPing {
			_ = m.SendControl(sess.deviceID, protocol.CtrlPong, nil)
			continue
		}
		if m.handlers.OnControlMessage != nil {
			select {
			case sess.ctrlMsgCh <- env:
			case <-sess.ctrlDone:
				return
			case <-ctx.Done():
				return
			}
		}
	}
}

func (m *Manager) SendControl(peerID, msgType string, payload any) error {
	peerID = protocol.NormalizeDeviceID(peerID)
	m.mu.RLock()
	sess, ok := m.sessions[peerID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("peer %s is not connected", peerID)
	}

	sess.ctrlMu.Lock()
	defer sess.ctrlMu.Unlock()

	if sess.ctrlStream == nil {
		openCtx, cancel := context.WithTimeout(m.rootCtx, 5*time.Second)
		stream, err := sess.conn.OpenStreamSync(openCtx)
		cancel()
		if err != nil {
			return err
		}
		if _, err := stream.Write([]byte{byte(protocol.StreamControl)}); err != nil {
			_ = stream.Close()
			return err
		}
		sess.ctrlStream = stream
		go m.readControlLoop(m.rootCtx, sess, stream)
	}
	_ = sess.ctrlStream.SetWriteDeadline(time.Now().Add(5 * time.Second))
	err := protocol.WriteControlEnvelope(sess.ctrlStream, msgType, payload)
	_ = sess.ctrlStream.SetWriteDeadline(time.Time{})
	return err
}

func (m *Manager) OpenDataStream(ctx context.Context, peerID string, reqKind protocol.DataRequestKind) (*quic.Stream, error) {
	stream, release, err := m.OpenTrackedDataStream(ctx, peerID, reqKind)
	if err != nil {
		return nil, err
	}
	// Release immediately if caller doesn't use OpenTrackedDataStream; callers wanting drain safety use OpenTrackedDataStream.
	release()
	return stream, nil
}

func (m *Manager) OpenTrackedDataStream(ctx context.Context, peerID string, reqKind protocol.DataRequestKind) (*quic.Stream, func(), error) {
	peerID = protocol.NormalizeDeviceID(peerID)
	m.mu.RLock()
	sess, ok := m.sessions[peerID]
	m.mu.RUnlock()
	if !ok {
		return nil, nil, fmt.Errorf("peer %s is not connected", peerID)
	}

	sess.activeStreams.Add(1)
	stream, err := sess.conn.OpenStreamSync(ctx)
	if err != nil {
		sess.activeStreams.Add(-1)
		return nil, nil, err
	}
	if _, err := stream.Write([]byte{byte(protocol.StreamData), byte(reqKind)}); err != nil {
		sess.activeStreams.Add(-1)
		_ = stream.Close()
		return nil, nil, err
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			sess.activeStreams.Add(-1)
		})
	}
	return stream, release, nil
}

func (m *Manager) IsConnected(peerID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.sessions[protocol.NormalizeDeviceID(peerID)]
	return ok
}

func (m *Manager) ConnectedPeerIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.sessions))
	for id := range m.sessions {
		out = append(out, id)
	}
	return out
}

func (m *Manager) PeerStatuses() []PeerRuntimeStatus {
	devices, _ := m.store.ListDevices()
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]PeerRuntimeStatus, 0, len(devices))
	for _, dev := range devices {
		st := PeerRuntimeStatus{
			DeviceID: dev.DeviceID,
			Name:     dev.Name,
			Route:    "offline",
		}
		if hint, ok := m.hints[dev.DeviceID]; ok {
			st.RelayOnline = hint.Online
			st.LANAddrs = hint.LANAddrs
			st.ObservedAddr = hint.ObservedAddr
		}
		if sess, ok := m.sessions[dev.DeviceID]; ok {
			st.Connected = true
			st.Route = sess.route
			st.RemoteAddr = sess.remoteAddr
		}
		out = append(out, st)
	}
	return out
}

func (m *Manager) SetPeerEndpoint(peerID, name, endpoint string) {
	if endpoint == "" {
		return
	}
	m.recordLANHint(peerID, name, endpoint)
}

func (m *Manager) recordLANHint(peerID, name, endpoint string) {
	peerID = protocol.NormalizeDeviceID(peerID)
	m.mu.Lock()
	hint := m.hints[peerID]
	hint.DeviceID = peerID
	if name != "" {
		hint.Name = name
	}
	found := false
	for _, a := range hint.LANAddrs {
		if a == endpoint {
			found = true
			break
		}
	}
	if !found {
		hint.LANAddrs = append([]string{endpoint}, hint.LANAddrs...)
	}
	m.hints[peerID] = hint
	delete(m.dialNextAt, peerID)
	delete(m.dialFailures, peerID)
	_, connected := m.sessions[peerID]
	m.mu.Unlock()

	if !connected {
		go m.tryConnectPeer(m.rootCtx, peerID)
	}
}

func (m *Manager) reconnectLoop(ctx context.Context) {
	ticker := time.NewTicker(8 * time.Second)
	defer ticker.Stop()
	var tickCount int

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tickCount++
			// Periodically prune expired LAN nearby peers (>60s)
			if tickCount%4 == 0 {
				m.pruneNearbyLAN()
			}
			devices, err := m.store.ListDevices()
			if err != nil {
				continue
			}
			for _, dev := range devices {
				m.mu.RLock()
				sess, connected := m.sessions[dev.DeviceID]
				isRelay := connected && sess.route == "relay"
				forceRelay := m.forceRelayOnly
				nextDial := m.dialNextAt[dev.DeviceID]
				m.mu.RUnlock()

				if !connected {
					if time.Now().After(nextDial) {
						go m.tryConnectPeer(ctx, dev.DeviceID)
					}
				} else if isRelay && !forceRelay && tickCount%4 == 0 {
					// Periodically attempt upgrading an active relay session to direct LAN/WAN
					go m.tryConnectPeerOpt(ctx, dev.DeviceID, true)
				}
			}
		}
	}
}

func (m *Manager) pruneNearbyLAN() {
	cutoff := time.Now().Add(-60 * time.Second).Unix()
	changed := false
	m.mu.Lock()
	for id, np := range m.nearbyLAN {
		if np.LastSeen < cutoff {
			delete(m.nearbyLAN, id)
			changed = true
		}
	}
	m.mu.Unlock()
	if changed && m.handlers.OnNearbyPeers != nil {
		m.handlers.OnNearbyPeers()
	}
}

func (m *Manager) tryConnectPeer(ctx context.Context, peerID string) {
	m.tryConnectPeerOpt(ctx, peerID, false)
}

func (m *Manager) tryConnectPeerOpt(ctx context.Context, peerID string, directUpgradeOnly bool) {
	peerID = protocol.NormalizeDeviceID(peerID)
	if peerID == "" || peerID == m.id.DeviceID || !m.store.IsDeviceAllowed(peerID) {
		return
	}

	m.mu.Lock()
	sess, isConn := m.sessions[peerID]
	if isConn && (!directUpgradeOnly || sess.route != "relay") {
		m.mu.Unlock()
		return
	}
	if m.dialing[peerID] {
		m.mu.Unlock()
		return
	}
	hint := m.hints[peerID]
	forceRelay := m.forceRelayOnly
	m.dialing[peerID] = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.dialing, peerID)
		m.mu.Unlock()
	}()

	var lanTargets []net.Addr
	var wanTargets []net.Addr
	seenAddr := make(map[string]struct{})
	if !forceRelay {
		for _, addrStr := range hint.LANAddrs {
			if _, dup := seenAddr[addrStr]; dup {
				continue
			}
			seenAddr[addrStr] = struct{}{}
			if udpAddr, err := net.ResolveUDPAddr("udp", addrStr); err == nil {
				lanTargets = append(lanTargets, udpAddr)
			}
		}
		if hint.ObservedAddr != "" {
			if _, dup := seenAddr[hint.ObservedAddr]; !dup {
				if udpAddr, err := net.ResolveUDPAddr("udp", hint.ObservedAddr); err == nil {
					wanTargets = append(wanTargets, udpAddr)
				}
			}
		}
	}

	useRelay := !directUpgradeOnly && m.cfg.Snapshot().EnableRelayFall && m.RelayConnected() && hint.Online
	if len(lanTargets) == 0 && len(wanTargets) == 0 && !useRelay {
		return
	}

	// Happy-Eyeballs style concurrent dialing: LAN first, WAN 150ms later, Relay 650ms later.
	type dialAttempt struct {
		addr  net.Addr
		delay time.Duration
	}
	var attempts []dialAttempt
	for _, a := range lanTargets {
		attempts = append(attempts, dialAttempt{addr: a, delay: 0})
	}
	wanDelay := time.Duration(0)
	if len(lanTargets) > 0 {
		wanDelay = 150 * time.Millisecond
	}
	for _, a := range wanTargets {
		attempts = append(attempts, dialAttempt{addr: a, delay: wanDelay})
	}
	if useRelay {
		relayDelay := time.Duration(0)
		if len(lanTargets)+len(wanTargets) > 0 {
			relayDelay = 650 * time.Millisecond
		}
		attempts = append(attempts, dialAttempt{addr: relayAddr(peerID), delay: relayDelay})
	}

	dialAllCtx, dialAllCancel := context.WithTimeout(ctx, 4500*time.Millisecond)
	defer dialAllCancel()

	type dialResult struct {
		conn   *quic.Conn
		stream *quic.Stream
		route  string
	}
	winCh := make(chan dialResult, 1)
	var wg sync.WaitGroup
	var won atomic.Bool

	for _, att := range attempts {
		wg.Add(1)
		go func(targetAddr net.Addr, delay time.Duration) {
			defer wg.Done()
			if delay > 0 {
				t := time.NewTimer(delay)
				select {
				case <-dialAllCtx.Done():
					t.Stop()
					return
				case <-t.C:
				}
			}
			if won.Load() || dialAllCtx.Err() != nil {
				return
			}
			if udpAddr, ok := targetAddr.(*net.UDPAddr); ok && !isPrivateIP(udpAddr.IP) {
				m.sendRelayPunchRequest(peerID)
				m.sendPunchBurst(dialAllCtx, udpAddr)
			}

			attemptCtx, attemptCancel := context.WithTimeout(dialAllCtx, 3500*time.Millisecond)
			tr := m.directTr
			isRelay := inferRoute(targetAddr) == "relay"
			if isRelay {
				tr = m.relayTr
			}
			conn, err := tr.Dial(attemptCtx, targetAddr, m.id.ClientTLSConfig(peerID), &quic.Config{
				KeepAlivePeriod:         10 * time.Second,
				MaxIdleTimeout:          45 * time.Second,
				DisablePathMTUDiscovery: isRelay,
			})
			attemptCancel()
			if err != nil {
				return
			}

			stream, err := conn.OpenStreamSync(dialAllCtx)
			if err != nil {
				_ = conn.CloseWithError(1, "control open failed")
				return
			}
			if _, err := stream.Write([]byte{byte(protocol.StreamControl)}); err != nil {
				_ = conn.CloseWithError(1, "control header failed")
				return
			}

			if won.CompareAndSwap(false, true) {
				select {
				case winCh <- dialResult{conn: conn, stream: stream, route: inferRoute(conn.RemoteAddr())}:
				default:
					_ = conn.CloseWithError(0, "duplicate dial")
				}
				dialAllCancel()
			} else {
				_ = conn.CloseWithError(0, "duplicate dial")
			}
		}(att.addr, att.delay)
	}

	doneWait := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneWait)
	}()

	select {
	case res := <-winCh:
		m.registerSession(ctx, peerID, res.conn, res.stream, res.route, false)
	case <-doneWait:
		m.recordDialFailure(peerID)
	case <-dialAllCtx.Done():
		m.recordDialFailure(peerID)
	}
}

func (m *Manager) recordDialFailure(peerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, connected := m.sessions[peerID]; connected {
		return
	}
	fails := m.dialFailures[peerID] + 1
	m.dialFailures[peerID] = fails
	backoff := 8 * time.Second
	for i := 1; i < fails && backoff < 64*time.Second; i++ {
		backoff *= 2
	}
	if backoff > 64*time.Second {
		backoff = 64 * time.Second
	}
	m.dialNextAt[peerID] = time.Now().Add(backoff)
}

// --- Relay Signaling Client ---

// RestartRelay interrupts any current relay connection/backoff and reconnects immediately with updated config.
func (m *Manager) RestartRelay() {
	m.relayMu.Lock()
	ws := m.relayWS
	m.relayMu.Unlock()
	if ws != nil {
		_ = ws.Close(websocket.StatusNormalClosure, "config updated")
	}
	select {
	case m.relayRestartCh <- struct{}{}:
	default:
	}
}

func (m *Manager) runRelayClient(ctx context.Context) {
	backoff := 2 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		snap := m.cfg.Snapshot()
		if snap.RelayURL == "" {
			t := time.NewTimer(5 * time.Second)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-m.relayRestartCh:
				t.Stop()
				backoff = 2 * time.Second
				continue
			case <-t.C:
				continue
			}
		}

		connCtx, connCancel := context.WithCancel(ctx)
		doneWatcher := make(chan struct{})
		var restarted atomic.Bool
		go func() {
			select {
			case <-m.relayRestartCh:
				restarted.Store(true)
				connCancel()
			case <-doneWatcher:
			case <-ctx.Done():
			}
		}()

		err := m.connectAndServeRelay(connCtx, snap)
		close(doneWatcher)
		connCancel()
		m.setRelayConnected(false)
		if ctx.Err() != nil {
			return
		}
		if restarted.Load() {
			backoff = 2 * time.Second
			continue
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[relay-client] Disconnected (%v). Reconnecting in %v...", err, backoff)
		}
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-m.relayRestartCh:
			t.Stop()
			backoff = 2 * time.Second
			continue
		case <-t.C:
		}
		backoff *= 2
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
}

func (m *Manager) setRelayConnected(ok bool) {
	m.mu.Lock()
	m.relayConn = ok
	m.mu.Unlock()
	if m.handlers.OnPeerState != nil {
		m.handlers.OnPeerState()
	}
}

func (m *Manager) connectAndServeRelay(ctx context.Context, snap config.Settings) error {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	ws, _, err := websocket.Dial(dialCtx, snap.RelayURL, nil)
	cancel()
	if err != nil {
		return err
	}
	ws.SetReadLimit(256 * 1024)
	defer ws.Close(websocket.StatusNormalClosure, "")

	m.relayMu.Lock()
	m.relayWS = ws
	m.relayMu.Unlock()
	defer func() {
		m.relayMu.Lock()
		if m.relayWS == ws {
			m.relayWS = nil
		}
		m.relayMu.Unlock()
	}()

	// Expect challenge
	chMsg, err := readRelayWS(ctx, ws)
	if err != nil {
		return err
	}
	if chMsg.Type != protocol.SigChallenge {
		return fmt.Errorf("unexpected first message: %s", chMsg.Type)
	}
	nonce, err := base64.StdEncoding.DecodeString(chMsg.Challenge)
	if err != nil {
		return err
	}
	sig := m.id.SignChallenge(nonce)

	if err := m.sendRelayWS(ctx, ws, protocol.SignalingMessage{
		Type:         protocol.SigRegister,
		DeviceID:     m.id.DeviceID,
		Name:         snap.NodeName,
		PubKeyB64:    base64.StdEncoding.EncodeToString(m.id.PubKeyDER),
		SignatureB64: base64.StdEncoding.EncodeToString(sig),
		AuthToken:    snap.AuthToken,
		LANAddrs:     LocalLANAddrs(m.quicPort),
		QUICPort:     m.quicPort,
	}); err != nil {
		return err
	}

	regAck, err := readRelayWS(ctx, ws)
	if err != nil {
		return err
	}
	if regAck.Type == protocol.SigError {
		return errors.New(regAck.Error)
	}
	if regAck.Type != protocol.SigRegistered {
		return fmt.Errorf("expected registered, got %s", regAck.Type)
	}

	m.setRelayConnected(true)
	log.Printf("[relay-client] Registered on %s (observed=%s, stun_port=%d)", snap.RelayURL, regAck.ObservedAddr, regAck.STUNPort)

	// Send initial UDP STUN probe if relay-server exposes UDP STUN
	sendSTUNProbe := func() {
		if regAck.STUNPort <= 0 {
			return
		}
		u, err := url.Parse(snap.RelayURL)
		if err != nil || u.Hostname() == "" {
			return
		}
		if udpAddr, err := net.ResolveUDPAddr("udp4", net.JoinHostPort(u.Hostname(), itoa(regAck.STUNPort))); err == nil {
			_, _ = m.directTr.WriteTo([]byte("GENSSTUN:"+m.id.DeviceID), udpAddr)
		}
	}
	sendSTUNProbe()

	// Query watched peers immediately and every 20s, keepalive ping every 25s, STUN probe every 25s
	_ = m.QueryRelayPeers(ctx)
	wsCtx, wsCancel := context.WithCancel(ctx)
	defer wsCancel()
	go func() {
		queryTicker := time.NewTicker(20 * time.Second)
		pingTicker := time.NewTicker(25 * time.Second)
		stunTicker := time.NewTicker(25 * time.Second)
		defer queryTicker.Stop()
		defer pingTicker.Stop()
		defer stunTicker.Stop()
		for {
			select {
			case <-wsCtx.Done():
				return
			case <-queryTicker.C:
				if err := m.QueryRelayPeers(wsCtx); err != nil {
					return
				}
			case <-pingTicker.C:
				pCtx, pCancel := context.WithTimeout(wsCtx, 5*time.Second)
				pErr := ws.Ping(pCtx)
				pCancel()
				if pErr != nil {
					wsCancel()
					return
				}
			case <-stunTicker.C:
				sendSTUNProbe()
			}
		}
	}()

	for {
		msgType, data, err := ws.Read(wsCtx)
		if err != nil {
			return err
		}
		if msgType == websocket.MessageBinary {
			if len(data) >= 3 && data[0] == 0x01 {
				idLen := int(data[1])
				if idLen > 0 && len(data) >= 2+idLen {
					senderID := protocol.NormalizeDeviceID(string(data[2 : 2+idLen]))
					if m.pktConn != nil && m.store.IsDeviceAllowed(senderID) {
						m.pktConn.DeliverRelayPacket(senderID, data[2+idLen:])
					}
				}
			}
			continue
		}
		var msg protocol.SignalingMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		switch msg.Type {
		case protocol.SigPairCodeCreated:
			select {
			case m.pairCodeCh <- msg:
			default:
			}
		case protocol.SigPairResolved:
			select {
			case m.pairResolvedCh <- msg:
			default:
			}
		case protocol.SigPairIncoming:
			senderID := protocol.NormalizeDeviceID(msg.SenderID)
			var lanAddrs []string
			var obs string
			name := msg.Name
			if msg.Peer != nil {
				lanAddrs = msg.Peer.LANAddrs
				obs = msg.Peer.ObservedAddr
				if name == "" {
					name = msg.Peer.Name
				}
			}
			m.handleIncomingPairRequest(ctx, senderID, name, msg.Message, lanAddrs, obs)
		case protocol.SigPairAccepted:
			senderID := protocol.NormalizeDeviceID(msg.SenderID)
			var lanAddrs []string
			var obs string
			name := msg.Name
			if msg.Peer != nil {
				lanAddrs = msg.Peer.LANAddrs
				obs = msg.Peer.ObservedAddr
				if name == "" {
					name = msg.Peer.Name
				}
			}
			m.handlePairAccepted(ctx, senderID, name, lanAddrs, obs)
		case protocol.SigPairRejected:
			m.handlePairRejected(protocol.NormalizeDeviceID(msg.SenderID))
		case protocol.SigPeersStatus:
			m.mu.Lock()
			for _, p := range msg.Peers {
				pid := protocol.NormalizeDeviceID(p.DeviceID)
				existing := m.hints[pid]
				mergedLAN := mergeUnique(existing.LANAddrs, p.LANAddrs)
				p.DeviceID = pid
				p.LANAddrs = mergedLAN
				m.hints[pid] = p
				if p.Online {
					delete(m.dialNextAt, pid)
					delete(m.dialFailures, pid)
				}
			}
			m.mu.Unlock()
			if m.handlers.OnPeerState != nil {
				m.handlers.OnPeerState()
			}
			for _, p := range msg.Peers {
				if p.Online && !m.IsConnected(p.DeviceID) {
					go m.tryConnectPeer(ctx, p.DeviceID)
				}
			}
		case protocol.SigPunchNotify:
			if msg.Peer != nil && m.store.IsDeviceAllowed(msg.Peer.DeviceID) {
				if msg.Peer.ObservedAddr != "" {
					if udpAddr, err := net.ResolveUDPAddr("udp4", msg.Peer.ObservedAddr); err == nil {
						go m.sendPunchBurst(wsCtx, udpAddr)
					}
				}
			}
		case protocol.SigBlindRelay:
			senderID := protocol.NormalizeDeviceID(msg.SenderID)
			if m.pktConn != nil && m.store.IsDeviceAllowed(senderID) && msg.PayloadB64 != "" {
				if rawPkt, err := base64.StdEncoding.DecodeString(msg.PayloadB64); err == nil {
					m.pktConn.DeliverRelayPacket(senderID, rawPkt)
				}
			}
		case protocol.SigError:
			select {
			case m.pairResolvedCh <- msg:
			default:
			}
		}
	}
}

func (m *Manager) QueryRelayPeers(ctx context.Context) error {
	m.relayMu.Lock()
	ws := m.relayWS
	m.relayMu.Unlock()
	if ws == nil {
		return nil
	}
	devices, err := m.store.ListDevices()
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(devices))
	for _, d := range devices {
		ids = append(ids, d.DeviceID)
	}
	return m.sendRelayWS(ctx, ws, protocol.SignalingMessage{
		Type:     protocol.SigQueryPeers,
		WatchIDs: ids,
	})
}

func (m *Manager) CreatePairCode(ctx context.Context) (string, int, error) {
	m.relayMu.Lock()
	ws := m.relayWS
	m.relayMu.Unlock()
	if ws == nil {
		return "", 0, errors.New("not connected to relay-server (configure RELAY_URL or add DeviceID directly)")
	}
	// Drain stale
	for len(m.pairCodeCh) > 0 {
		<-m.pairCodeCh
	}
	if err := m.sendRelayWS(ctx, ws, protocol.SignalingMessage{Type: protocol.SigCreatePairCode}); err != nil {
		return "", 0, err
	}
	select {
	case <-ctx.Done():
		return "", 0, ctx.Err()
	case <-time.After(5 * time.Second):
		return "", 0, errors.New("timeout waiting for pair code")
	case msg := <-m.pairCodeCh:
		return msg.PairCode, msg.ExpiresInSec, nil
	}
}

func (m *Manager) RedeemPairCode(ctx context.Context, code string) (*protocol.PeerHint, error) {
	m.relayMu.Lock()
	ws := m.relayWS
	m.relayMu.Unlock()
	if ws == nil {
		return nil, errors.New("not connected to relay-server")
	}
	for len(m.pairResolvedCh) > 0 {
		<-m.pairResolvedCh
	}
	if err := m.sendRelayWS(ctx, ws, protocol.SignalingMessage{
		Type:     protocol.SigRedeemPairCode,
		PairCode: code,
	}); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Second):
		return nil, errors.New("timeout waiting for pair resolution")
	case msg := <-m.pairResolvedCh:
		if msg.Type == protocol.SigError {
			return nil, errors.New(msg.Error)
		}
		if msg.Peer == nil {
			return nil, errors.New("empty peer hint")
		}
		return msg.Peer, nil
	}
}

func (m *Manager) sendPunchBurst(ctx context.Context, udpAddr *net.UDPAddr) {
	if m.directTr == nil || udpAddr == nil {
		return
	}
	for i := 0; i < 3; i++ {
		if ctx.Err() != nil {
			return
		}
		_, _ = m.directTr.WriteTo(punchMagic, udpAddr)
		if i < 2 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
}

func (m *Manager) sendRelayPunchRequest(targetID string) {
	m.relayMu.Lock()
	ws := m.relayWS
	m.relayMu.Unlock()
	if ws == nil {
		return
	}
	_ = m.sendRelayWS(m.rootCtx, ws, protocol.SignalingMessage{
		Type:     protocol.SigPunchRequest,
		TargetID: targetID,
	})
}

func (m *Manager) sendBlindRelayPacket(ctx context.Context, targetID string, pkt []byte) error {
	m.relayMu.Lock()
	ws := m.relayWS
	m.relayMu.Unlock()
	if ws == nil {
		return errors.New("relay websocket not connected")
	}
	normID := protocol.NormalizeDeviceID(targetID)
	if len(normID) == 0 || len(normID) > 255 {
		return errors.New("invalid target device id")
	}
	frame := make([]byte, 2+len(normID)+len(pkt))
	frame[0] = 0x01
	frame[1] = byte(len(normID))
	copy(frame[2:], normID)
	copy(frame[2+len(normID):], pkt)

	m.relayWriteMu.Lock()
	defer m.relayWriteMu.Unlock()
	wCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return ws.Write(wCtx, websocket.MessageBinary, frame)
}

func (m *Manager) sendRelayWS(ctx context.Context, ws *websocket.Conn, msg protocol.SignalingMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	m.relayWriteMu.Lock()
	defer m.relayWriteMu.Unlock()
	wCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return ws.Write(wCtx, websocket.MessageText, data)
}

func readRelayWS(ctx context.Context, ws *websocket.Conn) (protocol.SignalingMessage, error) {
	var msg protocol.SignalingMessage
	_, data, err := ws.Read(ctx)
	if err != nil {
		return msg, err
	}
	err = json.Unmarshal(data, &msg)
	return msg, err
}

func inferRoute(addr net.Addr) string {
	if _, ok := addr.(relayAddr); ok {
		return "relay"
	}
	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		return "wan"
	}
	if isPrivateIP(udpAddr.IP) {
		return "lan"
	}
	return "wan"
}

func isPrivateIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast()
}

func mergeUnique(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	var out []string
	for _, s := range append(append([]string{}, a...), b...) {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

func itoa(n int) string {
	return strconv.Itoa(n)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// --- Friend / Device Pairing Requests ---

func (m *Manager) SendPairRequest(ctx context.Context, targetID, nameHint, endpoint, message string) error {
	targetID = protocol.NormalizeDeviceID(targetID)
	if targetID == "" || targetID == m.id.DeviceID {
		return errors.New("invalid target device_id")
	}
	if m.store.IsDeviceAllowed(targetID) {
		if endpoint != "" {
			m.SetPeerEndpoint(targetID, nameHint, endpoint)
		}
		return nil
	}

	// If we already have an incoming request from this device, accept it directly
	if existing, err := m.store.GetPairRequest(targetID); err == nil && existing.Direction == "incoming" {
		return m.AcceptPairRequest(ctx, targetID, nameHint)
	}

	if nameHint == "" {
		nameHint = targetID[:minInt(9, len(targetID))]
	}
	var lanAddrs []string
	if endpoint != "" {
		lanAddrs = []string{endpoint}
	}
	if err := m.store.UpsertPairRequest(store.PairRequestRecord{
		DeviceID:  targetID,
		Direction: "outgoing",
		Name:      nameHint,
		Message:   message,
		Endpoint:  endpoint,
		LANAddrs:  lanAddrs,
	}); err != nil {
		return err
	}

	m.sendLANPairSignal("pair_request", targetID, message, endpoint)
	m.relayMu.Lock()
	ws := m.relayWS
	m.relayMu.Unlock()
	if ws != nil {
		_ = m.sendRelayWS(ctx, ws, protocol.SignalingMessage{
			Type:     protocol.SigPairRequest,
			TargetID: targetID,
			Message:  message,
		})
	}

	if m.handlers.OnPairRequests != nil {
		m.handlers.OnPairRequests()
	}
	return nil
}

func (m *Manager) AcceptPairRequest(ctx context.Context, deviceID, customName string) error {
	deviceID = protocol.NormalizeDeviceID(deviceID)
	req, err := m.store.GetPairRequest(deviceID)
	if err != nil {
		return fmt.Errorf("pair request not found: %w", err)
	}
	name := customName
	if name == "" {
		name = req.Name
	}
	if name == "" {
		name = deviceID[:minInt(9, len(deviceID))]
	}

	if err := m.store.UpsertDevice(store.DeviceRecord{
		DeviceID: deviceID,
		Name:     name,
	}); err != nil {
		return err
	}
	_ = m.store.DeletePairRequest(deviceID)

	m.mu.Lock()
	delete(m.nearbyLAN, deviceID)
	hint := m.hints[deviceID]
	hint.DeviceID = deviceID
	hint.Name = name
	hint.LANAddrs = mergeUnique(hint.LANAddrs, req.LANAddrs)
	if req.Endpoint != "" {
		hint.LANAddrs = mergeUnique(hint.LANAddrs, []string{req.Endpoint})
	}
	if req.ObservedAddr != "" {
		hint.ObservedAddr = req.ObservedAddr
	}
	m.hints[deviceID] = hint
	m.mu.Unlock()

	m.sendLANPairSignal("pair_accept", deviceID, "", req.Endpoint)
	m.relayMu.Lock()
	ws := m.relayWS
	m.relayMu.Unlock()
	if ws != nil {
		_ = m.sendRelayWS(ctx, ws, protocol.SignalingMessage{
			Type:     protocol.SigPairAccept,
			TargetID: deviceID,
		})
	}

	_ = m.QueryRelayPeers(ctx)
	go m.tryConnectPeer(m.rootCtx, deviceID)

	if m.handlers.OnPairRequests != nil {
		m.handlers.OnPairRequests()
	}
	if m.handlers.OnNearbyPeers != nil {
		m.handlers.OnNearbyPeers()
	}
	if m.handlers.OnPeerState != nil {
		m.handlers.OnPeerState()
	}
	return nil
}

func (m *Manager) RejectPairRequest(ctx context.Context, deviceID string) error {
	deviceID = protocol.NormalizeDeviceID(deviceID)
	req, _ := m.store.GetPairRequest(deviceID)
	endpoint := ""
	if req != nil {
		endpoint = req.Endpoint
	}
	_ = m.store.DeletePairRequest(deviceID)

	m.sendLANPairSignal("pair_reject", deviceID, "", endpoint)
	m.relayMu.Lock()
	ws := m.relayWS
	m.relayMu.Unlock()
	if ws != nil {
		_ = m.sendRelayWS(ctx, ws, protocol.SignalingMessage{
			Type:     protocol.SigPairReject,
			TargetID: deviceID,
		})
	}

	if m.handlers.OnPairRequests != nil {
		m.handlers.OnPairRequests()
	}
	return nil
}

func (m *Manager) handleIncomingPairRequest(ctx context.Context, senderID, name, message string, lanAddrs []string, observedAddr string) {
	senderID = protocol.NormalizeDeviceID(senderID)
	if senderID == "" || senderID == m.id.DeviceID {
		return
	}
	if m.store.IsDeviceAllowed(senderID) {
		for _, a := range lanAddrs {
			m.recordLANHint(senderID, name, a)
		}
		return
	}

	// Mutual request: if we already sent an outgoing request to senderID, auto-accept!
	if existing, err := m.store.GetPairRequest(senderID); err == nil && existing.Direction == "outgoing" {
		_ = m.store.UpsertPairRequest(store.PairRequestRecord{
			DeviceID:     senderID,
			Direction:    "incoming",
			Name:         firstNonEmpty(existing.Name, name),
			LANAddrs:     mergeUnique(existing.LANAddrs, lanAddrs),
			ObservedAddr: firstNonEmpty(observedAddr, existing.ObservedAddr),
		})
		_ = m.AcceptPairRequest(ctx, senderID, existing.Name)
		return
	}

	ep := ""
	if len(lanAddrs) > 0 {
		ep = lanAddrs[0]
	}
	_ = m.store.UpsertPairRequest(store.PairRequestRecord{
		DeviceID:     senderID,
		Direction:    "incoming",
		Name:         firstNonEmpty(name, senderID[:minInt(9, len(senderID))]),
		Message:      message,
		Endpoint:     ep,
		LANAddrs:     lanAddrs,
		ObservedAddr: observedAddr,
	})
	if m.handlers.OnPairRequests != nil {
		m.handlers.OnPairRequests()
	}
}

func (m *Manager) handlePairAccepted(ctx context.Context, senderID, name string, lanAddrs []string, observedAddr string) {
	senderID = protocol.NormalizeDeviceID(senderID)
	if senderID == "" || senderID == m.id.DeviceID {
		return
	}
	req, err := m.store.GetPairRequest(senderID)
	if err != nil && !m.store.IsDeviceAllowed(senderID) {
		return
	}

	finalName := name
	var mergedLAN []string
	obs := observedAddr
	if req != nil {
		if req.Name != "" && !strings.HasPrefix(senderID, req.Name) {
			finalName = req.Name
		}
		mergedLAN = mergeUnique(req.LANAddrs, lanAddrs)
		if obs == "" {
			obs = req.ObservedAddr
		}
	} else {
		mergedLAN = lanAddrs
	}
	if finalName == "" {
		finalName = senderID[:minInt(9, len(senderID))]
	}

	_ = m.store.UpsertDevice(store.DeviceRecord{
		DeviceID: senderID,
		Name:     finalName,
	})
	_ = m.store.DeletePairRequest(senderID)

	m.mu.Lock()
	delete(m.nearbyLAN, senderID)
	hint := m.hints[senderID]
	hint.DeviceID = senderID
	hint.Name = finalName
	hint.LANAddrs = mergeUnique(hint.LANAddrs, mergedLAN)
	if obs != "" {
		hint.ObservedAddr = obs
	}
	m.hints[senderID] = hint
	m.mu.Unlock()

	_ = m.QueryRelayPeers(ctx)
	go m.tryConnectPeer(m.rootCtx, senderID)

	if m.handlers.OnPairRequests != nil {
		m.handlers.OnPairRequests()
	}
	if m.handlers.OnNearbyPeers != nil {
		m.handlers.OnNearbyPeers()
	}
	if m.handlers.OnPeerState != nil {
		m.handlers.OnPeerState()
	}
}

func (m *Manager) handlePairRejected(senderID string) {
	senderID = protocol.NormalizeDeviceID(senderID)
	if senderID == "" {
		return
	}
	_ = m.store.DeletePairRequest(senderID)
	if m.handlers.OnPairRequests != nil {
		m.handlers.OnPairRequests()
	}
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
