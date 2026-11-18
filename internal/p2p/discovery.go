package p2p

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
)

type lanAnnouncement struct {
	Kind         string `json:"kind,omitempty"` // ""|"announce"|"pair_request"|"pair_accept"|"pair_reject"
	DeviceID     string `json:"device_id"`
	TargetID     string `json:"target_id,omitempty"`
	Name         string `json:"name"`
	QUICPort     int    `json:"quic_port"`
	Message      string `json:"message,omitempty"`
	Timestamp    int64  `json:"ts,omitempty"`
	PubKeyB64    string `json:"pub_key,omitempty"`
	SignatureB64 string `json:"sig,omitempty"`
}

func (m *Manager) signLANAnnouncement(ann lanAnnouncement) []byte {
	ann.Timestamp = time.Now().Unix()
	ann.PubKeyB64 = base64.StdEncoding.EncodeToString(m.id.PubKeyDER)
	signPayload := lanSignBytes(ann)
	ann.SignatureB64 = base64.StdEncoding.EncodeToString(m.id.SignChallenge(signPayload))
	b, _ := json.Marshal(ann)
	return b
}

func lanSignBytes(ann lanAnnouncement) []byte {
	return []byte(fmt.Sprintf("%s|%s|%s|%d|%s|%d",
		ann.Kind,
		protocol.NormalizeDeviceID(ann.DeviceID),
		protocol.NormalizeDeviceID(ann.TargetID),
		ann.QUICPort,
		ann.Message,
		ann.Timestamp,
	))
}

func verifyLANAnnouncement(ann lanAnnouncement) bool {
	pubDER, err := base64.StdEncoding.DecodeString(ann.PubKeyB64)
	if err != nil {
		return false
	}
	sig, err := base64.StdEncoding.DecodeString(ann.SignatureB64)
	if err != nil {
		return false
	}
	if err := protocol.VerifyChallenge(ann.DeviceID, pubDER, lanSignBytes(ann), sig); err != nil {
		return false
	}
	if ann.Timestamp <= 0 {
		return false
	}
	diff := time.Now().Unix() - ann.Timestamp
	if diff < -120 || diff > 120 {
		return false
	}
	return true
}

type NearbyPeer struct {
	DeviceID string `json:"device_id"`
	Name     string `json:"name"`
	Endpoint string `json:"endpoint"`
	LastSeen int64  `json:"last_seen"`
}

// LocalLANAddrs returns non-loopback IPv4 interface addresses formatted as ip:quicPort.
func LocalLANAddrs(quicPort int) []string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var out []string
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			ip4 := ipNet.IP.To4()
			if ip4 == nil || ip4.IsLoopback() || ip4.IsLinkLocalUnicast() {
				continue
			}
			if quicPort > 0 {
				out = append(out, net.JoinHostPort(ip4.String(), itoa(quicPort)))
			} else {
				out = append(out, ip4.String())
			}
		}
	}
	return out
}

func (m *Manager) NearbyPeers() []NearbyPeer {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cutoff := time.Now().Add(-30 * time.Second).Unix()
	out := make([]NearbyPeer, 0, len(m.nearbyLAN))
	for id, np := range m.nearbyLAN {
		if np.LastSeen < cutoff {
			continue
		}
		if m.store.IsDeviceAllowed(id) {
			continue
		}
		out = append(out, np)
	}
	return out
}

func (m *Manager) sendLANPairSignal(kind, targetID, message, endpoint string) {
	if !m.cfg.Snapshot().EnableLANDisc || m.cfg.Snapshot().DiscoveryPort <= 0 {
		return
	}
	port := m.cfg.Snapshot().DiscoveryPort
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return
	}
	defer conn.Close()

	payload := m.signLANAnnouncement(lanAnnouncement{
		Kind:     kind,
		DeviceID: m.id.DeviceID,
		TargetID: protocol.NormalizeDeviceID(targetID),
		Name:     m.cfg.Snapshot().NodeName,
		QUICPort: m.quicPort,
		Message:  message,
	})

	targets := collectBroadcastAddrs(port)
	if endpoint != "" {
		if host, _, err := net.SplitHostPort(endpoint); err == nil {
			if ip := net.ParseIP(host); ip != nil {
				targets = append(targets, &net.UDPAddr{IP: ip, Port: port})
			}
		} else if ip := net.ParseIP(endpoint); ip != nil {
			targets = append(targets, &net.UDPAddr{IP: ip, Port: port})
		}
	}
	for _, dst := range targets {
		_, _ = conn.WriteToUDP(payload, dst)
	}
}

func (m *Manager) runLANDiscovery(ctx context.Context) {
	if !m.cfg.Snapshot().EnableLANDisc || m.cfg.Snapshot().DiscoveryPort <= 0 {
		return
	}
	port := m.cfg.Snapshot().DiscoveryPort

	// Listener (auto-recovers on transient network interface / sleep-wake errors)
	go func() {
		addr := &net.UDPAddr{IP: net.IPv4zero, Port: port}
		buf := make([]byte, 4096)
		warned := false
		for {
			if ctx.Err() != nil {
				return
			}
			conn, err := net.ListenUDP("udp4", addr)
			if err != nil {
				if !warned {
					warned = true
					log.Printf("[p2p] LAN discovery UDP :%d unavailable (%v); retrying in background (change discovery_port if Syncthing is running)", port, err)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(3 * time.Second):
					continue
				}
			}
			warned = false
			stopClose := context.AfterFunc(ctx, func() {
				_ = conn.Close()
			})

			for {
				n, remoteAddr, err := conn.ReadFromUDP(buf)
				if err != nil {
					break
				}
				var ann lanAnnouncement
				if err := json.Unmarshal(buf[:n], &ann); err != nil {
					continue
				}
				peerID := protocol.NormalizeDeviceID(ann.DeviceID)
				if peerID == "" || peerID == m.id.DeviceID || ann.QUICPort <= 0 {
					continue
				}
				if !verifyLANAnnouncement(ann) {
					continue
				}
				endpoint := net.JoinHostPort(remoteAddr.IP.String(), itoa(ann.QUICPort))

				switch ann.Kind {
				case "pair_request":
					if protocol.NormalizeDeviceID(ann.TargetID) == m.id.DeviceID {
						m.handleIncomingPairRequest(ctx, peerID, ann.Name, ann.Message, []string{endpoint}, "")
					}
					continue
				case "pair_accept":
					if protocol.NormalizeDeviceID(ann.TargetID) == m.id.DeviceID {
						m.handlePairAccepted(ctx, peerID, ann.Name, []string{endpoint}, "")
					}
					continue
				case "pair_reject":
					if protocol.NormalizeDeviceID(ann.TargetID) == m.id.DeviceID {
						m.handlePairRejected(peerID)
					}
					continue
				}

				if m.store.IsDeviceAllowed(peerID) {
					m.recordLANHint(peerID, ann.Name, endpoint)
					continue
				}

				// Record unpaired nearby LAN device so user can send a friend request in 1 click
				m.mu.Lock()
				_, existed := m.nearbyLAN[peerID]
				m.nearbyLAN[peerID] = NearbyPeer{
					DeviceID: peerID,
					Name:     ann.Name,
					Endpoint: endpoint,
					LastSeen: time.Now().Unix(),
				}
				m.mu.Unlock()
				if !existed && m.handlers.OnNearbyPeers != nil {
					m.handlers.OnNearbyPeers()
				}
			}
			stopClose()
			_ = conn.Close()
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}()

	// Broadcaster
	go func() {
		conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
		if err != nil {
			return
		}
		defer conn.Close()

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		sendOnce := func() {
			snap := m.cfg.Snapshot()
			bcastTargets := collectBroadcastAddrs(port)
			payload := m.signLANAnnouncement(lanAnnouncement{
				Kind:     "announce",
				DeviceID: m.id.DeviceID,
				Name:     snap.NodeName,
				QUICPort: m.quicPort,
			})
			for _, dst := range bcastTargets {
				_, _ = conn.WriteToUDP(payload, dst)
			}
		}

		sendOnce()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sendOnce()
			}
		}
	}()
}

func collectBroadcastAddrs(port int) []*net.UDPAddr {
	targets := []*net.UDPAddr{
		{IP: net.IPv4bcast, Port: port},
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return targets
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagBroadcast == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			ip4 := ipNet.IP.To4()
			if ip4 == nil || len(ipNet.Mask) != 4 {
				continue
			}
			bcast := make(net.IP, 4)
			for i := 0; i < 4; i++ {
				bcast[i] = ip4[i] | ^ipNet.Mask[i]
			}
			targets = append(targets, &net.UDPAddr{IP: bcast, Port: port})
		}
	}
	return targets
}
