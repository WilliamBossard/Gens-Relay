package protocol

import "bytes"

// SignalingMessage is the JSON envelope exchanged over WebSocket between daemons and relay-server.
type SignalingMessage struct {
	Type          string     `json:"type"`
	Challenge     string     `json:"challenge,omitempty"`
	DeviceID      string     `json:"device_id,omitempty"`
	Name          string     `json:"name,omitempty"`
	PubKeyB64     string     `json:"pub_key,omitempty"`
	SignatureB64  string     `json:"signature,omitempty"`
	AuthProofB64  string     `json:"auth_proof,omitempty"`
	LANAddrs      []string   `json:"lan_addrs,omitempty"`
	QUICPort      int        `json:"quic_port,omitempty"`
	STUNPort      int        `json:"stun_port,omitempty"`
	STUNToken     string     `json:"stun_token,omitempty"`
	UDPRelayPort  int        `json:"udp_relay_port,omitempty"`
	UDPRelayToken string     `json:"udp_relay_token,omitempty"`
	ObservedAddr  string     `json:"observed_addr,omitempty"`
	PairCode      string     `json:"pair_code,omitempty"`
	ExpiresInSec  int        `json:"expires_in_sec,omitempty"`
	TargetID      string     `json:"target_id,omitempty"`
	SenderID      string     `json:"sender_id,omitempty"`
	Message       string     `json:"message,omitempty"`
	WatchIDs      []string   `json:"watch_ids,omitempty"`
	Peers         []PeerHint `json:"peers,omitempty"`
	Peer          *PeerHint  `json:"peer,omitempty"`
	PayloadB64    string     `json:"payload,omitempty"`
	Error         string     `json:"error,omitempty"`
}

// PeerHint describes how to reach a peer (direct LAN preferred, WAN observed fallback).
type PeerHint struct {
	DeviceID     string   `json:"device_id"`
	Name         string   `json:"name"`
	Online       bool     `json:"online"`
	LANAddrs     []string `json:"lan_addrs,omitempty"`
	ObservedAddr string   `json:"observed_addr,omitempty"`
	QUICPort     int      `json:"quic_port,omitempty"`
}

const (
	SigChallenge       = "challenge"
	SigRegister        = "register"
	SigRegistered      = "registered"
	SigCreatePairCode  = "create_pair_code"
	SigPairCodeCreated = "pair_code_created"
	SigRedeemPairCode  = "redeem_pair_code"
	SigPairResolved    = "pair_resolved"
	SigPairRequest     = "pair_request"
	SigPairIncoming    = "pair_incoming"
	SigPairAccept      = "pair_accept"
	SigPairAccepted    = "pair_accepted"
	SigPairReject      = "pair_reject"
	SigPairRejected    = "pair_rejected"
	SigQueryPeers      = "query_peers"
	SigPeersStatus     = "peers_status"
	SigPunchRequest    = "punch_request"
	SigPunchNotify     = "punch_notify"
	SigBlindRelay      = "blind_relay"
	SigError           = "error"
)

// UDPRelayMagic identifies native UDP Blind Relay frames multiplexed on the relay UDP socket.
var UDPRelayMagic = [4]byte{'G', 'R', 'R', 0x01}

const udpRelayHeaderBase = 4 + 16 + 1

// EncodeUDPRelayPacket writes a native UDP Blind Relay packet into dst (reusing capacity if large enough).
// Wire format: [4B magic][16B token][1B idLen][idLen bytes peerID][payload].
// An empty peerID denotes a keepalive/bind probe (or server pong).
func EncodeUDPRelayPacket(dst []byte, token [16]byte, peerID string, payload []byte) []byte {
	normID := NormalizeDeviceID(peerID)
	if len(normID) > 255 {
		normID = normID[:255]
	}
	total := udpRelayHeaderBase + len(normID) + len(payload)
	if cap(dst) < total {
		dst = make([]byte, total)
	} else {
		dst = dst[:total]
	}
	copy(dst[0:4], UDPRelayMagic[:])
	copy(dst[4:20], token[:])
	dst[20] = byte(len(normID))
	copy(dst[21:21+len(normID)], normID)
	copy(dst[21+len(normID):], payload)
	return dst
}

// DecodeUDPRelayPacket parses a native UDP Blind Relay packet.
func DecodeUDPRelayPacket(pkt []byte) (token [16]byte, peerID string, payload []byte, ok bool) {
	if len(pkt) < udpRelayHeaderBase || !bytes.Equal(pkt[:4], UDPRelayMagic[:]) {
		return token, "", nil, false
	}
	copy(token[:], pkt[4:20])
	idLen := int(pkt[20])
	if len(pkt) < udpRelayHeaderBase+idLen {
		return token, "", nil, false
	}
	if idLen > 0 {
		peerID = NormalizeDeviceID(string(pkt[21 : 21+idLen]))
	}
	payload = pkt[21+idLen:]
	return token, peerID, payload, true
}
