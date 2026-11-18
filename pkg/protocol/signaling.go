package protocol

// SignalingMessage is the JSON envelope exchanged over WebSocket between daemons and relay-server.
type SignalingMessage struct {
	Type         string     `json:"type"`
	Challenge    string     `json:"challenge,omitempty"`
	DeviceID     string     `json:"device_id,omitempty"`
	Name         string     `json:"name,omitempty"`
	PubKeyB64    string     `json:"pub_key,omitempty"`
	SignatureB64 string     `json:"signature,omitempty"`
	AuthToken    string     `json:"auth_token,omitempty"`
	LANAddrs     []string   `json:"lan_addrs,omitempty"`
	QUICPort     int        `json:"quic_port,omitempty"`
	STUNPort     int        `json:"stun_port,omitempty"`
	ObservedAddr string     `json:"observed_addr,omitempty"`
	PairCode     string     `json:"pair_code,omitempty"`
	ExpiresInSec int        `json:"expires_in_sec,omitempty"`
	TargetID     string     `json:"target_id,omitempty"`
	SenderID     string     `json:"sender_id,omitempty"`
	Message      string     `json:"message,omitempty"`
	WatchIDs     []string   `json:"watch_ids,omitempty"`
	Peers        []PeerHint `json:"peers,omitempty"`
	Peer         *PeerHint  `json:"peer,omitempty"`
	PayloadB64   string     `json:"payload,omitempty"`
	Error        string     `json:"error,omitempty"`
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
