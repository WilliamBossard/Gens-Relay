package protocol

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base32"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const ALPNProtocol = "gens-relay/1"

// Identity holds the persistent Ed25519 TLS identity and derived DeviceID of a node.
type Identity struct {
	DeviceID   string
	Cert       tls.Certificate
	Leaf       *x509.Certificate
	PublicKey  ed25519.PublicKey
	PrivateKey ed25519.PrivateKey
	PubKeyDER  []byte
}

// DeviceIDFromPubKeyDER derives a deterministic human-readable DeviceID from a PKIX public key.
func DeviceIDFromPubKeyDER(pubDER []byte) string {
	sum := sha256.Sum256(pubDER)
	enc := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:20]) // 32 chars
	var groups []string
	for i := 0; i < len(enc); i += 4 {
		end := i + 4
		if end > len(enc) {
			end = len(enc)
		}
		groups = append(groups, enc[i:end])
	}
	return strings.Join(groups, "-")
}

// DeviceIDFromCert computes the DeviceID from an X.509 certificate.
func DeviceIDFromCert(cert *x509.Certificate) string {
	return DeviceIDFromPubKeyDER(cert.RawSubjectPublicKeyInfo)
}

// NormalizeDeviceID canonicalizes a user-entered DeviceID.
func NormalizeDeviceID(id string) string {
	clean := strings.ToUpper(strings.TrimSpace(id))
	clean = strings.ReplaceAll(clean, " ", "")
	return clean
}

// LoadOrCreateIdentity loads cert.pem/key.pem from dataDir or generates a new Ed25519 identity.
func LoadOrCreateIdentity(dataDir string) (*Identity, error) {
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("create dataDir: %w", err)
	}

	certPath := filepath.Join(dataDir, "cert.pem")
	keyPath := filepath.Join(dataDir, "key.pem")

	if _, err := os.Stat(certPath); os.IsNotExist(err) {
		if err := generateAndSaveCert(certPath, keyPath); err != nil {
			return nil, err
		}
	}

	tlsCert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load keypair: %w", err)
	}
	if len(tlsCert.Certificate) == 0 {
		return nil, errors.New("empty certificate chain")
	}

	leaf, err := x509.ParseCertificate(tlsCert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("parse leaf cert: %w", err)
	}
	tlsCert.Leaf = leaf

	priv, ok := tlsCert.PrivateKey.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("private key is not Ed25519")
	}
	pub, ok := leaf.PublicKey.(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("public key is not Ed25519")
	}

	return &Identity{
		DeviceID:   DeviceIDFromCert(leaf),
		Cert:       tlsCert,
		Leaf:       leaf,
		PublicKey:  pub,
		PrivateKey: priv,
		PubKeyDER:  leaf.RawSubjectPublicKeyInfo,
	}, nil
}

func generateAndSaveCert(certPath, keyPath string) error {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("generate ed25519 key: %w", err)
	}

	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return fmt.Errorf("generate serial: %w", err)
	}

	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "gens-relay-node",
			Organization: []string{"Gens-Relay"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(20 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, pub, priv)
	if err != nil {
		return fmt.Errorf("create certificate: %w", err)
	}

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return fmt.Errorf("marshal private key: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})

	if err := os.WriteFile(certPath, certPEM, 0644); err != nil {
		return fmt.Errorf("write cert.pem: %w", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		return fmt.Errorf("write key.pem: %w", err)
	}
	return nil
}

// ServerTLSConfig builds a TLS 1.3 mTLS config for listening QUIC peers.
func (id *Identity) ServerTLSConfig(isAllowed func(deviceID string) bool) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{id.Cert},
		ClientAuth:   tls.RequireAnyClientCert,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{ALPNProtocol},
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("missing peer certificate")
			}
			cert, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return fmt.Errorf("parse peer cert: %w", err)
			}
			peerID := DeviceIDFromCert(cert)
			if peerID == id.DeviceID {
				return errors.New("self-connection rejected")
			}
			if isAllowed != nil && !isAllowed(peerID) {
				return fmt.Errorf("unauthorized peer device: %s", peerID)
			}
			return nil
		},
	}
}

// ClientTLSConfig builds a TLS 1.3 mTLS config for dialing a specific expected peer.
func (id *Identity) ClientTLSConfig(expectedDeviceID string) *tls.Config {
	expected := NormalizeDeviceID(expectedDeviceID)
	return &tls.Config{
		Certificates:       []tls.Certificate{id.Cert},
		InsecureSkipVerify: true, // Verified via VerifyPeerCertificate pinning DeviceID
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{ALPNProtocol},
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("missing server peer certificate")
			}
			cert, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return fmt.Errorf("parse peer cert: %w", err)
			}
			peerID := DeviceIDFromCert(cert)
			if expected != "" && peerID != expected {
				return fmt.Errorf("device ID mismatch: got %s, want %s", peerID, expected)
			}
			return nil
		},
	}
}

// SignChallenge signs an authentication challenge with the node's Ed25519 private key.
func (id *Identity) SignChallenge(challenge []byte) []byte {
	return ed25519.Sign(id.PrivateKey, challenge)
}

// VerifyChallenge checks a challenge signature against a PKIX DER public key and DeviceID.
func VerifyChallenge(deviceID string, pubKeyDER, challenge, sig []byte) error {
	if NormalizeDeviceID(deviceID) != DeviceIDFromPubKeyDER(pubKeyDER) {
		return errors.New("device ID does not match public key")
	}
	parsed, err := x509.ParsePKIXPublicKey(pubKeyDER)
	if err != nil {
		return fmt.Errorf("parse public key: %w", err)
	}
	pub, ok := parsed.(ed25519.PublicKey)
	if !ok {
		return errors.New("public key is not Ed25519")
	}
	if !ed25519.Verify(pub, challenge, sig) {
		return errors.New("invalid Ed25519 signature")
	}
	return nil
}
