package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/WilliamBossard/Gens-Relay/internal/config"
)

type cliConfig struct {
	baseURL string
	user    string
	pass    string
	client  *http.Client
}

func main() {
	dataDir := flag.String("data-dir", config.DefaultDataDir(), "Daemon data directory to auto-detect address")
	baseURL := flag.String("url", "", "Override daemon Web API base URL (e.g. http://127.0.0.1:8384)")
	userFlag := flag.String("user", os.Getenv("GENS_WEB_USER"), "Web UI username if authentication is enabled")
	passFlag := flag.String("pass", os.Getenv("GENS_WEB_PASS"), "Web UI password if authentication is enabled")
	insecureFlag := flag.Bool("insecure", false, "Désactiver la vérification TLS du daemon (non recommandé : par défaut, le certificat web-cert.pem du data-dir est exigé)")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	c, err := buildClient(*dataDir, *baseURL, *userFlag, *passFlag, *insecureFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gens-drop: %v\n", err)
		os.Exit(1)
	}
	cmd := strings.ToLower(args[0])
	rest := args[1:]

	switch cmd {
	case "status":
		err = cmdStatus(c)
	case "peers":
		err = cmdPeers(c)
	case "send":
		err = cmdSend(c, rest)
	case "text":
		err = cmdText(c, rest)
	case "help", "-h", "--help":
		printUsage()
		return
	default:
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "gens-drop: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Gens-Drop CLI — Transferts P2P instantanés via gens-daemon")
	fmt.Println("")
	fmt.Println("Usage:")
	fmt.Println("  gens-drop [flags] status")
	fmt.Println("  gens-drop [flags] peers")
	fmt.Println("  gens-drop [flags] send <peer-name-or-id> <path1> [path2...]")
	fmt.Println("  gens-drop [flags] text <peer-name-or-id> <message...>")
	fmt.Println("")
	fmt.Println("Flags:")
	flag.PrintDefaults()
}

func buildClient(dataDir, overrideURL, user, pass string, insecure bool) (*cliConfig, error) {
	resolvedURL := strings.TrimRight(strings.TrimSpace(overrideURL), "/")
	useTLS := false
	detectedUser := user

	if resolvedURL == "" {
		listenAddr := "127.0.0.1:8384"
		if raw, err := os.ReadFile(filepath.Join(dataDir, "config.json")); err == nil {
			var parsed struct {
				WebListen string `json:"web_listen"`
				WebTLS    bool   `json:"web_tls"`
				WebUser   string `json:"web_user"`
			}
			if json.Unmarshal(raw, &parsed) == nil {
				if parsed.WebListen != "" {
					listenAddr = parsed.WebListen
				}
				useTLS = parsed.WebTLS
				if detectedUser == "" && parsed.WebUser != "" {
					detectedUser = parsed.WebUser
				}
			}
		}
		host, port, err := net.SplitHostPort(listenAddr)
		if err != nil || port == "" {
			host = "127.0.0.1"
			port = "8384"
		}
		if host == "" || host == "0.0.0.0" || host == "::" {
			host = "127.0.0.1"
		}
		scheme := "http"
		if useTLS {
			scheme = "https"
		}
		resolvedURL = fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(host, port))
	} else {
		useTLS = strings.HasPrefix(strings.ToLower(resolvedURL), "https://")
	}

	tr := &http.Transport{}
	if useTLS {
		// The local daemon uses a self-signed certificate stored in dataDir.
		// Pin it (RootCAs) instead of skipping verification entirely — an
		// InsecureSkipVerify client would hand Basic Auth credentials to any
		// MITM on the path.
		caFile := filepath.Join(dataDir, "web-cert.pem")
		certPEM, readErr := os.ReadFile(caFile)
		switch {
		case readErr == nil:
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(certPEM) {
				return nil, fmt.Errorf("certificat %s illisible (PEM invalide)", caFile)
			}
			tr.TLSClientConfig = &tls.Config{RootCAs: pool}
		case insecure:
			fmt.Fprintln(os.Stderr, "gens-drop: --insecure activé — vérification TLS désactivée (non recommandé)")
			tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		default:
			return nil, fmt.Errorf("connexion TLS au daemon impossible : certificat %s introuvable. Copiez web-cert.pem du data-dir du daemon, ou passez --insecure pour désactiver la vérification (non recommandé)", caFile)
		}
	}

	return &cliConfig{
		baseURL: resolvedURL,
		user:    detectedUser,
		pass:    pass,
		client: &http.Client{
			Timeout:   30 * time.Second,
			Transport: tr,
		},
	}, nil
}

func (c *cliConfig) doJSON(method, path string, reqBody any, out any) error {
	var bodyReader io.Reader
	if reqBody != nil {
		b, err := json.Marshal(reqBody)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.baseURL+path, bodyReader)
	if err != nil {
		return err
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.pass != "" {
		u := c.user
		if u == "" {
			u = "admin"
		}
		req.SetBasicAuth(u, c.pass)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("impossible de joindre gens-daemon (%s) : %w", c.baseURL, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		var errObj struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(raw, &errObj) == nil && errObj.Error != "" {
			return fmt.Errorf("HTTP %d: %s", resp.StatusCode, errObj.Error)
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if out != nil && len(raw) > 0 {
		return json.Unmarshal(raw, out)
	}
	return nil
}

type peerInfo struct {
	DeviceID  string `json:"device_id"`
	Name      string `json:"name"`
	Connected bool   `json:"connected"`
	Addr      string `json:"addr"`
	ViaRelay  bool   `json:"via_relay"`
}

func resolvePeerID(c *cliConfig, target string) (string, error) {
	var peers []peerInfo
	if err := c.doJSON(http.MethodGet, "/api/v1/peers", nil, &peers); err != nil {
		return "", err
	}
	needle := strings.TrimSpace(target)
	for _, p := range peers {
		if strings.EqualFold(p.DeviceID, needle) || strings.EqualFold(p.Name, needle) {
			return p.DeviceID, nil
		}
	}
	return "", fmt.Errorf("pair introuvable : %q (utilise 'gens-drop peers' pour lister les appareils)", target)
}

func cmdStatus(c *cliConfig) error {
	var status map[string]any
	if err := c.doJSON(http.MethodGet, "/api/v1/status", nil, &status); err != nil {
		return err
	}
	fmt.Printf("Noeud       : %v (%v)\n", status["node_name"], status["device_id"])
	fmt.Printf("API         : %s\n", c.baseURL)
	fmt.Printf("Relais      : connected=%v active=%v mode=%v\n",
		status["relay_connected"], status["active_relay_url"], status["relay_transport_mode"])

	var drops []struct {
		TransferID string `json:"transfer_id"`
		Direction  string `json:"direction"`
		PeerName   string `json:"peer_name"`
		Title      string `json:"title"`
		Status     string `json:"status"`
		TotalSize  int64  `json:"total_size"`
	}
	if err := c.doJSON(http.MethodGet, "/api/v1/drop", nil, &drops); err == nil {
		fmt.Printf("\nTransferts (%d):\n", len(drops))
		limit := len(drops)
		if limit > 10 {
			limit = 10
		}
		for _, d := range drops[:limit] {
			fmt.Printf("  - [%s] %s (%s -> %s): %s\n", d.TransferID, d.Title, d.Direction, d.PeerName, d.Status)
		}
	}
	return nil
}

func cmdPeers(c *cliConfig) error {
	var peers []peerInfo
	if err := c.doJSON(http.MethodGet, "/api/v1/peers", nil, &peers); err != nil {
		return err
	}
	if len(peers) == 0 {
		fmt.Println("Aucun appareil appairé.")
		return nil
	}
	for _, p := range peers {
		state := "hors-ligne"
		if p.Connected {
			mode := "LAN/Direct"
			if p.ViaRelay {
				mode = "Relais"
			}
			state = fmt.Sprintf("connecté (%s %s)", mode, p.Addr)
		}
		fmt.Printf("%-24s  %-44s  %s\n", p.Name, p.DeviceID, state)
	}
	return nil
}

func cmdSend(c *cliConfig, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: gens-drop send <peer> <path1> [path2...]")
	}
	peerID, err := resolvePeerID(c, args[0])
	if err != nil {
		return err
	}
	absPaths := make([]string, 0, len(args)-1)
	for _, raw := range args[1:] {
		abs, err := filepath.Abs(raw)
		if err != nil {
			return err
		}
		absPaths = append(absPaths, abs)
	}
	var rec struct {
		TransferID string `json:"transfer_id"`
		Title      string `json:"title"`
		Status     string `json:"status"`
		PeerName   string `json:"peer_name"`
	}
	if err := c.doJSON(http.MethodPost, "/api/v1/drop/send", map[string]any{
		"peer_id": peerID,
		"paths":   absPaths,
	}, &rec); err != nil {
		return err
	}
	fmt.Printf("Envoi lancé [%s] vers %s : %s (statut: %s)\n", rec.TransferID, rec.PeerName, rec.Title, rec.Status)
	return nil
}

func cmdText(c *cliConfig, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: gens-drop text <peer> <message...>")
	}
	peerID, err := resolvePeerID(c, args[0])
	if err != nil {
		return err
	}
	msg := strings.Join(args[1:], " ")
	var rec struct {
		TransferID string `json:"transfer_id"`
		Status     string `json:"status"`
		PeerName   string `json:"peer_name"`
	}
	if err := c.doJSON(http.MethodPost, "/api/v1/drop/send-text", map[string]any{
		"peer_id": peerID,
		"text":    msg,
	}, &rec); err != nil {
		return err
	}
	fmt.Printf("Texte envoyé [%s] vers %s (statut: %s)\n", rec.TransferID, rec.PeerName, rec.Status)
	return nil
}
