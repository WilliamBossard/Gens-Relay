package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

type Settings struct {
	NodeName        string `json:"node_name"`
	RelayURL        string `json:"relay_url"`
	AuthToken       string `json:"auth_token"`
	QUICPort        int    `json:"quic_port"`
	DiscoveryPort   int    `json:"discovery_port"`
	EnableLANDisc   bool   `json:"enable_lan_discovery"`
	EnableRelayFall bool   `json:"enable_relay_fallback"`
	WebListen       string `json:"web_listen"`
	WebTLS          bool   `json:"web_tls"`
	WebUser         string `json:"web_user"`
	WebPassHash     string `json:"web_pass_hash,omitempty"`
	DropInboxDir    string `json:"drop_inbox_dir"`
	MaxUploadKBps   int    `json:"max_upload_kbps"`   // 0 = unlimited
	MaxDownloadKBps int    `json:"max_download_kbps"` // 0 = unlimited
}

type DaemonConfig struct {
	mu sync.RWMutex
	Settings
	filePath      string
	authCacheMu   sync.RWMutex
	authCacheKey  string
	authCacheHash string
	authCacheExp  time.Time
}

func DefaultDataDir() string {
	if env := os.Getenv("GENS_DATA_DIR"); env != "" {
		return env
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return ".gens-relay"
	}
	return filepath.Join(base, "Gens-Relay")
}

func DefaultDropInbox(dataDir string) string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, "Downloads", "Gens-Relay")
	}
	return filepath.Join(dataDir, "inbox")
}

func LoadOrCreateDaemonConfig(dataDir string) (*DaemonConfig, error) {
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, err
	}
	cfgPath := filepath.Join(dataDir, "config.json")

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "Gens-Node"
	}

	cfg := &DaemonConfig{
		Settings: Settings{
			NodeName:        hostname,
			RelayURL:        envOr("RELAY_URL", ""),
			AuthToken:       envOr("AUTH_TOKEN", ""),
			QUICPort:        22000,
			DiscoveryPort:   21027,
			EnableLANDisc:   true,
			EnableRelayFall: true,
			WebListen:       envOr("GENS_WEB_LISTEN", "127.0.0.1:8385"),
			DropInboxDir:    DefaultDropInbox(dataDir),
		},
		filePath: cfgPath,
	}

	if data, err := os.ReadFile(cfgPath); err == nil {
		if uErr := json.Unmarshal(data, &cfg.Settings); uErr != nil {
			backupPath := fmt.Sprintf("%s.corrupt.%d", cfgPath, time.Now().Unix())
			_ = os.WriteFile(backupPath, data, 0600)
			return nil, fmt.Errorf("parse %s (backup saved to %s): %w", cfgPath, backupPath, uErr)
		}
	}

	// Environment overrides (useful in Docker / TrueNAS SCALE)
	if v := os.Getenv("NODE_NAME"); v != "" {
		cfg.NodeName = v
	}
	if v := os.Getenv("RELAY_URL"); v != "" {
		cfg.RelayURL = v
	}
	if v := os.Getenv("AUTH_TOKEN"); v != "" {
		cfg.AuthToken = v
	}
	if v := os.Getenv("GENS_WEB_LISTEN"); v != "" {
		cfg.WebListen = v
	}
	if v := os.Getenv("GENS_WEB_TLS"); v == "true" || v == "1" {
		cfg.WebTLS = true
	}
	if v := os.Getenv("GENS_DROP_INBOX"); v != "" {
		cfg.DropInboxDir = v
	}

	_ = os.MkdirAll(cfg.DropInboxDir, 0755)
	if err := cfg.Save(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *DaemonConfig) DataDir() string {
	return filepath.Dir(c.filePath)
}

func (c *DaemonConfig) Save() error {
	c.mu.RLock()
	data, err := json.MarshalIndent(c.Settings, "", "  ")
	path := c.filePath
	c.mu.RUnlock()
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (c *DaemonConfig) Snapshot() Settings {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Settings
}

func (c *DaemonConfig) Update(fn func(*DaemonConfig)) error {
	c.mu.Lock()
	fn(c)
	c.mu.Unlock()
	return c.Save()
}

func (c *DaemonConfig) SetWebPassword(plain string) error {
	c.authCacheMu.Lock()
	c.authCacheKey = ""
	c.authCacheHash = ""
	c.authCacheExp = time.Time{}
	c.authCacheMu.Unlock()

	if plain == "" {
		return c.Update(func(dc *DaemonConfig) {
			dc.WebPassHash = ""
		})
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	return c.Update(func(dc *DaemonConfig) {
		dc.WebPassHash = string(hash)
	})
}

func (c *DaemonConfig) VerifyWebAuth(user, pass string) bool {
	c.mu.RLock()
	expectedUser := c.WebUser
	hash := c.WebPassHash
	c.mu.RUnlock()

	if hash == "" {
		return true
	}
	if expectedUser != "" && user != expectedUser {
		return false
	}

	credDigest := sha256.Sum256([]byte(user + "\x00" + pass))
	credKey := hex.EncodeToString(credDigest[:])

	c.authCacheMu.RLock()
	cached := c.authCacheHash == hash && c.authCacheKey == credKey && time.Now().Before(c.authCacheExp)
	c.authCacheMu.RUnlock()
	if cached {
		return true
	}

	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(pass)) != nil {
		return false
	}

	c.authCacheMu.Lock()
	c.authCacheHash = hash
	c.authCacheKey = credKey
	c.authCacheExp = time.Now().Add(2 * time.Minute)
	c.authCacheMu.Unlock()
	return true
}

// CleanupOrphanedTemps removes stale .gens.tmp files older than 10 minutes,
// preserving temp files belonging to active or paused QuickDrop transfers.
func CleanupOrphanedTemps(root string, activeTransferIDs ...map[string]struct{}) {
	if root == "" {
		return
	}
	var preserve map[string]struct{}
	if len(activeTransferIDs) > 0 {
		preserve = activeTransferIDs[0]
	}
	cutoff := time.Now().Add(-10 * time.Minute)
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		name := d.Name()
		if idx := strings.Index(name, ".gens.tmp."); idx >= 0 {
			suffix := name[idx+len(".gens.tmp."):]
			if preserve != nil {
				if _, keep := preserve[suffix]; keep {
					return nil
				}
			}
			if info, statErr := d.Info(); statErr == nil && info.ModTime().Before(cutoff) {
				_ = os.Remove(path)
			}
		}
		return nil
	})
}

// CleanupOrphanedStaging removes stale browser upload staging directories older than 30 minutes.
func CleanupOrphanedStaging(stagingRoots ...string) {
	cutoff := time.Now().Add(-30 * time.Minute)
	for _, root := range stagingRoots {
		if root == "" {
			continue
		}
		entries, err := os.ReadDir(root)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			if info, err := e.Info(); err == nil && info.ModTime().Before(cutoff) {
				_ = os.RemoveAll(filepath.Join(root, e.Name()))
			}
		}
	}
}

func envOr(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
