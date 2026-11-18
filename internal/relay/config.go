package relay

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
	"golang.org/x/crypto/bcrypt"
)

type ConfigSettings struct {
	ListenAddr       string            `json:"listen_addr"`
	AdminListen      string            `json:"admin_listen"`
	AuthToken        string            `json:"auth_token"`
	EnableBlindRelay bool              `json:"enable_blind_relay"`
	TrustProxy       bool              `json:"trust_proxy"`
	MaxRelayKBps     int               `json:"max_relay_kbps"` // 0 = unlimited
	MaxClients       int               `json:"max_clients"`    // 0 = default 2000
	AdminUser        string            `json:"admin_user"`
	AdminPassHash    string            `json:"admin_pass_hash,omitempty"`
	BannedDevices    map[string]string `json:"banned_devices,omitempty"` // DeviceID -> reason
}

type RelayConfig struct {
	mu            sync.RWMutex
	ConfigSettings
	filePath      string
	authCacheMu   sync.RWMutex
	authCacheKey  string
	authCacheHash string
	authCacheExp  time.Time
}

func DefaultDataDir() string {
	if env := os.Getenv("RELAY_DATA_DIR"); env != "" {
		return env
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return ".gens-relay-server"
	}
	return filepath.Join(base, "Gens-Relay-Server")
}

func LoadOrCreateConfig(dataDir string, seed Options) (*RelayConfig, error) {
	if dataDir != "" {
		if err := os.MkdirAll(dataDir, 0700); err != nil {
			return nil, err
		}
	}
	cfgPath := ""
	if dataDir != "" {
		cfgPath = filepath.Join(dataDir, "relay-config.json")
	}

	rc := &RelayConfig{
		ConfigSettings: ConfigSettings{
			ListenAddr:       ":38420",
			AdminListen:      "127.0.0.1:38421",
			AuthToken:        seed.AuthToken,
			EnableBlindRelay: seed.EnableBlindRelay,
			TrustProxy:       seed.TrustProxy,
			MaxRelayKBps:     seed.MaxRelayKBps,
			MaxClients:       2000,
			AdminUser:        "admin",
			BannedDevices:    make(map[string]string),
		},
		filePath: cfgPath,
	}

	if cfgPath != "" {
		if data, err := os.ReadFile(cfgPath); err == nil {
			if uErr := json.Unmarshal(data, &rc.ConfigSettings); uErr != nil {
				backup := fmt.Sprintf("%s.corrupt.%d", cfgPath, time.Now().Unix())
				_ = os.WriteFile(backup, data, 0600)
				return nil, fmt.Errorf("parse %s (backup saved to %s): %w", cfgPath, backup, uErr)
			}
			if rc.BannedDevices == nil {
				rc.BannedDevices = make(map[string]string)
			}
		}
	}

	// Explicit CLI / env overrides take precedence when provided
	if seed.ListenAddr != "" {
		rc.ListenAddr = seed.ListenAddr
	}
	if seed.AdminListen != "" {
		rc.AdminListen = seed.AdminListen
	}
	if seed.AuthToken != "" {
		rc.AuthToken = seed.AuthToken
	}
	if seed.EnableBlindRelay {
		rc.EnableBlindRelay = true
	}
	if seed.TrustProxy {
		rc.TrustProxy = true
	}
	if seed.MaxRelayKBps > 0 {
		rc.MaxRelayKBps = seed.MaxRelayKBps
	}
	if seed.AdminUser != "" {
		rc.AdminUser = seed.AdminUser
	}
	if seed.AdminPassword != "" {
		_ = rc.SetAdminPassword(seed.AdminPassword)
	}

	if cfgPath != "" {
		_ = rc.Save()
	}
	return rc, nil
}

func (c *RelayConfig) Snapshot() ConfigSettings {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cp := c.ConfigSettings
	cp.BannedDevices = make(map[string]string, len(c.BannedDevices))
	for k, v := range c.BannedDevices {
		cp.BannedDevices[k] = v
	}
	return cp
}

func (c *RelayConfig) Update(fn func(*RelayConfig)) error {
	c.mu.Lock()
	fn(c)
	c.mu.Unlock()
	return c.Save()
}

func (c *RelayConfig) Save() error {
	c.mu.RLock()
	path := c.filePath
	if path == "" {
		c.mu.RUnlock()
		return nil
	}
	data, err := json.MarshalIndent(c.ConfigSettings, "", "  ")
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

func (c *RelayConfig) IsBanned(deviceID string) bool {
	norm := protocol.NormalizeDeviceID(deviceID)
	c.mu.RLock()
	_, ok := c.BannedDevices[norm]
	c.mu.RUnlock()
	return ok
}

func (c *RelayConfig) BanDevice(deviceID, reason string) error {
	norm := protocol.NormalizeDeviceID(deviceID)
	if norm == "" {
		return nil
	}
	return c.Update(func(rc *RelayConfig) {
		if rc.BannedDevices == nil {
			rc.BannedDevices = make(map[string]string)
		}
		rc.BannedDevices[norm] = reason
	})
}

func (c *RelayConfig) UnbanDevice(deviceID string) error {
	norm := protocol.NormalizeDeviceID(deviceID)
	return c.Update(func(rc *RelayConfig) {
		delete(rc.BannedDevices, norm)
	})
}

func (c *RelayConfig) SetAdminPassword(plain string) error {
	c.authCacheMu.Lock()
	c.authCacheKey = ""
	c.authCacheHash = ""
	c.authCacheExp = time.Time{}
	c.authCacheMu.Unlock()

	if plain == "" {
		return c.Update(func(rc *RelayConfig) {
			rc.AdminPassHash = ""
		})
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	return c.Update(func(rc *RelayConfig) {
		rc.AdminPassHash = string(hash)
	})
}

func (c *RelayConfig) VerifyAdminAuth(user, pass string) bool {
	c.mu.RLock()
	expectedUser := c.AdminUser
	hash := c.AdminPassHash
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
