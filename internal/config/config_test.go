package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCorruptConfigCreatesBackup(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte("{corrupt json"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreateDaemonConfig(dir); err == nil {
		t.Fatal("expected error when loading corrupt config.json")
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "config.json.corrupt.*"))
	if len(matches) != 1 {
		t.Fatalf("expected corrupt backup file, got %v", matches)
	}
}

func TestVerifyWebAuthCacheAndInvalidation(t *testing.T) {
	dir := t.TempDir()
	cfg, err := LoadOrCreateDaemonConfig(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = cfg.Update(func(c *DaemonConfig) {
		c.WebUser = "admin"
	})
	if err := cfg.SetWebPassword("s3cret!"); err != nil {
		t.Fatal(err)
	}
	if !cfg.VerifyWebAuth("admin", "s3cret!") {
		t.Fatal("expected valid credentials to pass")
	}
	// Second check hits cache
	if !cfg.VerifyWebAuth("admin", "s3cret!") {
		t.Fatal("expected cached valid credentials to pass")
	}
	if cfg.VerifyWebAuth("admin", "wrong") {
		t.Fatal("expected wrong password to fail")
	}
	if err := cfg.SetWebPassword("newpass"); err != nil {
		t.Fatal(err)
	}
	if cfg.VerifyWebAuth("admin", "s3cret!") {
		t.Fatal("expected old password to be invalidated after SetWebPassword")
	}
	if !cfg.VerifyWebAuth("admin", "newpass") {
		t.Fatal("expected new password to pass")
	}
}

func TestCleanupOrphanedTempsAndStagingPreservesActiveDrops(t *testing.T) {
	dir := t.TempDir()
	activeTmp := filepath.Join(dir, ".file.gens.tmp.tr-active")
	staleTmp := filepath.Join(dir, ".file.gens.tmp.tr-stale")
	_ = os.WriteFile(activeTmp, []byte("keep"), 0644)
	_ = os.WriteFile(staleTmp, []byte("drop"), 0644)

	past := time.Now().Add(-20 * time.Minute)
	_ = os.Chtimes(activeTmp, past, past)
	_ = os.Chtimes(staleTmp, past, past)

	CleanupOrphanedTemps(dir, map[string]struct{}{"tr-active": {}})

	if _, err := os.Stat(activeTmp); err != nil {
		t.Fatalf("expected active temp file to be preserved: %v", err)
	}
	if _, err := os.Stat(staleTmp); !os.IsNotExist(err) {
		t.Fatalf("expected stale temp file to be deleted")
	}

	stageRoot := filepath.Join(dir, "staging")
	activeStage := filepath.Join(stageRoot, "s1")
	staleStage := filepath.Join(stageRoot, "s2")
	_ = os.MkdirAll(activeStage, 0755)
	_ = os.MkdirAll(staleStage, 0755)
	oldStage := time.Now().Add(-45 * time.Minute)
	_ = os.Chtimes(activeStage, oldStage, oldStage)
	_ = os.Chtimes(staleStage, oldStage, oldStage)

	CleanupOrphanedStaging(map[string]struct{}{filepath.Clean(activeStage): {}}, stageRoot)
	if _, err := os.Stat(activeStage); err != nil {
		t.Fatalf("expected active staging dir to be preserved: %v", err)
	}
	if _, err := os.Stat(staleStage); !os.IsNotExist(err) {
		t.Fatalf("expected stale staging dir to be removed")
	}
}

func TestMultiRelayEnvAndNormalization(t *testing.T) {
	t.Setenv("GENS_RELAY_URLS", "relay1.example.com:38420/ws, wss://relay2.example.com/ws")
	dir := t.TempDir()
	cfg, err := LoadOrCreateDaemonConfig(dir)
	if err != nil {
		t.Fatal(err)
	}
	snap := cfg.Snapshot()
	if len(snap.RelayURLs) != 2 || snap.RelayURLs[0] != "ws://relay1.example.com:38420/ws" || snap.RelayURLs[1] != "wss://relay2.example.com/ws" {
		t.Fatalf("unexpected RelayURLs: %+v", snap.RelayURLs)
	}
	if snap.RelayURL != snap.RelayURLs[0] {
		t.Fatalf("expected RelayURL synced with RelayURLs[0]")
	}
	if !snap.EnableUPnP || !snap.EnableCompression {
		t.Fatalf("expected UPnP and Compression enabled by default")
	}
}
