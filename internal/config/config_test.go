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

func TestCleanupOrphanedTempsPreservesActiveDrops(t *testing.T) {
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
}
