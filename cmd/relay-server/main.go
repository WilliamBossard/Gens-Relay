package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/WilliamBossard/Gens-Relay/internal/lock"
	"github.com/WilliamBossard/Gens-Relay/internal/logs"
	"github.com/WilliamBossard/Gens-Relay/internal/relay"
)

func main() {
	logRing := logs.NewRingBuffer(500, nil)
	log.SetOutput(io.MultiWriter(os.Stderr, logRing))

	dataDirFlag := flag.String("data-dir", relay.DefaultDataDir(), "Relay config & state directory")
	listenAddr := flag.String("listen", os.Getenv("RELAY_LISTEN"), "HTTP/WebSocket listen address (default :38420)")
	adminListen := flag.String("admin-listen", os.Getenv("RELAY_ADMIN_LISTEN"), "Admin Web UI listen address (default 127.0.0.1:38421, 'off' to disable separate port)")
	authToken := flag.String("token", os.Getenv("AUTH_TOKEN"), "Optional shared network token")
	enableRelay := flag.Bool("enable-relay", os.Getenv("ENABLE_BLIND_RELAY") == "true", "Allow opaque encrypted relay fallback when direct P2P fails")
	trustProxy := flag.Bool("trust-proxy", os.Getenv("TRUST_PROXY") == "true", "Trust X-Forwarded-For / X-Real-IP headers from reverse proxies")
	maxRelayKBps := flag.Int("max-relay-kbps", envIntOr("MAX_RELAY_KBPS", 0), "Max blind relay bandwidth in KB/s (0 = unlimited)")
	adminUser := flag.String("admin-user", os.Getenv("RELAY_ADMIN_USER"), "Admin Web UI username")
	adminPass := flag.String("admin-pass", os.Getenv("RELAY_ADMIN_PASS"), "Admin Web UI password")
	tlsCert := flag.String("tls-cert", os.Getenv("RELAY_TLS_CERT"), "Optional TLS certificate PEM path for WSS/HTTPS")
	tlsKey := flag.String("tls-key", os.Getenv("RELAY_TLS_KEY"), "Optional TLS private key PEM path for WSS/HTTPS")
	flag.Parse()

	dataDir := *dataDirFlag
	if dataDir != "" {
		instLock, err := lock.Acquire(dataDir)
		if err != nil {
			log.Fatalf("[relay-server] %v", err)
		}
		defer instLock.Release()
	}

	seed := relay.Options{
		ListenAddr:       *listenAddr,
		AdminListen:      *adminListen,
		AuthToken:        *authToken,
		EnableBlindRelay: *enableRelay,
		TrustProxy:       *trustProxy,
		MaxRelayKBps:     *maxRelayKBps,
		AdminUser:        *adminUser,
		AdminPassword:    *adminPass,
	}

	cfg, err := relay.LoadOrCreateConfig(dataDir, seed)
	if err != nil {
		log.Fatalf("[relay-server] config error: %v", err)
	}
	snap := cfg.Snapshot()

	var tokBytes [16]byte
	_, _ = rand.Read(tokBytes[:])
	shutdownToken := hex.EncodeToString(tokBytes[:])
	if dataDir != "" {
		tokPath := filepath.Join(dataDir, "shutdown.token")
		_ = os.WriteFile(tokPath, []byte(shutdownToken), 0600)
		defer os.Remove(tokPath)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := relay.NewServer(relay.Options{
		Config:        cfg,
		Logs:          logRing,
		ShutdownToken: shutdownToken,
		OnShutdown:    stop,
	})
	logRing.SetCallback(func(e logs.Entry) {
		srv.BroadcastLog(e)
	})
	srv.StartBackground(ctx)

	httpServer := &http.Server{
		Addr:              snap.ListenAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	var adminServer *http.Server
	if snap.AdminListen != "" && snap.AdminListen != "off" && snap.AdminListen != snap.ListenAddr {
		adminServer = &http.Server{
			Addr:              snap.AdminListen,
			Handler:           srv.AdminHandler(),
			ReadHeaderTimeout: 10 * time.Second,
		}
	}

	if err := srv.StartUDP(ctx, snap.ListenAddr); err != nil {
		log.Printf("[relay-server] UDP STUN/Relay listener disabled (%v)", err)
	} else {
		log.Printf("[relay-server] UDP STUN & Native Blind Relay active on %s/udp", snap.ListenAddr)
	}

	useTLS := *tlsCert != "" && *tlsKey != ""
	go func() {
		log.Printf("[relay-server] Listening on %s (tls=%v, blind_relay=%v, stores_files=false)", snap.ListenAddr, useTLS, snap.EnableBlindRelay)
		var sErr error
		if useTLS {
			sErr = httpServer.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			sErr = httpServer.ListenAndServe()
		}
		if sErr != nil && sErr != http.ErrServerClosed {
			log.Fatalf("[relay-server] fatal: %v", sErr)
		}
	}()

	if adminServer != nil {
		go func() {
			log.Printf("[relay-server] Admin Web UI listening on http://%s", snap.AdminListen)
			if err := adminServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("[relay-server] admin server error: %v", err)
			}
		}()
	} else {
		log.Printf("[relay-server] Admin Web UI mounted at %s/admin/", snap.ListenAddr)
	}

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutCtx)
	_ = httpServer.Shutdown(shutCtx)
	if adminServer != nil {
		_ = adminServer.Shutdown(shutCtx)
	}
	log.Println("[relay-server] Shutdown complete.")
}

func envIntOr(k string, fallback int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
