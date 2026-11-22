package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/WilliamBossard/Gens-Relay/internal/config"
	"github.com/WilliamBossard/Gens-Relay/internal/drop"
	"github.com/WilliamBossard/Gens-Relay/internal/lock"
	"github.com/WilliamBossard/Gens-Relay/internal/logs"
	"github.com/WilliamBossard/Gens-Relay/internal/p2p"
	"github.com/WilliamBossard/Gens-Relay/internal/store"
	"github.com/WilliamBossard/Gens-Relay/internal/syncer"
	"github.com/WilliamBossard/Gens-Relay/internal/webapi"
	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
	"github.com/quic-go/quic-go"
)

func main() {
	logRing := logs.NewRingBuffer(500, nil)

	dataDirFlag := flag.String("data-dir", config.DefaultDataDir(), "Daemon config & state directory")
	webListenFlag := flag.String("web", "", "Override Web UI listen address (e.g. 0.0.0.0:8385)")
	webTLSFlag := flag.Bool("web-tls", false, "Enable HTTPS with a self-signed certificate for the Web UI")
	flag.Parse()

	dataDir := *dataDirFlag

	instLock, err := lock.Acquire(dataDir)
	if err != nil {
		log.Fatalf("[gens-daemon] %v", err)
	}
	defer instLock.Release()

	cfg, err := config.LoadOrCreateDaemonConfig(dataDir)
	if err != nil {
		log.Fatalf("[gens-daemon] config error: %v", err)
	}
	if *webListenFlag != "" {
		_ = cfg.Update(func(c *config.DaemonConfig) { c.WebListen = *webListenFlag })
	}
	if *webTLSFlag {
		_ = cfg.Update(func(c *config.DaemonConfig) { c.WebTLS = true })
	}

	// Structured logging: stderr + Web UI ring buffer + dataDir/gens-relay.log
	snapCfg := cfg.Snapshot()
	logs.Setup(snapCfg.LogLevel, snapCfg.LogFormat, filepath.Join(dataDir, "gens-relay.log"), logRing)

	id, err := protocol.LoadOrCreateIdentity(dataDir)
	if err != nil {
		log.Fatalf("[gens-daemon] identity error: %v", err)
	}

	st, err := store.Open(dataDir)
	if err != nil {
		log.Fatalf("[gens-daemon] store error: %v", err)
	}
	defer st.Close()

	// Clean orphaned .gens.tmp files and stale browser upload staging directories on startup
	activeDropIDs, _ := st.ListActiveDropTransferIDs()
	activeStaging, _ := st.ListActiveDropStagingDirs()
	inboxDir := cfg.Snapshot().DropInboxDir
	config.CleanupOrphanedTemps(inboxDir, activeDropIDs)
	config.CleanupOrphanedStaging(activeStaging, filepath.Join(dataDir, "drop_staging"), filepath.Join(inboxDir, ".gens.staging"))
	_ = st.PruneOldDropTransfers(30 * 24 * time.Hour)
	if folders, err := st.ListFolders(); err == nil {
		for _, f := range folders {
			config.CleanupOrphanedTemps(f.LocalPath, activeDropIDs)
		}
	}
	if drops, err := st.ListDropTransfersSummary(); err == nil {
		seenDirs := make(map[string]struct{})
		for _, d := range drops {
			if d.DestDir != "" {
				if _, ok := seenDirs[d.DestDir]; !ok {
					seenDirs[d.DestDir] = struct{}{}
					config.CleanupOrphanedTemps(d.DestDir, activeDropIDs)
				}
			}
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Write a local IPC shutdown token (0600) so desktop tray wrapper can shut down cleanly even with Web Auth enabled
	var tokBytes [16]byte
	_, _ = rand.Read(tokBytes[:])
	shutdownToken := hex.EncodeToString(tokBytes[:])
	shutdownTokenPath := filepath.Join(dataDir, "shutdown.token")
	shutdownReqPath := filepath.Join(dataDir, "shutdown.req")
	_ = os.Remove(shutdownReqPath)
	_ = os.WriteFile(shutdownTokenPath, []byte(shutdownToken), 0600)
	defer os.Remove(shutdownTokenPath)
	defer os.Remove(shutdownReqPath)

	// Watch local shutdown.req file so desktop wrappers can trigger clean exit even with HTTPS Web UI enabled
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if raw, err := os.ReadFile(shutdownReqPath); err == nil {
					got := strings.TrimSpace(string(raw))
					if subtle.ConstantTimeCompare([]byte(got), []byte(shutdownToken)) == 1 {
						_ = os.Remove(shutdownReqPath)
						log.Println("[gens-daemon] Graceful shutdown requested via IPC token.")
						stop()
						return
					}
				}
			}
		}
	}()

	var apiSrv *webapi.Server
	emit := func(event string, data any) {
		if apiSrv != nil {
			apiSrv.BroadcastEvent(event, data)
		}
	}

	var syncMgr *syncer.Manager
	var dropMgr *drop.Manager
	var p2pMgr *p2p.Manager

	p2pMgr = p2p.NewManager(id, cfg, st, p2p.Handlers{
		OnPeerConnected: func(c context.Context, peerID string) {
			if syncMgr != nil {
				syncMgr.OnPeerConnected(c, peerID)
			}
			if dropMgr != nil {
				dropMgr.OnPeerConnected(c, peerID)
			}
		},
		OnPeerState: func() {
			if apiSrv != nil && p2pMgr != nil {
				emit("peers_updated", p2pMgr.PeerStatuses())
			}
		},
		OnPairRequests: func() {
			if apiSrv != nil {
				if reqs, err := st.ListPairRequests(); err == nil {
					emit("pair_requests_updated", reqs)
				}
			}
		},
		OnNearbyPeers: func() {
			if apiSrv != nil && p2pMgr != nil {
				emit("nearby_updated", p2pMgr.NearbyPeers())
			}
		},
		OnControlMessage: func(c context.Context, peerID string, env protocol.ControlEnvelope) {
			switch env.Type {
			case protocol.CtrlFolderSummary, protocol.CtrlFolderIndex, protocol.CtrlFolderInvite, protocol.CtrlFolderInviteReject:
				if syncMgr != nil {
					syncMgr.HandleControlMessage(c, peerID, env)
				}
			case protocol.CtrlDropOffer:
				if dropMgr != nil {
					dropMgr.HandleOffer(c, peerID, env.Payload)
				}
			case protocol.CtrlDropResponse:
				if dropMgr != nil {
					dropMgr.HandleResponse(c, peerID, env.Payload)
				}
			case protocol.CtrlDropComplete:
				if dropMgr != nil {
					dropMgr.HandleComplete(peerID, env.Payload)
				}
			case protocol.CtrlDropPause:
				if dropMgr != nil {
					dropMgr.HandlePause(peerID, env.Payload)
				}
			case protocol.CtrlDropResume:
				if dropMgr != nil {
					dropMgr.HandleResume(peerID, env.Payload)
				}
			case protocol.CtrlDropCancel:
				if dropMgr != nil {
					dropMgr.HandleCancel(peerID, env.Payload)
				}
			}
		},
		OnDataStream: func(c context.Context, peerID string, kind protocol.DataRequestKind, stream *quic.Stream) {
			switch kind {
			case protocol.DataReqSyncBlock, protocol.DataReqSyncMultiBlock:
				if syncMgr != nil {
					syncMgr.HandleDataStream(c, peerID, kind, stream)
				} else {
					_ = stream.Close()
				}
			case protocol.DataReqDropChunk:
				if dropMgr != nil {
					dropMgr.ServeChunkStream(c, peerID, stream)
				} else {
					_ = stream.Close()
				}
			default:
				_ = stream.Close()
			}
		},
	})

	syncMgr = syncer.NewManager(id.DeviceID, st, p2pMgr, emit)
	dropMgr = drop.NewManager(cfg, st, p2pMgr, emit)
	dropMgr.SetRootContext(ctx)
	apiSrv = webapi.NewServer(id, cfg, st, p2pMgr, syncMgr, dropMgr, logRing)
	apiSrv.SetRootContext(ctx)
	apiSrv.SetShutdownToken(shutdownToken)
	apiSrv.SetShutdownFunc(stop)
	logRing.SetCallback(func(e logs.Entry) {
		emit("log_entry", e)
	})

	if err := p2pMgr.Start(ctx); err != nil {
		log.Fatalf("[gens-daemon] p2p start error: %v", err)
	}
	syncMgr.Start(ctx)

	snap := cfg.Snapshot()
	webAddr := snap.WebListen
	// Guard: never expose the Web UI on a non-loopback interface without
	// authentication — a loud, unmissable warning at startup.
	webHost := strings.Trim(strings.TrimPrefix(strings.SplitN(webAddr, ":", 2)[0], "["), "]")
	if webHost != "127.0.0.1" && webHost != "::1" && webHost != "localhost" &&
		(snap.WebUser == "" || snap.WebPassHash == "") {
		slog.Warn("web UI exposed without authentication — set web_user/web_pass before exposing beyond loopback", "addr", webAddr)
	}
	httpSrv := &http.Server{
		Addr:              webAddr,
		Handler:           apiSrv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
	slog.Info("daemon starting", "device", id.DeviceID)
		if snap.WebTLS {
			certPath, keyPath, err := webapi.EnsureWebCert(dataDir)
			if err != nil {
				log.Fatalf("[gens-daemon] web TLS cert error: %v", err)
			}
			slog.Info("Web UI listening (https)", "addr", webAddr)
			if err := httpSrv.ListenAndServeTLS(certPath, keyPath); err != nil && err != http.ErrServerClosed {
				log.Fatalf("[gens-daemon] web server fatal: %v", err)
			}
			return
		}
		slog.Info("Web UI listening (http)", "addr", webAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[gens-daemon] web server fatal: %v", err)
		}
	}()

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutCtx)
	syncMgr.Shutdown(shutCtx)
	dropMgr.Shutdown(shutCtx)
	p2pMgr.Shutdown(shutCtx)
	log.Println("[gens-daemon] Shutdown complete.")
}
