package drop

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WilliamBossard/Gens-Relay/internal/chunker"
	"github.com/WilliamBossard/Gens-Relay/internal/config"
	"github.com/WilliamBossard/Gens-Relay/internal/p2p"
	"github.com/WilliamBossard/Gens-Relay/internal/store"
	"github.com/WilliamBossard/Gens-Relay/internal/syncer"
	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
	"github.com/quic-go/quic-go"
)

const streamDeadline = 30 * time.Second

type recvProgressBuffer struct {
	peerID  string
	destDir string
	pending []store.DropBlockCompletion
	seen    map[string]struct{}
}

// StagedFile describes a file already written to staging and hashed in-flight during upload.
type StagedFile struct {
	RelPath   string
	LocalPath string
	Size      int64
	BlockSize int
	Blocks    []protocol.BlockInfo
	FileHash  string
}

type Manager struct {
	cfg     *config.DaemonConfig
	store   *store.Store
	p2p     *p2p.Manager
	onEmit  func(event string, data any)
	handles *chunker.FileHandleCache

	rootMu     sync.RWMutex
	rootCtx    context.Context
	shutCancel context.CancelFunc

	bgWg sync.WaitGroup

	mu                sync.Mutex
	sourceMaps        map[string]map[string]string // transferID -> relPath -> local fullPath
	stagingDirs       map[string]string            // transferID -> staging dir to clean after finish
	activeCanc        map[string]context.CancelFunc
	pauseAfterPrep    map[string]bool
	pausedBeforeOffer map[string]bool
	receiveLock       map[string]*sync.Mutex
	recvBuffers       map[string]*recvProgressBuffer
	lastEmitNs        atomic.Int64
	bytesRx           atomic.Uint64
	bytesTx           atomic.Uint64
}

func NewManager(cfg *config.DaemonConfig, st *store.Store, p2pMgr *p2p.Manager, onEmit func(string, any)) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		cfg:               cfg,
		store:             st,
		p2p:               p2pMgr,
		onEmit:            onEmit,
		handles:           chunker.NewFileHandleCache(48, 5*time.Second),
		rootCtx:           ctx,
		shutCancel:        cancel,
		sourceMaps:        make(map[string]map[string]string),
		stagingDirs:       make(map[string]string),
		activeCanc:        make(map[string]context.CancelFunc),
		pauseAfterPrep:    make(map[string]bool),
		pausedBeforeOffer: make(map[string]bool),
		receiveLock:       make(map[string]*sync.Mutex),
		recvBuffers:       make(map[string]*recvProgressBuffer),
	}
}

func (m *Manager) getRootContext() context.Context {
	m.rootMu.RLock()
	defer m.rootMu.RUnlock()
	return m.rootCtx
}

func (m *Manager) SetRootContext(ctx context.Context) {
	cCtx, cancel := context.WithCancel(ctx)
	m.rootMu.Lock()
	if m.shutCancel != nil {
		m.shutCancel()
	}
	m.rootCtx = cCtx
	m.shutCancel = cancel
	m.rootMu.Unlock()

	go func() {
		<-cCtx.Done()
		m.handles.CloseAll()
	}()
}

// Shutdown cancels active Drop tasks, flushes pending receive progress to SQLite, and closes file handles.
func (m *Manager) Shutdown(ctx context.Context) {
	m.rootMu.RLock()
	cancel := m.shutCancel
	m.rootMu.RUnlock()
	if cancel != nil {
		cancel()
	}

	m.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(m.activeCanc))
	for _, fn := range m.activeCanc {
		cancels = append(cancels, fn)
	}
	recvIDs := make([]string, 0, len(m.recvBuffers))
	for id := range m.recvBuffers {
		recvIDs = append(recvIDs, id)
	}
	m.mu.Unlock()

	for _, fn := range cancels {
		fn()
	}
	for _, id := range recvIDs {
		mu := m.getReceiveMutex(id)
		mu.Lock()
		m.flushRecvBlocks(id)
		mu.Unlock()
	}

	done := make(chan struct{})
	go func() {
		m.bgWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}

	m.handles.CloseAll()
}

func (m *Manager) OnPeerConnected(ctx context.Context, peerID string) {
	peerID = protocol.NormalizeDeviceID(peerID)

	if queuedIDs, err := m.store.ListQueuedOfflineSendDropIDs(peerID); err == nil {
		for _, id := range queuedIDs {
			m.launchBg(func() {
				m.deliverQueuedTransfer(m.getRootContext(), id)
			})
		}
	}

	ids, err := m.store.ListResumableSendDropIDs(peerID)
	if err != nil {
		return
	}
	for _, id := range ids {
		m.mu.Lock()
		_, running := m.activeCanc[id]
		m.mu.Unlock()
		if !running {
			tid := id
			m.launchBg(func() {
				m.runSendTransfer(m.getRootContext(), tid)
			})
		}
	}
}

func (m *Manager) launchBg(fn func()) {
	m.bgWg.Add(1)
	go func() {
		defer m.bgWg.Done()
		fn()
	}()
}

// SendText sends an instant clipboard/text snippet to a peer (or queues it offline until connected).
func (m *Manager) SendText(ctx context.Context, peerID, text string) (*store.DropTransferRecord, error) {
	peerID = protocol.NormalizeDeviceID(peerID)
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil, errors.New("texte vide")
	}
	transferID := newTransferID()
	title := trimmed
	if idx := strings.IndexByte(title, '\n'); idx >= 0 {
		title = title[:idx]
	}
	if len(title) > 64 {
		title = title[:64] + "..."
	}

	peerName := peerID
	if dev, err := m.store.GetDevice(peerID); err == nil && dev.Name != "" {
		peerName = dev.Name
	}

	rec := store.DropTransferRecord{
		TransferID:      transferID,
		Direction:       "send",
		Kind:            "text",
		PeerID:          peerID,
		PeerName:        peerName,
		Title:           title,
		TextContent:     text,
		TotalSize:       int64(len(text)),
		Status:          "queued_offline",
		CompletedBlocks: map[string][]int{},
	}

	if !m.p2p.IsConnected(peerID) {
		if err := m.store.UpsertDropTransfer(rec); err != nil {
			return nil, err
		}
		m.emitUpdateForce()
		return &rec, nil
	}

	rec.TransferredBytes = int64(len(text))
	rec.Status = "completed"
	if err := m.store.UpsertDropTransfer(rec); err != nil {
		return nil, err
	}

	offer := protocol.DropOfferMsg{
		TransferID:  transferID,
		SenderName:  m.cfg.Snapshot().NodeName,
		Kind:        "text",
		Title:       title,
		TextContent: text,
		MimeType:    "text/plain",
		TotalSize:   int64(len(text)),
		FileCount:   0,
	}
	if err := m.p2p.SendControl(peerID, protocol.CtrlDropOffer, offer); err != nil {
		slog.Error("drop offer send failed", "peer", peerID, "transfer", rec.TransferID, "err", err)
		if !m.p2p.IsConnected(peerID) {
			rec.Status = "queued_offline"
			rec.TransferredBytes = 0
			_ = m.store.UpsertDropTransfer(rec)
			m.emitUpdateForce()
			return &rec, nil
		}
		rec.Status = "failed"
		rec.TransferredBytes = 0
		rec.ErrorMsg = err.Error()
		_ = m.store.UpsertDropTransfer(rec)
		m.emitUpdateForce()
		return nil, err
	}
	m.bytesTx.Add(uint64(len(text)))
	m.emitUpdateForce()
	return &rec, nil
}

func (m *Manager) TransferCounters() (rx, tx uint64) {
	return m.bytesRx.Load(), m.bytesTx.Load()
}

func (m *Manager) ListTransfers() ([]store.DropTransferRecord, error) {
	return m.store.ListDropTransfersSummary()
}

// SendPaths creates a Drop offer for one or more local files/directories, hashes asynchronously,
// and dispatches or queues it for peerID.
func (m *Manager) SendPaths(ctx context.Context, peerID string, localPaths []string) (*store.DropTransferRecord, error) {
	return m.sendPathsInternal(ctx, peerID, localPaths, "")
}

// SendStagedPaths sends uploaded/staged files and cleans up stagingDir once the transfer completes or is rejected.
func (m *Manager) SendStagedPaths(ctx context.Context, peerID, stagingDir string, localPaths []string) (*store.DropTransferRecord, error) {
	return m.sendPathsInternal(ctx, peerID, localPaths, stagingDir)
}

// SendPrehashedStagedPaths dispatches multipart uploads whose BLAKE3 adaptive blocks were already
// computed inline during upload streaming, eliminating a second disk read pass.
func (m *Manager) SendPrehashedStagedPaths(ctx context.Context, peerID, stagingDir string, staged []StagedFile) (*store.DropTransferRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	peerID = protocol.NormalizeDeviceID(peerID)
	if len(staged) == 0 {
		return nil, errors.New("aucun fichier ou dossier sélectionné")
	}

	usedRel := make(map[string]struct{}, len(staged))
	entries := make([]protocol.DropFileEntry, 0, len(staged))
	srcMap := make(map[string]string, len(staged))
	var totalSize int64

	for _, sf := range staged {
		rel := ensureUniqueRelPath(filepath.ToSlash(strings.TrimLeft(filepath.ToSlash(sf.RelPath), "/")), usedRel)
		entries = append(entries, protocol.DropFileEntry{
			RelPath:   rel,
			Size:      sf.Size,
			IsDir:     false,
			BlockSize: sf.BlockSize,
			Blocks:    sf.Blocks,
			FileHash:  sf.FileHash,
		})
		srcMap[rel] = sf.LocalPath
		totalSize += sf.Size
	}

	transferID := newTransferID()
	title := path.Base(entries[0].RelPath)
	if len(entries) > 1 {
		title = fmt.Sprintf("%s (+%d)", title, len(entries)-1)
	}

	peerName := peerID
	if dev, err := m.store.GetDevice(peerID); err == nil && dev.Name != "" {
		peerName = dev.Name
	}

	srcJSON, _ := json.Marshal(srcMap)
	rec := store.DropTransferRecord{
		TransferID:      transferID,
		Direction:       "send",
		Kind:            "files",
		PeerID:          peerID,
		PeerName:        peerName,
		Title:           title,
		SourceRoot:      string(srcJSON),
		StagingDir:      stagingDir,
		TotalSize:       totalSize,
		Status:          "pending_accept",
		Files:           entries,
		CompletedBlocks: map[string][]int{},
	}

	m.mu.Lock()
	m.sourceMaps[transferID] = srcMap
	if stagingDir != "" {
		m.stagingDirs[transferID] = stagingDir
	}
	m.mu.Unlock()

	if err := m.dispatchOffer(&rec, len(entries)); err != nil {
		m.cleanupTransferMem(transferID)
		return nil, err
	}
	return &rec, nil
}

func (m *Manager) sendPathsInternal(ctx context.Context, peerID string, localPaths []string, stagingDir string) (*store.DropTransferRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	peerID = protocol.NormalizeDeviceID(peerID)
	if len(localPaths) == 0 {
		return nil, errors.New("aucun fichier ou dossier sélectionné")
	}

	entries, srcMap, totalSize, fileCount, err := collectPathsForDrop(localPaths)
	if err != nil {
		return nil, err
	}
	if fileCount == 0 {
		return nil, errors.New("no files found to send")
	}

	transferID := newTransferID()
	title := filepath.Base(localPaths[0])
	if len(localPaths) > 1 {
		title = fmt.Sprintf("%s (+%d)", title, len(localPaths)-1)
	}

	peerName := peerID
	if dev, err := m.store.GetDevice(peerID); err == nil && dev.Name != "" {
		peerName = dev.Name
	}

	srcJSON, _ := json.Marshal(srcMap)
	rec := store.DropTransferRecord{
		TransferID:      transferID,
		Direction:       "send",
		Kind:            "files",
		PeerID:          peerID,
		PeerName:        peerName,
		Title:           title,
		SourceRoot:      string(srcJSON),
		StagingDir:      stagingDir,
		TotalSize:       totalSize,
		Status:          "preparing",
		Files:           entries,
		CompletedBlocks: map[string][]int{},
	}

	if err := m.store.UpsertDropTransfer(rec); err != nil {
		return nil, err
	}

	prepCtx, prepCancel := context.WithCancel(m.getRootContext())
	m.mu.Lock()
	m.sourceMaps[transferID] = srcMap
	if stagingDir != "" {
		m.stagingDirs[transferID] = stagingDir
	}
	m.activeCanc[transferID] = prepCancel
	m.mu.Unlock()

	m.emitUpdateForce()
	m.launchBg(func() {
		m.prepareAndDispatchTransfer(prepCtx, prepCancel, transferID, rec, entries, srcMap, fileCount)
	})

	return &rec, nil
}

func (m *Manager) prepareAndDispatchTransfer(
	prepCtx context.Context,
	prepCancel context.CancelFunc,
	transferID string,
	rec store.DropTransferRecord,
	entries []protocol.DropFileEntry,
	srcMap map[string]string,
	fileCount int,
) {
	defer func() {
		prepCancel()
		m.mu.Lock()
		delete(m.activeCanc, transferID)
		m.mu.Unlock()
	}()

	type hashJob struct {
		idx       int
		localPath string
	}
	var jobs []hashJob
	for i, e := range entries {
		if e.IsDir {
			continue
		}
		if lp, ok := srcMap[e.RelPath]; ok {
			jobs = append(jobs, hashJob{idx: i, localPath: lp})
		}
	}

	if len(jobs) > 0 {
		workers := runtime.NumCPU()
		if workers > 8 {
			workers = 8
		}
		if workers < 1 {
			workers = 1
		}
		if len(jobs) < workers {
			workers = len(jobs)
		}

		jobCh := make(chan hashJob, len(jobs))
		for _, j := range jobs {
			jobCh <- j
		}
		close(jobCh)

		var firstErr error
		var errOnce sync.Once
		var wg sync.WaitGroup
		var hashedTotalSize atomic.Int64

		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range jobCh {
					if prepCtx.Err() != nil {
						return
					}
					if entries[j.idx].Size == 0 {
						// Verify readable and record empty block metadata cleanly
						if _, err := os.Stat(j.localPath); err != nil {
							errOnce.Do(func() {
								firstErr = err
								prepCancel()
							})
							return
						}
						entries[j.idx].BlockSize = chunker.AdaptiveBlockSize(0)
						entries[j.idx].FileHash = chunker.ComputeMerkleFileHash(nil)
						continue
					}
					blocks, fileHash, sz, blkSize, err := chunker.HashFileBlocksAdaptive(j.localPath)
					if err != nil {
						errOnce.Do(func() {
							firstErr = err
							prepCancel()
						})
						return
					}
					entries[j.idx].Size = sz
					entries[j.idx].BlockSize = blkSize
					entries[j.idx].Blocks = blocks
					entries[j.idx].FileHash = fileHash
					hashedTotalSize.Add(sz)
				}
			}()
		}
		wg.Wait()

		if firstErr != nil || prepCtx.Err() != nil {
			latest, _ := m.store.GetDropTransfer(transferID)
			if latest != nil && latest.Status == "canceled" {
				m.cleanupTransferMem(transferID)
				m.emitUpdateForce()
				return
			}
			errMsg := ""
			if firstErr != nil {
				errMsg = firstErr.Error()
			} else if prepCtx.Err() != nil {
				errMsg = prepCtx.Err().Error()
			}
			_ = m.store.UpdateDropStatusOnly(transferID, "failed", errMsg)
			m.cleanupTransferMem(transferID)
			m.emitUpdateForce()
			return
		}
		rec.TotalSize = hashedTotalSize.Load()
	}

	latest, err := m.store.GetDropTransfer(transferID)
	if err == nil && latest.Status == "canceled" {
		m.cleanupTransferMem(transferID)
		m.emitUpdateForce()
		return
	}

	rec.Files = entries

	m.mu.Lock()
	shouldPause := m.pauseAfterPrep[transferID]
	delete(m.pauseAfterPrep, transferID)
	if shouldPause {
		m.pausedBeforeOffer[transferID] = true
	}
	m.mu.Unlock()

	if shouldPause {
		rec.Status = "paused"
		_ = m.store.UpsertDropTransfer(rec)
		m.emitUpdateForce()
		return
	}

	if err := m.dispatchOffer(&rec, fileCount); err != nil {
		m.emitUpdateForce()
	}
}

func (m *Manager) dispatchOffer(rec *store.DropTransferRecord, fileCount int) error {
	if !m.p2p.IsConnected(rec.PeerID) {
		rec.Status = "queued_offline"
		rec.ErrorMsg = ""
		if err := m.store.UpsertDropTransfer(*rec); err != nil {
			return err
		}
		m.emitUpdateForce()
		return nil
	}

	rec.Status = "pending_accept"
	rec.ErrorMsg = ""
	if err := m.store.UpsertDropTransfer(*rec); err != nil {
		return err
	}

	offer := protocol.DropOfferMsg{
		TransferID: rec.TransferID,
		SenderName: m.cfg.Snapshot().NodeName,
		Kind:       "files",
		Title:      rec.Title,
		TotalSize:  rec.TotalSize,
		FileCount:  fileCount,
		Files:      rec.Files,
	}
	if err := m.p2p.SendControl(rec.PeerID, protocol.CtrlDropOffer, offer); err != nil {
		slog.Error("drop offer dispatch failed", "peer", rec.PeerID, "transfer", rec.TransferID, "err", err)
		if !m.p2p.IsConnected(rec.PeerID) {
			rec.Status = "queued_offline"
			rec.ErrorMsg = ""
			_ = m.store.UpsertDropTransfer(*rec)
			m.emitUpdateForce()
			return nil
		}
		rec.Status = "failed"
		rec.ErrorMsg = err.Error()
		_ = m.store.UpsertDropTransfer(*rec)
		m.emitUpdateForce()
		return err
	}

	m.emitUpdateForce()
	return nil
}

func (m *Manager) deliverQueuedTransfer(ctx context.Context, transferID string) {
	if ctx.Err() != nil {
		return
	}
	rec, err := m.store.GetDropTransfer(transferID)
	if err != nil || rec.Status != "queued_offline" {
		return
	}
	if !m.p2p.IsConnected(rec.PeerID) {
		return
	}

	if rec.Kind == "text" {
		offer := protocol.DropOfferMsg{
			TransferID:  rec.TransferID,
			SenderName:  m.cfg.Snapshot().NodeName,
			Kind:        "text",
			Title:       rec.Title,
			TextContent: rec.TextContent,
			MimeType:    "text/plain",
			TotalSize:   rec.TotalSize,
		}
		if err := m.p2p.SendControl(rec.PeerID, protocol.CtrlDropOffer, offer); err == nil {
			rec.Status = "completed"
			rec.TransferredBytes = rec.TotalSize
			_ = m.store.UpsertDropTransfer(*rec)
			m.bytesTx.Add(uint64(len(rec.TextContent)))
			m.emitUpdateForce()
		}
		return
	}

	fileCount := 0
	for _, f := range rec.Files {
		if !f.IsDir {
			fileCount++
		}
	}
	_ = m.dispatchOffer(rec, fileCount)
}

func (m *Manager) HandleOffer(ctx context.Context, peerID string, raw json.RawMessage) {
	var offer protocol.DropOfferMsg
	if err := json.Unmarshal(raw, &offer); err != nil {
		return
	}
	peerID = protocol.NormalizeDeviceID(peerID)

	peerName := offer.SenderName
	autoAccept := false
	if dev, err := m.store.GetDevice(peerID); err == nil {
		if dev.Name != "" {
			peerName = dev.Name
		}
		autoAccept = dev.AutoAcceptDrop
	}

	if offer.Kind == "text" {
		rec := store.DropTransferRecord{
			TransferID:       offer.TransferID,
			Direction:        "receive",
			Kind:             "text",
			PeerID:           peerID,
			PeerName:         peerName,
			Title:            offer.Title,
			TextContent:      offer.TextContent,
			TotalSize:        int64(len(offer.TextContent)),
			TransferredBytes: int64(len(offer.TextContent)),
			Status:           "completed",
			CompletedBlocks:  map[string][]int{},
		}
		if err := m.store.UpsertDropTransfer(rec); err == nil {
			m.bytesRx.Add(uint64(len(offer.TextContent)))
			m.emitUpdateForce()
		}
		return
	}

	if existing, err := m.store.GetDropTransfer(offer.TransferID); err == nil && existing != nil {
		return
	}

	rec := store.DropTransferRecord{
		TransferID:      offer.TransferID,
		Direction:       "receive",
		Kind:            "files",
		PeerID:          peerID,
		PeerName:        peerName,
		Title:           offer.Title,
		DestDir:         m.cfg.Snapshot().DropInboxDir,
		TotalSize:       offer.TotalSize,
		Status:          "pending_accept",
		Files:           offer.Files,
		CompletedBlocks: map[string][]int{},
	}

	if err := m.store.UpsertDropTransfer(rec); err != nil {
		return
	}

	if autoAccept {
		_ = m.RespondOffer(offer.TransferID, true, rec.DestDir)
		return
	}
	m.emitUpdateForce()
}

func (m *Manager) RespondOffer(transferID string, accept bool, destDir string) error {
	rec, err := m.store.GetDropTransfer(transferID)
	if err != nil {
		return err
	}
	if rec.Direction != "receive" {
		return errors.New("not a receiving transfer")
	}

	if !accept {
		slog.Info("drop offer rejected", "peer", rec.PeerID, "transfer", transferID)
		rec.Status = "rejected"
		_ = m.store.UpdateDropProgress(transferID, "rejected", "", rec.TransferredBytes, rec.CompletedBlocks)
		_ = m.p2p.SendControl(rec.PeerID, protocol.CtrlDropResponse, protocol.DropResponseMsg{
			TransferID: transferID,
			Accepted:   false,
			Reason:     "rejected by user",
		})
		_ = m.store.PruneCompletedDropBlocks(transferID)
		m.cleanupTransferMem(transferID)
		m.emitUpdateForce()
		return nil
	}

	if strings.TrimSpace(destDir) == "" {
		destDir = m.cfg.Snapshot().DropInboxDir
	}
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return err
	}

	if rec.TotalSize > 0 {
		if freeBytes, dErr := syncer.AvailableDiskBytes(destDir); dErr == nil && freeBytes < uint64(rec.TotalSize) {
			return fmt.Errorf("espace disque insuffisant dans %s (%d Mo requis, %d Mo disponibles)",
				destDir, rec.TotalSize/(1024*1024)+1, freeBytes/(1024*1024))
		}
	}

	rec.DestDir = destDir
	rec.Status = "transferring"
	if err := m.store.UpsertDropTransfer(*rec); err != nil {
		return err
	}

	if err := m.p2p.SendControl(rec.PeerID, protocol.CtrlDropResponse, protocol.DropResponseMsg{
		TransferID: transferID,
		Accepted:   true,
	}); err != nil {
		slog.Error("drop accept response send failed", "peer", rec.PeerID, "transfer", transferID, "err", err)
		rec.Status = "pending_accept"
		_ = m.store.UpdateDropProgress(transferID, "pending_accept", "", rec.TransferredBytes, rec.CompletedBlocks)
		m.emitUpdateForce()
		return err
	}

	for _, f := range rec.Files {
		if f.IsDir {
			if p, err := safeJoin(destDir, f.RelPath); err == nil {
				if mErr := os.MkdirAll(p, 0755); mErr != nil {
					slog.Warn("drop: folder creation failed", "err", mErr, "path", p)
				}
			}
		} else if f.Size == 0 {
			if p, err := safeJoin(destDir, f.RelPath); err == nil {
				if mErr := os.MkdirAll(filepath.Dir(p), 0755); mErr != nil {
					slog.Warn("drop: parent folder creation failed", "err", mErr, "path", p)
				}
				finalEmpty := uniqueDestPath(p)
				_ = os.WriteFile(finalEmpty, nil, 0644)
			}
		}
	}

	m.emitUpdateForce()
	return nil
}

func (m *Manager) HandleResponse(ctx context.Context, peerID string, raw json.RawMessage) {
	var resp protocol.DropResponseMsg
	if err := json.Unmarshal(raw, &resp); err != nil {
		return
	}
	rec, err := m.store.GetDropTransfer(resp.TransferID)
	if err != nil || rec.PeerID != protocol.NormalizeDeviceID(peerID) {
		return
	}
	if !resp.Accepted {
		_ = m.store.UpdateDropProgress(resp.TransferID, "rejected", resp.Reason, rec.TransferredBytes, rec.CompletedBlocks)
		_ = m.store.PruneCompletedDropBlocks(resp.TransferID)
		m.cleanupTransferMem(resp.TransferID)
		m.emitUpdateForce()
		return
	}

	_ = m.store.UpdateDropProgress(resp.TransferID, "transferring", "", rec.TransferredBytes, rec.CompletedBlocks)
	m.emitUpdateForce()

	m.mu.Lock()
	_, alreadyRunning := m.activeCanc[resp.TransferID]
	m.mu.Unlock()
	if !alreadyRunning {
		tid := resp.TransferID
		m.launchBg(func() {
			m.runSendTransfer(m.getRootContext(), tid)
		})
	}
}

func (m *Manager) PauseTransfer(transferID string) error {
	rec, err := m.store.GetDropTransfer(transferID)
	if err != nil {
		return err
	}
	if rec.Status == "preparing" {
		m.mu.Lock()
		m.pauseAfterPrep[transferID] = true
		m.mu.Unlock()
		return nil
	}

	wasTransferring := rec.Status == "transferring"
	if wasTransferring {
		_ = m.store.UpdateDropProgress(transferID, "paused", "", rec.TransferredBytes, rec.CompletedBlocks)
	}

	// Flush pending receive progress BEFORE deleting recvBuffers so up to 15 blocks aren't lost.
	mu := m.getReceiveMutex(transferID)
	mu.Lock()
	m.flushRecvBlocks(transferID)
	m.mu.Lock()
	cancel, ok := m.activeCanc[transferID]
	delete(m.recvBuffers, transferID)
	m.mu.Unlock()
	mu.Unlock()

	if ok && wasTransferring {
		cancel()
	}
	if wasTransferring {
		slog.Info("drop transfer paused", "peer", rec.PeerID, "transfer", transferID)
		_ = m.p2p.SendControl(rec.PeerID, protocol.CtrlDropPause, protocol.DropPauseMsg{
			TransferID: transferID,
			Reason:     "paused by user",
		})
	}
	m.emitUpdateForce()
	return nil
}

func (m *Manager) HandlePause(peerID string, raw json.RawMessage) {
	var msg protocol.DropPauseMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	rec, err := m.store.GetDropTransfer(msg.TransferID)
	if err != nil || rec.PeerID != protocol.NormalizeDeviceID(peerID) {
		return
	}
	if rec.Status == "transferring" {
		_ = m.store.UpdateDropProgress(msg.TransferID, "paused", "", rec.TransferredBytes, rec.CompletedBlocks)
	}

	mu := m.getReceiveMutex(msg.TransferID)
	mu.Lock()
	m.flushRecvBlocks(msg.TransferID)
	m.mu.Lock()
	cancel, ok := m.activeCanc[msg.TransferID]
	delete(m.recvBuffers, msg.TransferID)
	m.mu.Unlock()
	mu.Unlock()

	if ok {
		cancel()
	}
	m.emitUpdateForce()
}

func (m *Manager) HandleResume(peerID string, raw json.RawMessage) {
	var msg protocol.DropPauseMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	rec, err := m.store.GetDropTransfer(msg.TransferID)
	if err != nil || rec.PeerID != protocol.NormalizeDeviceID(peerID) {
		return
	}
	if rec.Status == "paused" || rec.Status == "failed" {
		_ = m.store.UpdateDropProgress(msg.TransferID, "transferring", "", rec.TransferredBytes, rec.CompletedBlocks)
		m.emitUpdateForce()
	}
}

func (m *Manager) CancelTransfer(transferID string) error {
	rec, err := m.store.GetDropTransfer(transferID)
	if err != nil {
		return err
	}
	wasLocalOnly := rec.Status == "preparing" || rec.Status == "queued_offline"

	_ = m.store.UpdateDropProgress(transferID, "canceled", "canceled by user", rec.TransferredBytes, rec.CompletedBlocks)

	m.mu.Lock()
	delete(m.pauseAfterPrep, transferID)
	delete(m.pausedBeforeOffer, transferID)
	cancel, ok := m.activeCanc[transferID]
	m.mu.Unlock()
	if ok {
		cancel()
	}

	if rec.Direction == "receive" && rec.DestDir != "" {
		m.cleanupTempFiles(rec)
	}
	if !wasLocalOnly {
		slog.Info("drop transfer canceled", "peer", rec.PeerID, "transfer", transferID)
		_ = m.p2p.SendControl(rec.PeerID, protocol.CtrlDropCancel, protocol.DropCancelMsg{
			TransferID: transferID,
			Reason:     "canceled by user",
		})
	}
	_ = m.store.PruneCompletedDropBlocks(transferID)
	m.cleanupTransferMem(transferID)
	m.emitUpdateForce()
	return nil
}

func (m *Manager) DeleteTransfer(transferID string) error {
	if rec, err := m.store.GetDropTransfer(transferID); err == nil {
		if rec.Status == "transferring" || rec.Status == "pending_accept" || rec.Status == "preparing" || rec.Status == "queued_offline" {
			_ = m.CancelTransfer(transferID)
		}
	}
	m.cleanupTransferMem(transferID)
	err := m.store.DeleteDropTransfer(transferID)
	m.emitUpdateForce()
	return err
}

func (m *Manager) HandleCancel(peerID string, raw json.RawMessage) {
	var msg protocol.DropCancelMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	rec, err := m.store.GetDropTransfer(msg.TransferID)
	if err != nil || rec.PeerID != protocol.NormalizeDeviceID(peerID) {
		return
	}
	_ = m.store.UpdateDropProgress(msg.TransferID, "canceled", firstNonEmpty(msg.Reason, "canceled by peer"), rec.TransferredBytes, rec.CompletedBlocks)

	m.mu.Lock()
	cancel, ok := m.activeCanc[msg.TransferID]
	m.mu.Unlock()
	if ok {
		cancel()
	}
	if rec.Direction == "receive" && rec.DestDir != "" {
		m.cleanupTempFiles(rec)
	}
	_ = m.store.PruneCompletedDropBlocks(msg.TransferID)
	m.cleanupTransferMem(msg.TransferID)
	m.emitUpdateForce()
}

func (m *Manager) ResumeTransfer(ctx context.Context, transferID string) error {
	rec, err := m.store.GetDropTransfer(transferID)
	if err != nil {
		return err
	}

	// If still preparing asynchronously, simply clear any pending pause flag.
	if rec.Status == "preparing" {
		m.mu.Lock()
		delete(m.pauseAfterPrep, transferID)
		m.mu.Unlock()
		return nil
	}

	if rec.Status == "queued_offline" && rec.Direction == "send" {
		if m.p2p.IsConnected(rec.PeerID) {
			m.launchBg(func() {
				m.deliverQueuedTransfer(m.getRootContext(), transferID)
			})
		}
		return nil
	}

	if rec.Status != "paused" && rec.Status != "failed" {
		return nil
	}

	if rec.Direction == "receive" {
		_ = m.store.UpdateDropProgress(transferID, "transferring", "", rec.TransferredBytes, rec.CompletedBlocks)
		if err := m.p2p.SendControl(rec.PeerID, protocol.CtrlDropResponse, protocol.DropResponseMsg{
			TransferID: transferID,
			Accepted:   true,
		}); err != nil {
			_ = m.store.UpdateDropProgress(transferID, "paused", "", rec.TransferredBytes, rec.CompletedBlocks)
			m.emitUpdateForce()
			return err
		}
		m.emitUpdateForce()
		return nil
	}

	m.mu.Lock()
	beforeOffer := m.pausedBeforeOffer[transferID]
	delete(m.pausedBeforeOffer, transferID)
	m.mu.Unlock()

	if beforeOffer {
		fileCount := 0
		for _, f := range rec.Files {
			if !f.IsDir {
				fileCount++
			}
		}
		return m.dispatchOffer(rec, fileCount)
	}

	_ = m.store.UpdateDropProgress(transferID, "transferring", "", rec.TransferredBytes, rec.CompletedBlocks)
	slog.Info("drop transfer resumed", "peer", rec.PeerID, "transfer", transferID)
	_ = m.p2p.SendControl(rec.PeerID, protocol.CtrlDropResume, protocol.DropPauseMsg{
		TransferID: transferID,
	})
	m.emitUpdateForce()
	m.launchBg(func() {
		m.runSendTransfer(m.getRootContext(), transferID)
	})
	return nil
}

func (m *Manager) runSendTransfer(parentCtx context.Context, transferID string) {
	rec, err := m.store.GetDropTransfer(transferID)
	if err != nil {
		return
	}

	srcMap := m.loadSourceMap(rec)
	if len(srcMap) == 0 {
		_ = m.store.UpdateDropProgress(transferID, "failed", "source paths unavailable", rec.TransferredBytes, rec.CompletedBlocks)
		m.emitUpdateForce()
		return
	}

	tCtx, cancel := context.WithCancel(parentCtx)
	m.mu.Lock()
	m.activeCanc[transferID] = cancel
	m.mu.Unlock()
	defer func() {
		cancel()
		m.mu.Lock()
		delete(m.activeCanc, transferID)
		m.mu.Unlock()
	}()

	type chunkJob struct {
		relPath   string
		localFile string
		idx       int
		blk       protocol.BlockInfo
	}
	var allJobs []chunkJob
	for _, entry := range rec.Files {
		if entry.IsDir || entry.Size == 0 {
			continue
		}
		localFile, ok := srcMap[entry.RelPath]
		if !ok {
			continue
		}
		doneSet := make(map[int]bool, len(rec.CompletedBlocks[entry.RelPath]))
		for _, idx := range rec.CompletedBlocks[entry.RelPath] {
			doneSet[idx] = true
		}
		for idx, blk := range entry.Blocks {
			if !doneSet[idx] {
				allJobs = append(allJobs, chunkJob{
					relPath:   entry.RelPath,
					localFile: localFile,
					idx:       idx,
					blk:       blk,
				})
			}
		}
	}

	if len(allJobs) > 0 {
		workers := 6
		if len(allJobs) < workers {
			workers = len(allJobs)
		}
		jobs := make(chan chunkJob, len(allJobs))
		for _, j := range allJobs {
			jobs <- j
		}
		close(jobs)

		var recMu sync.Mutex
		var lastTransferred atomic.Int64
		lastTransferred.Store(rec.TransferredBytes)
		var firstErr error
		var errOnce sync.Once
		var pending []store.DropBlockCompletion
		lastFlush := time.Now()
		var wg sync.WaitGroup

		flushPending := func(force bool) {
			recMu.Lock()
			if len(pending) == 0 || (!force && len(pending) < 16 && time.Since(lastFlush) < 400*time.Millisecond) {
				recMu.Unlock()
				return
			}
			batch := pending
			pending = nil
			lastFlush = time.Now()
			recMu.Unlock()

			if total, err := m.store.MarkDropBlocksCompleted(transferID, batch); err == nil {
				lastTransferred.Store(total)
			}
			m.emitUpdate()
		}

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Pipeline up to 4 in-flight chunks per QUIC DataReqDropChunk stream to eliminate per-chunk RTT stalls
				const pipelineWindow = 4
				var stream *quic.Stream
				var release func()
				var ackQueue chan chunkJob
				var ackErrCh chan error
				var ackWg sync.WaitGroup

				closeWorkerStream := func(waitAcks bool) {
					if ackQueue != nil {
						close(ackQueue)
						ackQueue = nil
					}
					if waitAcks {
						ackWg.Wait()
					}
					if stream != nil {
						_ = stream.Close()
						stream = nil
					}
					if !waitAcks {
						ackWg.Wait()
					}
					if release != nil {
						release()
						release = nil
					}
				}
				defer func() { closeWorkerStream(false) }()

				bufPtr := chunker.GetBuffer()
				defer chunker.PutBuffer(bufPtr)

				for job := range jobs {
					if tCtx.Err() != nil {
						return
					}
					if stream == nil {
						s, rel, sErr := m.p2p.OpenTrackedDataStream(tCtx, rec.PeerID, protocol.DataReqDropChunk)
						if sErr != nil {
							errOnce.Do(func() {
								firstErr = sErr
								cancel()
							})
							return
						}
						stream = s
						release = rel
						ackQueue = make(chan chunkJob, pipelineWindow)
						ackErrCh = make(chan error, 1)
						curStream := stream
						curAckQ := ackQueue
						curErrCh := ackErrCh
						ackWg.Add(1)
						go func() {
							defer ackWg.Done()
							for sentJob := range curAckQ {
								_ = curStream.SetReadDeadline(time.Now().Add(streamDeadline))
								var ack protocol.DropChunkAck
								if err := protocol.ReadFrame(curStream, &ack); err != nil {
									select {
									case curErrCh <- err:
									default:
									}
									return
								}
								if !ack.OK {
									select {
									case curErrCh <- errors.New(ack.Error):
									default:
									}
									return
								}
								m.bytesTx.Add(uint64(sentJob.blk.Size))
								recMu.Lock()
								pending = append(pending, store.DropBlockCompletion{
									RelPath:  sentJob.relPath,
									BlockIdx: sentJob.idx,
									Size:     int64(sentJob.blk.Size),
								})
								shouldFlush := len(pending) >= 16 || time.Since(lastFlush) >= 400*time.Millisecond
								recMu.Unlock()
								if shouldFlush {
									flushPending(true)
								}
							}
						}()
					}

					if err := m.writeChunkNoAck(tCtx, stream, transferID, job.relPath, job.idx, job.blk, job.localFile, bufPtr); err != nil {
						closeWorkerStream(false)
						errOnce.Do(func() {
							firstErr = err
							cancel()
						})
						return
					}

					select {
					case ackQueue <- job:
					case aErr := <-ackErrCh:
						closeWorkerStream(false)
						errOnce.Do(func() {
							firstErr = aErr
							cancel()
						})
						return
					case <-tCtx.Done():
						closeWorkerStream(false)
						return
					}
				}

				if stream != nil {
					closeWorkerStream(true)
					select {
					case aErr := <-ackErrCh:
						errOnce.Do(func() {
							firstErr = aErr
							cancel()
						})
					default:
					}
				}
			}()
		}
		wg.Wait()
		flushPending(true)

		for _, p := range srcMap {
			m.handles.ClosePath(p)
		}

		if firstErr != nil || tCtx.Err() != nil {
			latest, _ := m.store.GetDropTransfer(transferID)
			if latest != nil && (latest.Status == "paused" || latest.Status == "canceled") {
				m.emitUpdateForce()
				return
			}
			errMsg := ""
			if firstErr != nil {
				errMsg = firstErr.Error()
			} else {
				errMsg = tCtx.Err().Error()
			}
			_ = m.store.UpdateDropStatusOnly(transferID, "failed", errMsg)
			m.emitUpdateForce()
			return
		}
	}

	if err := m.p2p.SendControl(rec.PeerID, protocol.CtrlDropComplete, protocol.DropCompleteMsg{
		TransferID: transferID,
	}); err != nil {
		_ = m.store.UpdateDropStatusOnly(transferID, "failed", err.Error())
		m.emitUpdateForce()
		return
	}

	_ = m.store.UpdateDropProgress(transferID, "completed", "", rec.TotalSize, nil)
	_ = m.store.PruneCompletedDropBlocks(transferID)
	m.cleanupTransferMem(transferID)
	m.emitUpdateForce()
}

func (m *Manager) cleanupTransferMem(transferID string) {
	m.mu.Lock()
	dir, hasStage := m.stagingDirs[transferID]
	delete(m.stagingDirs, transferID)
	delete(m.sourceMaps, transferID)
	delete(m.receiveLock, transferID)
	delete(m.recvBuffers, transferID)
	delete(m.pauseAfterPrep, transferID)
	delete(m.pausedBeforeOffer, transferID)
	m.mu.Unlock()

	if (!hasStage || dir == "") && m.store != nil {
		if rec, err := m.store.GetDropTransfer(transferID); err == nil && rec.StagingDir != "" {
			dir = rec.StagingDir
		}
	}
	if dir != "" {
		if err := os.RemoveAll(dir); err != nil {
			slog.Warn("drop: staging dir cleanup failed", "err", err, "path", dir)
		}
		_ = m.store.ClearDropStagingDir(transferID)
	}
}

func (m *Manager) cleanupTempFiles(rec *store.DropTransferRecord) {
	for _, entry := range rec.Files {
		if entry.IsDir || entry.Size == 0 {
			continue
		}
		if tmpPath, err := tempTransferPath(rec.DestDir, rec.TransferID, entry.RelPath); err == nil {
			m.handles.ClosePath(tmpPath)
			_ = os.Remove(tmpPath) // best effort: le fichier temporaire peut ne plus exister (déjà déplacé ou nettoyé)
		}
	}
}

func (m *Manager) writeChunkNoAck(ctx context.Context, stream *quic.Stream, transferID, relPath string, blockIdx int, blk protocol.BlockInfo, localFile string, defaultBuf *[]byte) error {
	var data []byte
	if blk.Size > 0 && defaultBuf != nil && blk.Size <= cap(*defaultBuf) {
		data = (*defaultBuf)[:blk.Size]
	} else {
		sizedPtr := chunker.GetBufferSize(blk.Size)
		defer chunker.PutBufferSized(sizedPtr)
		data = (*sizedPtr)[:blk.Size]
	}
	if err := m.handles.ReadBlockInto(localFile, blk.Offset, blk.Size, blk.Hash, data); err != nil {
		return err
	}

	wireData := data
	compressed := false
	if m.p2p.CompressionEnabled() {
		wireData, compressed = chunker.CompressBlockIfWorthwhile(relPath, data)
	}

	hdr := protocol.DropChunkHeader{
		TransferID: transferID,
		RelPath:    relPath,
		BlockIdx:   blockIdx,
		Offset:     blk.Offset,
		Size:       blk.Size,
		Hash:       blk.Hash,
		Compressed: compressed,
		WireSize:   len(wireData),
	}
	_ = stream.SetWriteDeadline(time.Now().Add(streamDeadline))
	if err := protocol.WriteFrame(stream, hdr); err != nil {
		return err
	}
	_, err := m.p2p.LimitedWrite(ctx, stream, wireData)
	return err
}

// ServeChunkStream receives one or more pipelined incoming Drop blocks over StreamData.
func (m *Manager) ServeChunkStream(ctx context.Context, peerID string, stream *quic.Stream) {
	defer stream.Close()
	normPeer := protocol.NormalizeDeviceID(peerID)

	defaultPtr := chunker.GetBuffer()
	defer chunker.PutBuffer(defaultPtr)

	for {
		if !m.serveOneChunk(ctx, normPeer, stream, defaultPtr) {
			return
		}
	}
}

func (m *Manager) serveOneChunk(ctx context.Context, normPeer string, stream *quic.Stream, defaultPtr *[]byte) bool {
	_ = stream.SetDeadline(time.Now().Add(streamDeadline))

	var hdr protocol.DropChunkHeader
	if err := protocol.ReadFrame(stream, &hdr); err != nil {
		return false
	}
	if hdr.Size <= 0 || hdr.Size > chunker.MaxBlockSize || hdr.Size > protocol.MaxFrameSize {
		_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: "invalid chunk size"})
		return false
	}

	readLen := hdr.Size
	if hdr.WireSize > 0 {
		readLen = hdr.WireSize
	}
	if hdr.Compressed {
		if readLen <= 0 || readLen > hdr.Size || readLen > protocol.MaxFrameSize {
			_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: "invalid compressed wire size"})
			return false
		}
	} else if readLen != hdr.Size {
		_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: "invalid wire size"})
		return false
	}

	var wireBuf []byte
	if defaultPtr != nil && readLen <= cap(*defaultPtr) {
		wireBuf = (*defaultPtr)[:readLen]
	} else {
		wirePtr := chunker.GetBufferSize(readLen)
		defer chunker.PutBufferSized(wirePtr)
		wireBuf = (*wirePtr)[:readLen]
	}

	if _, err := m.p2p.LimitedReadFull(ctx, stream, wireBuf); err != nil {
		_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: err.Error()})
		return false
	}

	rawBuf := wireBuf
	if hdr.Compressed {
		rawPtr := chunker.GetBufferSize(hdr.Size)
		defer chunker.PutBufferSized(rawPtr)
		dec, err := chunker.DecompressBlock(wireBuf, (*rawPtr)[:hdr.Size], hdr.Size)
		if err != nil {
			_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: "decompression failed"})
			return false
		}
		rawBuf = dec
	}

	if hdr.Hash != "" && chunker.HashBytes(rawBuf) != hdr.Hash {
		_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: "chunk hash mismatch"})
		return false
	}

	destDir, ok := m.getActiveRecvDestDir(hdr.TransferID, normPeer)
	if !ok {
		_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: "transfer not active"})
		return false
	}

	tmpPath, err := tempTransferPath(destDir, hdr.TransferID, hdr.RelPath)
	if err != nil {
		_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: err.Error()})
		return false
	}
	if err := os.MkdirAll(filepath.Dir(tmpPath), 0755); err != nil {
		_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: err.Error()})
		return false
	}

	if err := m.handles.WriteAt(tmpPath, hdr.Offset, rawBuf); err != nil {
		_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: "disk write error"})
		return false
	}
	m.bytesRx.Add(uint64(len(rawBuf)))

	m.recordRecvBlock(hdr.TransferID, hdr.RelPath, hdr.BlockIdx, int64(hdr.Size))
	if err := protocol.WriteFrame(stream, protocol.DropChunkAck{OK: true}); err != nil {
		return false
	}
	return true
}

func (m *Manager) getActiveRecvDestDir(transferID, normPeer string) (string, bool) {
	m.mu.Lock()
	rb, ok := m.recvBuffers[transferID]
	m.mu.Unlock()
	if ok && rb.peerID == normPeer {
		return rb.destDir, true
	}

	rec, err := m.store.GetDropTransfer(transferID)
	if err != nil || rec.PeerID != normPeer || rec.Status != "transferring" {
		return "", false
	}
	m.mu.Lock()
	if rb, ok := m.recvBuffers[transferID]; ok {
		m.mu.Unlock()
		return rb.destDir, true
	}
	m.recvBuffers[transferID] = &recvProgressBuffer{
		peerID:  normPeer,
		destDir: rec.DestDir,
		seen:    make(map[string]struct{}),
	}
	m.mu.Unlock()
	return rec.DestDir, true
}

func (m *Manager) recordRecvBlock(transferID, relPath string, blockIdx int, size int64) {
	mu := m.getReceiveMutex(transferID)
	mu.Lock()
	defer mu.Unlock()

	m.mu.Lock()
	rb, ok := m.recvBuffers[transferID]
	m.mu.Unlock()
	if !ok {
		return
	}
	key := fmt.Sprintf("%s#%d", relPath, blockIdx)
	if _, dup := rb.seen[key]; !dup {
		rb.seen[key] = struct{}{}
		rb.pending = append(rb.pending, store.DropBlockCompletion{
			RelPath:  relPath,
			BlockIdx: blockIdx,
			Size:     size,
		})
	}
	if len(rb.pending) >= 16 {
		batch := rb.pending
		rb.pending = nil
		_, _ = m.store.MarkDropBlocksCompleted(transferID, batch)
		m.emitUpdate()
	}
}

func (m *Manager) flushRecvBlocks(transferID string) {
	m.mu.Lock()
	rb, ok := m.recvBuffers[transferID]
	m.mu.Unlock()
	if !ok || len(rb.pending) == 0 {
		return
	}
	batch := rb.pending
	rb.pending = nil
	_, _ = m.store.MarkDropBlocksCompleted(transferID, batch)
}

func (m *Manager) HandleComplete(peerID string, raw json.RawMessage) {
	var msg protocol.DropCompleteMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	m.launchBg(func() {
		m.handleCompleteAsync(peerID, msg)
	})
}

func (m *Manager) handleCompleteAsync(peerID string, msg protocol.DropCompleteMsg) {
	mu := m.getReceiveMutex(msg.TransferID)
	mu.Lock()
	defer mu.Unlock()

	m.flushRecvBlocks(msg.TransferID)

	rec, err := m.store.GetDropTransfer(msg.TransferID)
	if err != nil || rec.PeerID != protocol.NormalizeDeviceID(peerID) || rec.Status == "canceled" {
		return
	}

	for _, entry := range rec.Files {
		if entry.IsDir || entry.Size == 0 {
			continue
		}
		tmpPath, err := tempTransferPath(rec.DestDir, rec.TransferID, entry.RelPath)
		if err != nil {
			_ = m.store.UpdateDropStatusOnly(rec.TransferID, "failed", err.Error())
			m.emitUpdateForce()
			return
		}
		m.handles.ClosePath(tmpPath)

		finalPath, err := safeJoin(rec.DestDir, entry.RelPath)
		if err != nil {
			_ = m.store.UpdateDropStatusOnly(rec.TransferID, "failed", err.Error())
			m.emitUpdateForce()
			return
		}

		if err := verifyCompletedEntry(tmpPath, entry, rec.CompletedBlocks[entry.RelPath]); err != nil {
			_ = m.store.UpdateDropStatusOnly(rec.TransferID, "failed", err.Error())
			m.emitUpdateForce()
			return
		}

		if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
			_ = m.store.UpdateDropStatusOnly(rec.TransferID, "failed", err.Error())
			m.emitUpdateForce()
			return
		}
		if f, oErr := os.OpenFile(tmpPath, os.O_RDWR, 0); oErr == nil {
			_ = f.Sync()
			_ = f.Close()
		}
		targetPath := uniqueDestPath(finalPath)
		if err := os.Rename(tmpPath, targetPath); err != nil {
			_ = m.store.UpdateDropStatusOnly(rec.TransferID, "failed", err.Error())
			m.emitUpdateForce()
			return
		}
	}

	_ = m.store.UpdateDropProgress(rec.TransferID, "completed", "", rec.TotalSize, nil)
	_ = m.store.PruneCompletedDropBlocks(rec.TransferID)
	m.cleanupTransferMem(rec.TransferID)
	m.emitUpdateForce()
}

func verifyCompletedEntry(tmpPath string, entry protocol.DropFileEntry, doneBlocks []int) error {
	info, statErr := os.Stat(tmpPath)
	if statErr != nil || info.Size() != entry.Size {
		return fmt.Errorf("size mismatch for %s", entry.RelPath)
	}

	merkleMatch := entry.FileHash == "" || chunker.ComputeMerkleFileHash(entry.Blocks) == entry.FileHash
	if len(doneBlocks) == len(entry.Blocks) && merkleMatch {
		return nil
	}

	blkSize := entry.BlockSize
	if blkSize <= 0 {
		blkSize = chunker.AdaptiveBlockSize(entry.Size)
	}
	_, actualHash, _, err := chunker.HashFileBlocks(tmpPath, blkSize)
	if err != nil || (entry.FileHash != "" && actualHash != entry.FileHash) {
		return fmt.Errorf("integrity verification failed for %s", entry.RelPath)
	}
	return nil
}

func collectPathsForDrop(localPaths []string) ([]protocol.DropFileEntry, map[string]string, int64, int, error) {
	var entries []protocol.DropFileEntry
	srcMap := make(map[string]string)
	usedRel := make(map[string]struct{})
	var totalSize int64
	var fileCount int

	for _, rawPath := range localPaths {
		cleanPath := filepath.Clean(rawPath)
		info, err := os.Stat(cleanPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, nil, 0, 0, fmt.Errorf("fichier introuvable sur cette machine (%s). Utilise le glisser-déposer ou 'Choisir des fichiers' si le fichier est sur un autre PC", cleanPath)
			}
			return nil, nil, 0, 0, fmt.Errorf("accès impossible à %s : %v", cleanPath, err)
		}
		baseName := filepath.Base(cleanPath)

		if !info.IsDir() {
			rel := ensureUniqueRelPath(filepath.ToSlash(baseName), usedRel)
			sz := info.Size()
			entries = append(entries, protocol.DropFileEntry{
				RelPath:   rel,
				Size:      sz,
				IsDir:     false,
				BlockSize: chunker.AdaptiveBlockSize(sz),
			})
			srcMap[rel] = cleanPath
			totalSize += sz
			fileCount++
			continue
		}

		rootRel := ensureUniqueRelPath(filepath.ToSlash(baseName), usedRel)
		err = filepath.WalkDir(cleanPath, func(cur string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.Type()&os.ModeSymlink != 0 {
				return nil
			}
			subRel, err := filepath.Rel(cleanPath, cur)
			if err != nil {
				return err
			}
			var rel string
			if subRel == "." {
				rel = rootRel
			} else {
				rel = path.Join(rootRel, filepath.ToSlash(subRel))
			}
			usedRel[rel] = struct{}{}
			if d.IsDir() {
				entries = append(entries, protocol.DropFileEntry{
					RelPath: rel,
					IsDir:   true,
				})
				return nil
			}
			fInfo, err := d.Info()
			if err != nil {
				return err
			}
			sz := fInfo.Size()
			entries = append(entries, protocol.DropFileEntry{
				RelPath:   rel,
				Size:      sz,
				IsDir:     false,
				BlockSize: chunker.AdaptiveBlockSize(sz),
			})
			srcMap[rel] = cur
			totalSize += sz
			fileCount++
			return nil
		})
		if err != nil {
			return nil, nil, 0, 0, err
		}
	}

	return entries, srcMap, totalSize, fileCount, nil
}

func ensureUniqueRelPath(rel string, used map[string]struct{}) string {
	clean := strings.TrimLeft(rel, "/")
	if clean == "" {
		clean = "file"
	}
	if _, exists := used[clean]; !exists {
		used[clean] = struct{}{}
		return clean
	}
	dir := path.Dir(clean)
	base := path.Base(clean)
	ext := path.Ext(base)
	stem := strings.TrimSuffix(base, ext)
	for i := 1; i < 10000; i++ {
		name := fmt.Sprintf("%s (%d)%s", stem, i, ext)
		cand := name
		if dir != "." {
			cand = path.Join(dir, name)
		}
		if _, exists := used[cand]; !exists {
			used[cand] = struct{}{}
			return cand
		}
	}
	return clean
}

func (m *Manager) loadSourceMap(rec *store.DropTransferRecord) map[string]string {
	m.mu.Lock()
	if cached, ok := m.sourceMaps[rec.TransferID]; ok {
		m.mu.Unlock()
		return cached
	}
	m.mu.Unlock()

	var parsed map[string]string
	if err := json.Unmarshal([]byte(rec.SourceRoot), &parsed); err == nil && len(parsed) > 0 {
		m.mu.Lock()
		m.sourceMaps[rec.TransferID] = parsed
		m.mu.Unlock()
		return parsed
	}
	return nil
}

func (m *Manager) getReceiveMutex(transferID string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	mu, ok := m.receiveLock[transferID]
	if !ok {
		mu = &sync.Mutex{}
		m.receiveLock[transferID] = mu
	}
	return mu
}

func (m *Manager) emitUpdate() {
	if m.onEmit == nil {
		return
	}
	now := time.Now().UnixNano()
	prev := m.lastEmitNs.Load()
	if now-prev < int64(200*time.Millisecond) {
		return
	}
	if !m.lastEmitNs.CompareAndSwap(prev, now) {
		return
	}
	list, err := m.store.ListDropTransfersSummary()
	if err == nil {
		m.onEmit("drop_updated", list)
	}
}

func (m *Manager) emitUpdateForce() {
	if m.onEmit == nil {
		return
	}
	m.lastEmitNs.Store(time.Now().UnixNano())
	list, err := m.store.ListDropTransfersSummary()
	if err == nil {
		m.onEmit("drop_updated", list)
	}
}

func uniqueDestPath(path string) string {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return path
	}
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	stem := strings.TrimSuffix(base, ext)
	for i := 1; i < 10000; i++ {
		cand := filepath.Join(dir, fmt.Sprintf("%s (%d)%s", stem, i, ext))
		if _, err := os.Stat(cand); os.IsNotExist(err) {
			return cand
		}
	}
	return path
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func tempTransferPath(destDir, transferID, relPath string) (string, error) {
	finalPath, err := safeJoin(destDir, relPath)
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(finalPath), fmt.Sprintf(".%s.gens.tmp.%s", filepath.Base(finalPath), transferID)), nil
}

func safeJoin(root, rel string) (string, error) {
	cleanRel := filepath.Clean(filepath.FromSlash(rel))
	if cleanRel == "." || filepath.IsAbs(cleanRel) || strings.HasPrefix(cleanRel, "..") {
		return "", fmt.Errorf("unsafe relative path: %s", rel)
	}
	full := filepath.Join(root, cleanRel)
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	absFull, err := filepath.Abs(full)
	if err != nil {
		return "", err
	}
	if absFull != absRoot && !strings.HasPrefix(absFull, absRoot+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes destination: %s", rel)
	}
	return full, nil
}

func newTransferID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
