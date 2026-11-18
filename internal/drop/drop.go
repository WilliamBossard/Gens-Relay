package drop

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WilliamBossard/Gens-Relay/internal/chunker"
	"github.com/WilliamBossard/Gens-Relay/internal/config"
	"github.com/WilliamBossard/Gens-Relay/internal/p2p"
	"github.com/WilliamBossard/Gens-Relay/internal/store"
	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
	"github.com/quic-go/quic-go"
)

type recvProgressBuffer struct {
	peerID  string
	destDir string
	pending []store.DropBlockCompletion
	seen    map[string]struct{}
}

type Manager struct {
	cfg     *config.DaemonConfig
	store   *store.Store
	p2p     *p2p.Manager
	onEmit  func(event string, data any)
	handles *chunker.FileHandleCache
	rootCtx context.Context

	mu          sync.Mutex
	sourceMaps  map[string]map[string]string // transferID -> relPath -> local fullPath
	stagingDirs map[string]string            // transferID -> staging dir to clean after finish
	activeCanc  map[string]context.CancelFunc
	receiveLock map[string]*sync.Mutex
	recvBuffers map[string]*recvProgressBuffer
	lastEmitNs  atomic.Int64
	bytesRx     atomic.Uint64
	bytesTx     atomic.Uint64
}

func NewManager(cfg *config.DaemonConfig, st *store.Store, p2pMgr *p2p.Manager, onEmit func(string, any)) *Manager {
	return &Manager{
		cfg:         cfg,
		store:       st,
		p2p:         p2pMgr,
		onEmit:      onEmit,
		handles:     chunker.NewFileHandleCache(48, 5*time.Second),
		rootCtx:     context.Background(),
		sourceMaps:  make(map[string]map[string]string),
		stagingDirs: make(map[string]string),
		activeCanc:  make(map[string]context.CancelFunc),
		receiveLock: make(map[string]*sync.Mutex),
		recvBuffers: make(map[string]*recvProgressBuffer),
	}
}

func (m *Manager) SetRootContext(ctx context.Context) {
	m.rootCtx = ctx
	go func() {
		<-ctx.Done()
		m.handles.CloseAll()
	}()
}

func (m *Manager) OnPeerConnected(ctx context.Context, peerID string) {
	ids, err := m.store.ListResumableSendDropIDs(peerID)
	if err != nil {
		return
	}
	for _, id := range ids {
		m.mu.Lock()
		_, running := m.activeCanc[id]
		m.mu.Unlock()
		if !running {
			go m.runSendTransfer(m.rootCtx, id)
		}
	}
}

// SendText sends an instant clipboard/text snippet to a connected peer.
func (m *Manager) SendText(ctx context.Context, peerID, text string) (*store.DropTransferRecord, error) {
	peerID = protocol.NormalizeDeviceID(peerID)
	if !m.p2p.IsConnected(peerID) {
		return nil, fmt.Errorf("appareil %s non connecté en P2P", peerID)
	}
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
		TransferID:       transferID,
		Direction:        "send",
		Kind:             "text",
		PeerID:           peerID,
		PeerName:         peerName,
		Title:            title,
		TextContent:      text,
		TotalSize:        int64(len(text)),
		TransferredBytes: int64(len(text)),
		Status:           "completed",
		CompletedBlocks:  map[string][]int{},
	}
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
		rec.Status = "failed"
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

// SendPaths creates a Drop offer for one or more local files/directories and sends it to peerID.
func (m *Manager) SendPaths(ctx context.Context, peerID string, localPaths []string) (*store.DropTransferRecord, error) {
	return m.sendPathsInternal(ctx, peerID, localPaths, "")
}

// SendStagedPaths sends uploaded/staged files and cleans up stagingDir once the transfer completes or is rejected.
func (m *Manager) SendStagedPaths(ctx context.Context, peerID, stagingDir string, localPaths []string) (*store.DropTransferRecord, error) {
	return m.sendPathsInternal(ctx, peerID, localPaths, stagingDir)
}

func (m *Manager) sendPathsInternal(ctx context.Context, peerID string, localPaths []string, stagingDir string) (*store.DropTransferRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	peerID = protocol.NormalizeDeviceID(peerID)
	if !m.p2p.IsConnected(peerID) {
		return nil, fmt.Errorf("appareil %s non connecté en P2P", peerID)
	}
	if len(localPaths) == 0 {
		return nil, errors.New("aucun fichier ou dossier sélectionné")
	}

	var entries []protocol.DropFileEntry
	srcMap := make(map[string]string)
	var totalSize int64
	var fileCount int

	for _, rawPath := range localPaths {
		cleanPath := filepath.Clean(rawPath)
		info, err := os.Stat(cleanPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("fichier introuvable sur cette machine (%s). Utilise le glisser-déposer ou 'Choisir des fichiers' si le fichier est sur un autre PC", cleanPath)
			}
			return nil, fmt.Errorf("accès impossible à %s : %v", cleanPath, err)
		}
		baseName := filepath.Base(cleanPath)

		if !info.IsDir() {
			blocks, fileHash, sz, err := chunker.HashFileBlocks(cleanPath, chunker.DefaultBlockSize)
			if err != nil {
				return nil, err
			}
			rel := filepath.ToSlash(baseName)
			entries = append(entries, protocol.DropFileEntry{
				RelPath:  rel,
				Size:     sz,
				IsDir:    false,
				Blocks:   blocks,
				FileHash: fileHash,
			})
			srcMap[rel] = cleanPath
			totalSize += sz
			fileCount++
			continue
		}

		err = filepath.WalkDir(cleanPath, func(cur string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.Type()&os.ModeSymlink != 0 {
				return nil
			}
			subRel, err := filepath.Rel(filepath.Dir(cleanPath), cur)
			if err != nil {
				return err
			}
			rel := filepath.ToSlash(subRel)
			if d.IsDir() {
				entries = append(entries, protocol.DropFileEntry{
					RelPath: rel,
					IsDir:   true,
				})
				return nil
			}
			blocks, fileHash, sz, err := chunker.HashFileBlocks(cur, chunker.DefaultBlockSize)
			if err != nil {
				return err
			}
			entries = append(entries, protocol.DropFileEntry{
				RelPath:  rel,
				Size:     sz,
				IsDir:    false,
				Blocks:   blocks,
				FileHash: fileHash,
			})
			srcMap[rel] = cur
			totalSize += sz
			fileCount++
			return nil
		})
		if err != nil {
			return nil, err
		}
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

	rec := store.DropTransferRecord{
		TransferID:      transferID,
		Direction:       "send",
		PeerID:          peerID,
		PeerName:        peerName,
		Title:           title,
		SourceRoot:      filepath.Dir(localPaths[0]),
		TotalSize:       totalSize,
		Status:          "pending_accept",
		Files:           entries,
		CompletedBlocks: map[string][]int{},
	}

	srcJSON, _ := json.Marshal(srcMap)
	rec.SourceRoot = string(srcJSON)

	if err := m.store.UpsertDropTransfer(rec); err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.sourceMaps[transferID] = srcMap
	if stagingDir != "" {
		m.stagingDirs[transferID] = stagingDir
	}
	m.mu.Unlock()

	offer := protocol.DropOfferMsg{
		TransferID: transferID,
		SenderName: m.cfg.Snapshot().NodeName,
		Title:      title,
		TotalSize:  totalSize,
		FileCount:  fileCount,
		Files:      entries,
	}
	if err := m.p2p.SendControl(peerID, protocol.CtrlDropOffer, offer); err != nil {
		rec.Status = "failed"
		rec.ErrorMsg = err.Error()
		_ = m.store.UpsertDropTransfer(rec)
		m.cleanupTransferMem(transferID)
		m.emitUpdateForce()
		return nil, err
	}

	m.emitUpdateForce()
	return &rec, nil
}

func (m *Manager) HandleOffer(ctx context.Context, peerID string, raw json.RawMessage) {
	var offer protocol.DropOfferMsg
	if err := json.Unmarshal(raw, &offer); err != nil {
		return
	}

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
		rec.Status = "rejected"
		_ = m.store.UpdateDropProgress(transferID, "rejected", "", rec.TransferredBytes, rec.CompletedBlocks)
		_ = m.p2p.SendControl(rec.PeerID, protocol.CtrlDropResponse, protocol.DropResponseMsg{
			TransferID: transferID,
			Accepted:   false,
			Reason:     "rejected by user",
		})
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

	rec.DestDir = destDir
	rec.Status = "transferring"
	if err := m.store.UpsertDropTransfer(*rec); err != nil {
		return err
	}

	if err := m.p2p.SendControl(rec.PeerID, protocol.CtrlDropResponse, protocol.DropResponseMsg{
		TransferID: transferID,
		Accepted:   true,
	}); err != nil {
		// Revert status so user can retry accepting once sender is back online
		rec.Status = "pending_accept"
		_ = m.store.UpdateDropProgress(transferID, "pending_accept", "", rec.TransferredBytes, rec.CompletedBlocks)
		m.emitUpdateForce()
		return err
	}

	for _, f := range rec.Files {
		if f.IsDir {
			if p, err := safeJoin(destDir, f.RelPath); err == nil {
				_ = os.MkdirAll(p, 0755)
			}
		} else if f.Size == 0 {
			if p, err := safeJoin(destDir, f.RelPath); err == nil {
				_ = os.MkdirAll(filepath.Dir(p), 0755)
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
		go m.runSendTransfer(m.rootCtx, resp.TransferID)
	}
}

func (m *Manager) PauseTransfer(transferID string) error {
	rec, err := m.store.GetDropTransfer(transferID)
	if err != nil {
		return err
	}
	wasTransferring := rec.Status == "transferring"
	if wasTransferring {
		_ = m.store.UpdateDropProgress(transferID, "paused", "", rec.TransferredBytes, rec.CompletedBlocks)
	}
	m.mu.Lock()
	cancel, ok := m.activeCanc[transferID]
	delete(m.recvBuffers, transferID)
	m.mu.Unlock()
	if ok {
		cancel()
	}
	if wasTransferring {
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
	m.mu.Lock()
	cancel, ok := m.activeCanc[msg.TransferID]
	delete(m.recvBuffers, msg.TransferID)
	m.mu.Unlock()
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
	_ = m.store.UpdateDropProgress(transferID, "canceled", "canceled by user", rec.TransferredBytes, rec.CompletedBlocks)

	m.mu.Lock()
	cancel, ok := m.activeCanc[transferID]
	m.mu.Unlock()
	if ok {
		cancel()
	}

	if rec.Direction == "receive" && rec.DestDir != "" {
		m.cleanupTempFiles(rec)
	}
	_ = m.p2p.SendControl(rec.PeerID, protocol.CtrlDropCancel, protocol.DropCancelMsg{
		TransferID: transferID,
		Reason:     "canceled by user",
	})
	m.cleanupTransferMem(transferID)
	m.emitUpdateForce()
	return nil
}

func (m *Manager) DeleteTransfer(transferID string) error {
	if rec, err := m.store.GetDropTransfer(transferID); err == nil {
		if rec.Status == "transferring" || rec.Status == "pending_accept" {
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
	m.cleanupTransferMem(msg.TransferID)
	m.emitUpdateForce()
}

func (m *Manager) ResumeTransfer(ctx context.Context, transferID string) error {
	rec, err := m.store.GetDropTransfer(transferID)
	if err != nil {
		return err
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
	_ = m.store.UpdateDropProgress(transferID, "transferring", "", rec.TransferredBytes, rec.CompletedBlocks)
	_ = m.p2p.SendControl(rec.PeerID, protocol.CtrlDropResume, protocol.DropPauseMsg{
		TransferID: transferID,
	})
	m.emitUpdateForce()
	go m.runSendTransfer(m.rootCtx, transferID)
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
		var firstErr error
		var errOnce sync.Once
		var pending []store.DropBlockCompletion
		var lastFlush = time.Now()
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
				rec.TransferredBytes = total
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

		// Release cached read handles for source files once sending stops
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
	m.mu.Unlock()
	if hasStage && dir != "" {
		_ = os.RemoveAll(dir)
	}
}

func (m *Manager) cleanupTempFiles(rec *store.DropTransferRecord) {
	for _, entry := range rec.Files {
		if entry.IsDir || entry.Size == 0 {
			continue
		}
		if tmpPath, err := tempTransferPath(rec.DestDir, rec.TransferID, entry.RelPath); err == nil {
			m.handles.ClosePath(tmpPath)
			_ = os.Remove(tmpPath)
		}
	}
}

func (m *Manager) writeChunkNoAck(ctx context.Context, stream *quic.Stream, transferID, relPath string, blockIdx int, blk protocol.BlockInfo, localFile string, bufPtr *[]byte) error {
	var data []byte
	var err error
	if blk.Size > 0 && blk.Size <= chunker.DefaultBlockSize {
		data = (*bufPtr)[:blk.Size]
		err = m.handles.ReadBlockInto(localFile, blk.Offset, blk.Size, blk.Hash, data)
	} else {
		data, err = chunker.ReadBlock(localFile, blk.Offset, blk.Size, blk.Hash)
	}
	if err != nil {
		return err
	}

	hdr := protocol.DropChunkHeader{
		TransferID: transferID,
		RelPath:    relPath,
		BlockIdx:   blockIdx,
		Offset:     blk.Offset,
		Size:       blk.Size,
		Hash:       blk.Hash,
	}
	if err := protocol.WriteFrame(stream, hdr); err != nil {
		return err
	}
	_, err = m.p2p.LimitedWrite(ctx, stream, data)
	return err
}

// ServeChunkStream receives one or more pipelined incoming Drop blocks over StreamData.
func (m *Manager) ServeChunkStream(ctx context.Context, peerID string, stream *quic.Stream) {
	defer stream.Close()
	normPeer := protocol.NormalizeDeviceID(peerID)

	bufPtr := chunker.GetBuffer()
	defer chunker.PutBuffer(bufPtr)

	for {
		var hdr protocol.DropChunkHeader
		if err := protocol.ReadFrame(stream, &hdr); err != nil {
			return
		}
		if hdr.Size <= 0 || hdr.Size > protocol.MaxFrameSize {
			_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: "invalid chunk size"})
			return
		}

		var buf []byte
		if hdr.Size <= chunker.DefaultBlockSize {
			buf = (*bufPtr)[:hdr.Size]
		} else {
			buf = make([]byte, hdr.Size)
		}

		if _, err := m.p2p.LimitedReadFull(ctx, stream, buf); err != nil {
			_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: err.Error()})
			return
		}
		if hdr.Hash != "" && chunker.HashBytes(buf) != hdr.Hash {
			_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: "chunk hash mismatch"})
			return
		}

		destDir, ok := m.getActiveRecvDestDir(hdr.TransferID, normPeer)
		if !ok {
			_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: "transfer not active"})
			return
		}

		tmpPath, err := tempTransferPath(destDir, hdr.TransferID, hdr.RelPath)
		if err != nil {
			_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: err.Error()})
			return
		}
		if err := os.MkdirAll(filepath.Dir(tmpPath), 0755); err != nil {
			_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: err.Error()})
			return
		}

		if err := m.handles.WriteAt(tmpPath, hdr.Offset, buf); err != nil {
			_ = protocol.WriteFrame(stream, protocol.DropChunkAck{OK: false, Error: "disk write error"})
			return
		}
		m.bytesRx.Add(uint64(len(buf)))

		m.recordRecvBlock(hdr.TransferID, hdr.RelPath, hdr.BlockIdx, int64(hdr.Size))
		if err := protocol.WriteFrame(stream, protocol.DropChunkAck{OK: true}); err != nil {
			return
		}
	}
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
	// Run verification and atomic rename asynchronously so the QUIC control loop never blocks on large hashes.
	go m.handleCompleteAsync(peerID, msg)
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

		// Verify file size & block completeness; only re-read entire file if blocks aren't all recorded
		info, statErr := os.Stat(tmpPath)
		if statErr != nil || info.Size() != entry.Size {
			_ = m.store.UpdateDropStatusOnly(rec.TransferID, "failed", fmt.Sprintf("size mismatch for %s", entry.RelPath))
			m.emitUpdateForce()
			return
		}
		if len(rec.CompletedBlocks[entry.RelPath]) != len(entry.Blocks) {
			_, actualHash, _, err := chunker.HashFileBlocks(tmpPath, chunker.DefaultBlockSize)
			if err != nil || (entry.FileHash != "" && actualHash != entry.FileHash) {
				_ = m.store.UpdateDropStatusOnly(rec.TransferID, "failed", fmt.Sprintf("integrity verification failed for %s", entry.RelPath))
				m.emitUpdateForce()
				return
			}
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
	m.cleanupTransferMem(rec.TransferID)
	m.emitUpdateForce()
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

func containsInt(list []int, v int) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func newTransferID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
