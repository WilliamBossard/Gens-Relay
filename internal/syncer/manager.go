package syncer

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WilliamBossard/Gens-Relay/internal/chunker"
	"github.com/WilliamBossard/Gens-Relay/internal/p2p"
	"github.com/WilliamBossard/Gens-Relay/internal/store"
	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
	"github.com/fsnotify/fsnotify"
	"github.com/quic-go/quic-go"
)

const streamIOTimeout = 30 * time.Second

type FolderStatus struct {
	FolderID       string   `json:"folder_id"`
	State          string   `json:"state"` // "idle" | "scanning" | "syncing" | "paused" | "error"
	LocalFiles     int      `json:"local_files"`
	LocalBytes     int64    `json:"local_bytes"`
	NeedFiles      int      `json:"need_files"`
	NeedBytes      int64    `json:"need_bytes"`
	RemoteDiverged int      `json:"remote_diverged"` // send_only: remote ahead
	LocalChanges   int      `json:"local_changes"`   // receive_only: local modified/added
	RxBps          int64    `json:"rx_bps"`
	TxBps          int64    `json:"tx_bps"`
	LastScanAt     int64    `json:"last_scan_at"`
	LastError      string   `json:"last_error,omitempty"`
	CaseCollisions []string `json:"case_collisions,omitempty"`
}

type indexBatchBuffer struct {
	total   int
	parts   map[int][]protocol.FileMetadata
	updated time.Time
}

type watcherSpec struct {
	localPath      string
	ignorePatterns string
	cancel         context.CancelFunc
}

type scanJob struct {
	full  bool
	dirty map[string]struct{}
}

type folderByteCounter struct {
	rx    atomic.Uint64
	tx    atomic.Uint64
	rxBps atomic.Int64
	txBps atomic.Int64
}

type Manager struct {
	myID    string
	store   *store.Store
	p2p     *p2p.Manager
	onEmit  func(event string, data any)
	handles *chunker.FileHandleCache

	mu             sync.RWMutex
	statuses       map[string]*FolderStatus
	scanCh         map[string]chan struct{}
	scanStop       map[string]context.CancelFunc
	scanJobs       map[string]*scanJob
	watchers       map[string]watcherSpec
	pullMu         map[string]*sync.Mutex
	pendingBatches map[string]*indexBatchBuffer
	retryTimers    map[string]*time.Timer
	retryAttempts  map[string]int
	invitedSession map[string]bool
	rootCtx        context.Context
	workerWg       sync.WaitGroup

	folderCntMu sync.RWMutex
	folderCnts  map[string]*folderByteCounter

	recentMu     sync.Mutex
	recentWrites map[string]int64

	bytesRx atomic.Uint64
	bytesTx atomic.Uint64
	rxBps   atomic.Int64
	txBps   atomic.Int64
}

func NewManager(myID string, st *store.Store, p2pMgr *p2p.Manager, onEmit func(string, any)) *Manager {
	return &Manager{
		myID:           protocol.NormalizeDeviceID(myID),
		store:          st,
		p2p:            p2pMgr,
		onEmit:         onEmit,
		handles:        chunker.NewFileHandleCache(48, 5*time.Second),
		statuses:       make(map[string]*FolderStatus),
		scanCh:         make(map[string]chan struct{}),
		scanStop:       make(map[string]context.CancelFunc),
		scanJobs:       make(map[string]*scanJob),
		watchers:       make(map[string]watcherSpec),
		pullMu:         make(map[string]*sync.Mutex),
		pendingBatches: make(map[string]*indexBatchBuffer),
		retryTimers:    make(map[string]*time.Timer),
		retryAttempts:  make(map[string]int),
		invitedSession: make(map[string]bool),
		folderCnts:     make(map[string]*folderByteCounter),
		recentWrites:   make(map[string]int64),
		rootCtx:        context.Background(),
	}
}

func (m *Manager) TransferCounters() (rx, tx uint64) {
	return m.bytesRx.Load(), m.bytesTx.Load()
}

func (m *Manager) getFolderCounter(folderID string) *folderByteCounter {
	m.folderCntMu.RLock()
	fc, ok := m.folderCnts[folderID]
	m.folderCntMu.RUnlock()
	if ok {
		return fc
	}
	m.folderCntMu.Lock()
	fc, ok = m.folderCnts[folderID]
	if !ok {
		fc = &folderByteCounter{}
		m.folderCnts[folderID] = fc
	}
	m.folderCntMu.Unlock()
	return fc
}

func (m *Manager) addFolderRx(folderID string, n uint64) {
	m.bytesRx.Add(n)
	m.getFolderCounter(folderID).rx.Add(n)
}

func (m *Manager) addFolderTx(folderID string, n uint64) {
	m.bytesTx.Add(n)
	m.getFolderCounter(folderID).tx.Add(n)
}

func (m *Manager) Start(ctx context.Context) {
	m.rootCtx = ctx
	m.ReloadFolders()

	// Sample global + per-folder sync throughput every 1s for live UI speed badges
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		prevRx := m.bytesRx.Load()
		prevTx := m.bytesTx.Load()
		prevFolder := make(map[string][2]uint64)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				curRx := m.bytesRx.Load()
				curTx := m.bytesTx.Load()
				dRx := int64(curRx - prevRx)
				dTx := int64(curTx - prevTx)
				prevRx = curRx
				prevTx = curTx
				oldRx := m.rxBps.Swap(dRx)
				oldTx := m.txBps.Swap(dTx)

				changed := dRx != oldRx || dTx != oldTx

				m.folderCntMu.RLock()
				for fid, fc := range m.folderCnts {
					fRx := fc.rx.Load()
					fTx := fc.tx.Load()
					prev := prevFolder[fid]
					dfRx := int64(fRx - prev[0])
					dfTx := int64(fTx - prev[1])
					prevFolder[fid] = [2]uint64{fRx, fTx}
					if fc.rxBps.Swap(dfRx) != dfRx || fc.txBps.Swap(dfTx) != dfTx {
						changed = true
					}
				}
				m.folderCntMu.RUnlock()

				if changed {
					m.emitStatuses()
				}
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				folders, err := m.store.ListFolders()
				if err != nil {
					continue
				}
				now := time.Now().Unix()
				for _, f := range folders {
					if f.Paused {
						continue
					}
					st := m.getOrCreateStatus(f.FolderID)
					m.mu.RLock()
					due := now-st.LastScanAt >= int64(f.RescanSeconds)
					m.mu.RUnlock()
					if due {
						m.TriggerScan(f.FolderID)
					}
				}
			}
		}
	}()
}

// Shutdown stops all watchers and folder workers, waits for completion up to ctx, and closes pooled file handles.
func (m *Manager) Shutdown(ctx context.Context) {
	m.mu.Lock()
	for fid, spec := range m.watchers {
		spec.cancel()
		delete(m.watchers, fid)
	}
	for fid, stopFn := range m.scanStop {
		stopFn()
		delete(m.scanStop, fid)
		delete(m.scanCh, fid)
	}
	for fid, t := range m.retryTimers {
		t.Stop()
		delete(m.retryTimers, fid)
	}
	m.mu.Unlock()

	done := make(chan struct{})
	go func() {
		m.workerWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
	m.handles.CloseAll()
}

func (m *Manager) ReloadFolders() {
	folders, err := m.store.ListFolders()
	if err != nil {
		return
	}

	active := make(map[string]store.FolderRecord, len(folders))
	for _, f := range folders {
		active[f.FolderID] = f
	}

	m.mu.Lock()
	for fid, spec := range m.watchers {
		f, ok := active[fid]
		if !ok || f.Paused || spec.localPath != f.LocalPath || spec.ignorePatterns != f.IgnorePatterns {
			spec.cancel()
			delete(m.watchers, fid)
		}
	}
	for fid := range m.statuses {
		if _, ok := active[fid]; !ok {
			delete(m.statuses, fid)
		}
	}
	for fid, stopFn := range m.scanStop {
		if f, ok := active[fid]; !ok || f.Paused {
			stopFn()
			delete(m.scanStop, fid)
			delete(m.scanCh, fid)
			delete(m.scanJobs, fid)
			if t, has := m.retryTimers[fid]; has {
				t.Stop()
				delete(m.retryTimers, fid)
			}
			delete(m.retryAttempts, fid)
			if !ok {
				delete(m.pullMu, fid)
			}
		}
	}
	m.mu.Unlock()

	for _, f := range folders {
		if f.Paused {
			m.setStatusState(f.FolderID, "paused", "")
			continue
		}
		m.mu.Lock()
		st := m.getOrCreateStatusLocked(f.FolderID)
		if st.State == "paused" {
			st.State = "idle"
			st.LastError = ""
		}
		// Reset session invite suppression on explicit folder config update
		for _, pid := range f.Peers {
			delete(m.invitedSession, f.FolderID+"|"+protocol.NormalizeDeviceID(pid))
		}
		m.mu.Unlock()

		m.ensureWatcher(f)
		m.TriggerScan(f.FolderID)
		go m.sendFolderInvitesForFolder(f)
	}
}

func (m *Manager) TriggerScan(folderID string) {
	m.enqueueScan(folderID, true, nil)
}

func (m *Manager) TriggerPathsScan(folderID string, dirtyRelPaths []string) {
	if len(dirtyRelPaths) == 0 {
		m.TriggerScan(folderID)
		return
	}
	m.enqueueScan(folderID, false, dirtyRelPaths)
}

func (m *Manager) enqueueScan(folderID string, full bool, dirty []string) {
	m.mu.Lock()
	ch, ok := m.scanCh[folderID]
	if !ok {
		ch = make(chan struct{}, 1)
		wCtx, cancel := context.WithCancel(m.rootCtx)
		m.scanCh[folderID] = ch
		m.scanStop[folderID] = cancel
		m.workerWg.Add(1)
		go m.folderWorker(wCtx, folderID, ch)
	}
	job := m.scanJobs[folderID]
	if job == nil {
		job = &scanJob{dirty: make(map[string]struct{})}
		m.scanJobs[folderID] = job
	}
	if full {
		job.full = true
	} else if !job.full {
		for _, p := range dirty {
			job.dirty[p] = struct{}{}
		}
		if len(job.dirty) > 500 {
			job.full = true
		}
	}
	m.mu.Unlock()

	select {
	case ch <- struct{}{}:
	default:
	}
}

func (m *Manager) folderWorker(wCtx context.Context, folderID string, ch chan struct{}) {
	defer m.workerWg.Done()
	for {
		select {
		case <-wCtx.Done():
			return
		case <-ch:
			m.runScanAndSync(wCtx, folderID)
		}
	}
}

func (m *Manager) runScanAndSync(ctx context.Context, folderID string) {
	mu := m.folderPullMutex(folderID)
	mu.Lock()
	defer mu.Unlock()

	m.mu.Lock()
	job := m.scanJobs[folderID]
	full := true
	var dirtyList []string
	if job != nil {
		full = job.full || len(job.dirty) == 0
		if !full {
			dirtyList = make([]string, 0, len(job.dirty))
			for p := range job.dirty {
				dirtyList = append(dirtyList, p)
			}
		}
		job.full = false
		job.dirty = make(map[string]struct{})
	}
	m.mu.Unlock()

	f, err := m.store.GetFolder(folderID)
	if err != nil {
		return
	}
	if f.Paused {
		m.setStatusState(folderID, "paused", "")
		return
	}

	m.setStatusState(folderID, "scanning", "")

	prevIdx, err := m.store.GetLocalScanIndex(folderID)
	if err != nil {
		m.setStatusState(folderID, "error", err.Error())
		return
	}

	var res *ScanResult
	if full || len(prevIdx) == 0 {
		res, err = ScanFolder(f.LocalPath, m.myID, f.IgnorePatterns, prevIdx)
		if err == nil && f.Versioning == "trashcan" && (f.TrashcanMaxDays > 0 || f.TrashcanMaxMB > 0) {
			PruneTrashcan(f.LocalPath, f.TrashcanMaxDays, f.TrashcanMaxMB)
		}
	} else {
		res, err = ScanPaths(f.LocalPath, m.myID, f.IgnorePatterns, dirtyList, prevIdx)
	}
	if err != nil {
		m.setStatusState(folderID, "error", err.Error())
		return
	}

	if len(res.IgnoredRelPaths) > 0 {
		_ = m.store.DeleteLocalFiles(folderID, res.IgnoredRelPaths)
	}
	if len(res.Changed) > 0 {
		if err := m.store.UpsertLocalFiles(folderID, res.Changed); err != nil {
			m.setStatusState(folderID, "error", err.Error())
			return
		}
	}

	m.mu.Lock()
	st := m.getOrCreateStatusLocked(folderID)
	st.LocalFiles = res.FileCount
	st.LocalBytes = res.TotalSize
	st.LastScanAt = time.Now().Unix()
	st.CaseCollisions = res.CaseCollisions
	m.mu.Unlock()

	if len(res.Changed) > 0 {
		m.broadcastIndexDelta(*f, res.Changed)
	}

	m.reconcileFolderLocked(ctx, *f)
}

func (m *Manager) sendFolderIndexBatched(peerID, folderID string, full bool, indexEpoch uint64, list []protocol.FileMetadata) {
	const maxFilesPerBatch = 350
	if len(list) == 0 {
		_ = m.p2p.SendControl(peerID, protocol.CtrlFolderIndex, protocol.FolderIndexMsg{
			FolderID:     folderID,
			Full:         full,
			IndexEpoch:   indexEpoch,
			BatchIndex:   0,
			TotalBatches: 1,
			Files:        list,
		})
		return
	}
	totalBatches := (len(list) + maxFilesPerBatch - 1) / maxFilesPerBatch
	batchID := randomHex(6)
	for i := 0; i < totalBatches; i++ {
		start := i * maxFilesPerBatch
		end := start + maxFilesPerBatch
		if end > len(list) {
			end = len(list)
		}
		if err := m.p2p.SendControl(peerID, protocol.CtrlFolderIndex, protocol.FolderIndexMsg{
			FolderID:     folderID,
			Full:         full,
			IndexEpoch:   indexEpoch,
			BatchID:      batchID,
			BatchIndex:   i,
			TotalBatches: totalBatches,
			Files:        list[start:end],
		}); err != nil {
			log.Printf("[syncer] sendFolderIndexBatched(%s -> %s) error: %v", folderID, peerID, err)
			return
		}
	}
}

func (m *Manager) sendFolderInvitesForFolder(f store.FolderRecord) {
	if f.Paused {
		return
	}
	for _, peerID := range f.Peers {
		if m.p2p.IsConnected(peerID) {
			m.maybeSendFolderInvite(peerID, f)
		}
	}
}

func (m *Manager) maybeSendFolderInvite(peerID string, f store.FolderRecord) {
	peerID = protocol.NormalizeDeviceID(peerID)
	key := f.FolderID + "|" + peerID
	m.mu.Lock()
	already := m.invitedSession[key]
	if !already {
		m.invitedSession[key] = true
	}
	m.mu.Unlock()
	if already {
		return
	}

	m.mu.RLock()
	st := m.statuses[f.FolderID]
	fileCount := 0
	var totalBytes int64
	if st != nil {
		fileCount = st.LocalFiles
		totalBytes = st.LocalBytes
	}
	m.mu.RUnlock()

	suggested := string(protocol.ModeSendReceive)
	if f.Mode == protocol.ModeSendOnly {
		suggested = string(protocol.ModeReceiveOnly)
	} else if f.Mode == protocol.ModeReceiveOnly {
		suggested = string(protocol.ModeSendOnly)
	}

	_ = m.p2p.SendControl(peerID, protocol.CtrlFolderInvite, protocol.FolderInviteMsg{
		FolderID:      f.FolderID,
		Label:         f.Label,
		SuggestedMode: suggested,
		FileCount:     fileCount,
		TotalBytes:    totalBytes,
	})
}

func (m *Manager) OnPeerConnected(ctx context.Context, peerID string) {
	folders, err := m.store.ListFolders()
	if err != nil {
		return
	}
	for _, f := range folders {
		if f.Paused || !containsPeer(f.Peers, peerID) {
			continue
		}
		knownRemoteSeq, knownRemoteCount, knownRemoteEpoch, _ := m.store.GetPeerRemoteSummary(f.FolderID, peerID)
		localMaxSeq, localCount, localEpoch, _ := m.store.GetLocalIndexSummaryWithEpoch(f.FolderID)
		_ = m.p2p.SendControl(peerID, protocol.CtrlFolderSummary, protocol.FolderSummaryMsg{
			FolderID:         f.FolderID,
			KnownMaxSeq:      knownRemoteSeq,
			KnownRemoteCount: knownRemoteCount,
			KnownRemoteEpoch: knownRemoteEpoch,
			LocalMaxSeq:      localMaxSeq,
			LocalCount:       localCount,
			LocalIndexEpoch:  localEpoch,
		})
		m.maybeSendFolderInvite(peerID, f)
	}
}

func (m *Manager) HandleFolderSummary(ctx context.Context, peerID string, raw json.RawMessage) {
	var msg protocol.FolderSummaryMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	f, err := m.store.GetFolder(msg.FolderID)
	if err != nil || f.Paused || !containsPeer(f.Peers, peerID) {
		return
	}

	m.mu.Lock()
	m.invitedSession[f.FolderID+"|"+protocol.NormalizeDeviceID(peerID)] = true
	m.mu.Unlock()

	localMaxSeq, localCount, localEpoch, err := m.store.GetLocalIndexSummaryWithEpoch(f.FolderID)
	if err != nil {
		return
	}
	// True delta check: compare peer's known count and epoch of OUR index against OUR local count and epoch
	if msg.KnownMaxSeq > 0 && msg.KnownMaxSeq <= localMaxSeq &&
		msg.KnownRemoteCount == localCount && msg.KnownRemoteEpoch == localEpoch {
		if msg.KnownMaxSeq == localMaxSeq {
			go m.reconcileFolder(ctx, *f)
			return
		}
		delta, err := m.store.GetLocalIndexSince(f.FolderID, msg.KnownMaxSeq)
		if err == nil {
			m.sendFolderIndexBatched(peerID, f.FolderID, false, localEpoch, delta)
			return
		}
	}

	idx, err := m.store.GetLocalIndex(f.FolderID)
	if err != nil {
		return
	}
	list := make([]protocol.FileMetadata, 0, len(idx))
	for _, entry := range idx {
		list = append(list, entry)
	}
	m.sendFolderIndexBatched(peerID, f.FolderID, true, localEpoch, list)
}

func (m *Manager) HandleFolderIndex(ctx context.Context, peerID string, raw json.RawMessage) {
	var msg protocol.FolderIndexMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	f, err := m.store.GetFolder(msg.FolderID)
	if err != nil || !containsPeer(f.Peers, peerID) {
		return
	}

	m.mu.Lock()
	m.invitedSession[f.FolderID+"|"+protocol.NormalizeDeviceID(peerID)] = true
	m.mu.Unlock()

	filesToApply := msg.Files
	if msg.TotalBatches > 1 && msg.BatchID != "" {
		if msg.TotalBatches > 100000 || msg.BatchIndex < 0 || msg.BatchIndex >= msg.TotalBatches {
			return
		}
		key := fmt.Sprintf("%s|%s|%s", msg.FolderID, protocol.NormalizeDeviceID(peerID), msg.BatchID)
		m.mu.Lock()
		cutoff := time.Now().Add(-5 * time.Minute)
		for k, b := range m.pendingBatches {
			if b.updated.Before(cutoff) {
				delete(m.pendingBatches, k)
			}
		}
		buf, ok := m.pendingBatches[key]
		if !ok {
			buf = &indexBatchBuffer{
				total: msg.TotalBatches,
				parts: make(map[int][]protocol.FileMetadata, msg.TotalBatches),
			}
			m.pendingBatches[key] = buf
		}
		if buf.total != msg.TotalBatches {
			m.mu.Unlock()
			return
		}
		buf.parts[msg.BatchIndex] = msg.Files
		buf.updated = time.Now()
		ready := len(buf.parts) == buf.total
		if ready {
			delete(m.pendingBatches, key)
		}
		m.mu.Unlock()

		if !ready {
			return
		}
		filesToApply = nil
		for i := 0; i < buf.total; i++ {
			filesToApply = append(filesToApply, buf.parts[i]...)
		}
	}

	if err := m.store.UpsertRemoteFilesWithEpoch(msg.FolderID, peerID, msg.Full, msg.IndexEpoch, filesToApply); err != nil {
		return
	}
	if !f.Paused {
		go m.reconcileFolder(ctx, *f)
	}
}

func (m *Manager) HandleFolderInvite(peerID string, raw json.RawMessage) {
	var msg protocol.FolderInviteMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	if strings.TrimSpace(msg.FolderID) == "" {
		return
	}
	normPeer := protocol.NormalizeDeviceID(peerID)
	if !m.store.IsDeviceAllowed(normPeer) {
		return
	}
	// If we already configure this folder locally, no invitation needed
	if _, err := m.store.GetFolder(msg.FolderID); err == nil {
		return
	}

	senderName := msg.SenderName
	if dev, err := m.store.GetDevice(normPeer); err == nil && dev.Name != "" {
		senderName = dev.Name
	}
	if senderName == "" {
		senderName = normPeer
	}
	label := msg.Label
	if label == "" {
		label = msg.FolderID
	}

	_ = m.store.UpsertFolderInvitation(store.FolderInvitationRecord{
		FolderID:      msg.FolderID,
		SenderID:      normPeer,
		SenderName:    senderName,
		Label:         label,
		SuggestedMode: msg.SuggestedMode,
		FileCount:     msg.FileCount,
		TotalBytes:    msg.TotalBytes,
	})
	m.emitInvitations()
}

func (m *Manager) HandleFolderInviteReject(peerID string, raw json.RawMessage) {
	var msg protocol.FolderInviteRejectMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	log.Printf("[syncer] Peer %s declined shared folder invitation %q", peerID, msg.FolderID)
}

func (m *Manager) AcceptInvitation(folderID, senderID, localPath string, mode protocol.FolderMode) error {
	inv, err := m.store.GetFolderInvitation(folderID, senderID)
	if err != nil {
		return err
	}
	localPath = strings.TrimSpace(localPath)
	if localPath == "" {
		return errors.New("local_path is required")
	}
	if mode == "" {
		if inv.SuggestedMode != "" {
			mode = protocol.FolderMode(inv.SuggestedMode)
		} else {
			mode = protocol.ModeSendReceive
		}
	}
	if err := os.MkdirAll(localPath, 0755); err != nil {
		return err
	}
	if err := m.store.UpsertFolder(store.FolderRecord{
		FolderID:        inv.FolderID,
		Label:           inv.Label,
		LocalPath:       localPath,
		Mode:            mode,
		Versioning:      "trashcan",
		KeepVersions:    5,
		RescanSeconds:   60,
		MinFreeDiskMB:   500,
		TrashcanMaxDays: 30,
		Peers:           []string{inv.SenderID},
	}); err != nil {
		return err
	}
	_ = m.store.DeleteFolderInvitation(inv.FolderID, "")
	m.emitInvitations()
	m.ReloadFolders()
	if m.p2p.IsConnected(inv.SenderID) {
		go m.OnPeerConnected(m.rootCtx, inv.SenderID)
	}
	return nil
}

func (m *Manager) RejectInvitation(folderID, senderID string) error {
	norm := protocol.NormalizeDeviceID(senderID)
	if err := m.store.DeleteFolderInvitation(folderID, norm); err != nil {
		return err
	}
	_ = m.p2p.SendControl(norm, protocol.CtrlFolderInviteReject, protocol.FolderInviteRejectMsg{FolderID: folderID})
	m.emitInvitations()
	return nil
}

func (m *Manager) emitInvitations() {
	if m.onEmit != nil {
		if invs, err := m.store.ListFolderInvitations(); err == nil {
			m.onEmit("folder_invitations_updated", invs)
		}
	}
}

func (m *Manager) reconcileFolder(ctx context.Context, f store.FolderRecord) {
	mu := m.folderPullMutex(f.FolderID)
	mu.Lock()
	defer mu.Unlock()
	m.reconcileFolderLocked(ctx, f)
}

func (m *Manager) reconcileFolderLocked(ctx context.Context, f store.FolderRecord) {
	if f.Paused {
		return
	}

	localIdx, err := m.store.GetLocalScanIndex(f.FolderID)
	if err != nil {
		return
	}

	type pullTask struct {
		peerID   string
		remote   protocol.FileMetadata
		conflict bool
	}

	type remoteCandidate struct {
		peerID string
		meta   protocol.FileMetadata
	}
	bestRemote := make(map[string]remoteCandidate)
	for _, peerID := range f.Peers {
		// Use GetRemoteScanIndex (without blocks_json) to prevent RAM spikes on huge folders
		remoteIdx, err := m.store.GetRemoteScanIndex(f.FolderID, peerID)
		if err != nil {
			continue
		}
		for rel, rem := range remoteIdx {
			cur, exists := bestRemote[rel]
			if !exists {
				bestRemote[rel] = remoteCandidate{peerID: peerID, meta: rem}
				continue
			}
			cmp := CompareClocks(cur.meta.Version, rem.Version)
			if cmp == RelOlder {
				bestRemote[rel] = remoteCandidate{peerID: peerID, meta: rem}
			} else if cmp == RelConflict {
				if rem.MtimeNs > cur.meta.MtimeNs || (rem.MtimeNs == cur.meta.MtimeNs && peerID > cur.peerID) {
					bestRemote[rel] = remoteCandidate{peerID: peerID, meta: rem}
				}
			}
		}
	}

	localByHash := make(map[string]string, len(localIdx))
	for rel, loc := range localIdx {
		if !loc.Deleted && !loc.IsDir && loc.FileHash != "" && loc.Size > 0 {
			localByHash[loc.FileHash] = rel
		}
	}

	var dirCreates []pullTask
	var filePulls []pullTask
	var deletions []pullTask
	var remoteDiverged int
	var localChanges int
	var needBytes int64

	for rel, cand := range bestRemote {
		rem := cand.meta
		peerID := cand.peerID
		loc, hasLoc := localIdx[rel]
		relCmp := RelOlder
		if !hasLoc {
			if rem.Deleted {
				continue
			}
			relCmp = RelOlder
		} else {
			relCmp = CompareClocks(loc.Version, rem.Version)
		}

		addTask := func(t pullTask) {
			if t.remote.Deleted {
				deletions = append(deletions, t)
			} else if t.remote.IsDir {
				dirCreates = append(dirCreates, t)
			} else {
				filePulls = append(filePulls, t)
				needBytes += t.remote.Size
			}
		}

		switch relCmp {
		case RelOlder:
			if f.Mode == protocol.ModeSendOnly {
				remoteDiverged++
				continue
			}
			addTask(pullTask{peerID: peerID, remote: rem})

		case RelConflict:
			if f.Mode == protocol.ModeSendOnly {
				remoteDiverged++
				continue
			}
			if f.Mode == protocol.ModeReceiveOnly {
				localChanges++
				continue
			}
			addTask(pullTask{peerID: peerID, remote: rem, conflict: true})

		case RelNewer:
			if f.Mode == protocol.ModeReceiveOnly && hasLoc {
				localChanges++
			}
		}
	}

	// In receive_only mode, locally created files absent from all remote indexes also count as localChanges
	if f.Mode == protocol.ModeReceiveOnly {
		for rel, loc := range localIdx {
			if loc.Deleted {
				continue
			}
			if _, ok := bestRemote[rel]; !ok {
				localChanges++
			}
		}
	}

	totalTasks := len(dirCreates) + len(filePulls) + len(deletions)

	m.mu.Lock()
	st := m.getOrCreateStatusLocked(f.FolderID)
	st.NeedFiles = totalTasks
	st.NeedBytes = needBytes
	st.RemoteDiverged = remoteDiverged
	st.LocalChanges = localChanges
	if totalTasks > 0 {
		st.State = "syncing"
	} else {
		st.State = "idle"
		st.LastError = ""
		m.retryAttempts[f.FolderID] = 0
	}
	m.mu.Unlock()
	m.emitStatuses()

	if totalTasks == 0 {
		return
	}

	// Topological ordering prevents concurrent parent-dir delete vs child-file create races
	sort.Slice(dirCreates, func(i, j int) bool {
		di := strings.Count(dirCreates[i].remote.RelPath, "/")
		dj := strings.Count(dirCreates[j].remote.RelPath, "/")
		if di != dj {
			return di < dj
		}
		return dirCreates[i].remote.RelPath < dirCreates[j].remote.RelPath
	})
	sort.Slice(deletions, func(i, j int) bool {
		di := strings.Count(deletions[i].remote.RelPath, "/")
		dj := strings.Count(deletions[j].remote.RelPath, "/")
		if di != dj {
			return di > dj
		}
		return deletions[i].remote.RelPath > deletions[j].remote.RelPath
	})

	var stateMu sync.Mutex
	var pendingSave []protocol.FileMetadata
	var pulled []protocol.FileMetadata
	var lastSaveAt = time.Now()
	var conflictCreated bool
	var pullErrors int
	var lastPullErr error

	lookupHash := func(hash string) (string, bool) {
		stateMu.Lock()
		rel, ok := localByHash[hash]
		stateMu.Unlock()
		return rel, ok
	}

	flushSaved := func(force bool) {
		stateMu.Lock()
		if len(pendingSave) == 0 || (!force && len(pendingSave) < 32 && time.Since(lastSaveAt) < 250*time.Millisecond) {
			stateMu.Unlock()
			return
		}
		batch := pendingSave
		pendingSave = nil
		lastSaveAt = time.Now()
		stateMu.Unlock()

		if uErr := m.store.UpsertLocalFiles(f.FolderID, batch); uErr != nil {
			stateMu.Lock()
			pullErrors++
			lastPullErr = uErr
			stateMu.Unlock()
			return
		}
		stateMu.Lock()
		pulled = append(pulled, batch...)
		for _, saved := range batch {
			localIdx[saved.RelPath] = saved
			if !saved.Deleted && !saved.IsDir && saved.FileHash != "" && saved.Size > 0 {
				localByHash[saved.FileHash] = saved.RelPath
			}
		}
		stateMu.Unlock()
	}

	processTask := func(t pullTask) {
		if ctx.Err() != nil || !m.p2p.IsConnected(t.peerID) {
			return
		}
		stateMu.Lock()
		loc := localIdx[t.remote.RelPath]
		stateMu.Unlock()

		var updated *protocol.FileMetadata
		var madeConflict bool
		var pullErr error
		if t.conflict {
			updated, madeConflict, pullErr = m.resolveAndPullConflict(ctx, f, t.peerID, loc, t.remote, lookupHash)
		} else {
			updated, pullErr = m.applyRemoteEntry(ctx, f, t.peerID, loc, t.remote, lookupHash)
		}

		if pullErr != nil {
			log.Printf("[syncer] folder=%s file=%s pull error: %v", f.FolderID, t.remote.RelPath, pullErr)
			stateMu.Lock()
			pullErrors++
			lastPullErr = pullErr
			stateMu.Unlock()
			m.setStatusState(f.FolderID, "error", pullErr.Error())
			return
		}
		if updated != nil {
			stateMu.Lock()
			pendingSave = append(pendingSave, *updated)
			if madeConflict {
				conflictCreated = true
			}
			needFlush := len(pendingSave) >= 32 || time.Since(lastSaveAt) >= 250*time.Millisecond
			stateMu.Unlock()
			if needFlush {
				flushSaved(false)
			}
		}

		m.mu.Lock()
		if st.NeedFiles > 0 {
			st.NeedFiles--
		}
		if !t.remote.Deleted && !t.remote.IsDir {
			st.NeedBytes -= t.remote.Size
			if st.NeedBytes < 0 {
				st.NeedBytes = 0
			}
		}
		m.mu.Unlock()
		m.emitStatuses()
	}

	// Phase 1: directory creations (top-down, sequential)
	for _, t := range dirCreates {
		processTask(t)
	}

	// Phase 2: file pulls (parallelized up to 4 concurrent files)
	if len(filePulls) > 0 {
		workers := 4
		if len(filePulls) < workers {
			workers = len(filePulls)
		}
		if workers <= 1 {
			for _, t := range filePulls {
				processTask(t)
			}
		} else {
			taskCh := make(chan pullTask, len(filePulls))
			for _, t := range filePulls {
				taskCh <- t
			}
			close(taskCh)
			var wg sync.WaitGroup
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for t := range taskCh {
						processTask(t)
					}
				}()
			}
			wg.Wait()
		}
	}

	// Phase 3: deletions (bottom-up, sequential)
	for _, t := range deletions {
		processTask(t)
	}

	flushSaved(true)

	if len(pulled) > 0 {
		m.broadcastIndexDelta(f, pulled)
	}

	var count int
	var totalBytes int64
	for _, e := range localIdx {
		if !e.Deleted && !e.IsDir {
			count++
			totalBytes += e.Size
		}
	}

	m.mu.Lock()
	st.LocalFiles = count
	st.LocalBytes = totalBytes
	if pullErrors > 0 && lastPullErr != nil {
		st.State = "error"
		st.LastError = lastPullErr.Error()
	} else {
		st.State = "idle"
		st.LastError = ""
		m.retryAttempts[f.FolderID] = 0
	}
	m.mu.Unlock()
	m.emitStatuses()

	if pullErrors > 0 {
		m.scheduleRetry(f.FolderID)
	}
	if conflictCreated {
		m.TriggerScan(f.FolderID)
	}
}

func (m *Manager) scheduleRetry(folderID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rootCtx.Err() != nil {
		return
	}
	if old, ok := m.retryTimers[folderID]; ok {
		old.Stop()
	}
	attempt := m.retryAttempts[folderID] + 1
	m.retryAttempts[folderID] = attempt
	delay := 5 * time.Second
	for i := 1; i < attempt && delay < 60*time.Second; i++ {
		delay *= 2
	}
	if delay > 60*time.Second {
		delay = 60 * time.Second
	}
	m.retryTimers[folderID] = time.AfterFunc(delay, func() {
		m.TriggerScan(folderID)
	})
}

func (m *Manager) HandleControlMessage(ctx context.Context, peerID string, env protocol.ControlEnvelope) {
	switch env.Type {
	case protocol.CtrlFolderSummary:
		m.HandleFolderSummary(ctx, peerID, env.Payload)
	case protocol.CtrlFolderIndex:
		m.HandleFolderIndex(ctx, peerID, env.Payload)
	case protocol.CtrlFolderInvite:
		m.HandleFolderInvite(peerID, env.Payload)
	case protocol.CtrlFolderInviteReject:
		m.HandleFolderInviteReject(peerID, env.Payload)
	}
}

func (m *Manager) HandleDataStream(ctx context.Context, peerID string, kind protocol.DataRequestKind, stream *quic.Stream) {
	switch kind {
	case protocol.DataReqSyncBlock:
		m.ServeBlockStream(ctx, peerID, stream)
	case protocol.DataReqSyncMultiBlock:
		m.ServeMultiBlockStream(ctx, peerID, stream)
	default:
		_ = stream.Close()
	}
}

func (m *Manager) applyRemoteEntry(ctx context.Context, f store.FolderRecord, peerID string, loc, rem protocol.FileMetadata, lookupHash func(string) (string, bool)) (*protocol.FileMetadata, error) {
	dstPath, err := safeJoin(f.LocalPath, rem.RelPath)
	if err != nil {
		return nil, err
	}

	if rem.Deleted {
		m.handles.ClosePath(dstPath)
		m.markRecentWrite(f.FolderID, dstPath)
		if err := ArchiveFile(f.LocalPath, rem.RelPath, f.Versioning, f.KeepVersions); err != nil {
			return nil, err
		}
		res := rem
		return &res, nil
	}

	if rem.IsDir {
		if info, sErr := os.Stat(dstPath); sErr == nil && !info.IsDir() {
			m.handles.ClosePath(dstPath)
			m.markRecentWrite(f.FolderID, dstPath)
			_ = ArchiveFile(f.LocalPath, rem.RelPath, f.Versioning, f.KeepVersions)
		}
		m.markRecentWrite(f.FolderID, dstPath)
		if err := os.MkdirAll(dstPath, 0755); err != nil {
			return nil, err
		}
		res := rem
		return &res, nil
	}

	hydrated, err := m.pullFileContent(ctx, f, peerID, loc, rem, dstPath, lookupHash)
	if err != nil {
		return nil, err
	}
	return &hydrated, nil
}

func (m *Manager) resolveAndPullConflict(ctx context.Context, f store.FolderRecord, peerID string, loc, rem protocol.FileMetadata, lookupHash func(string) (string, bool)) (*protocol.FileMetadata, bool, error) {
	remoteWins := rem.MtimeNs > loc.MtimeNs || (rem.MtimeNs == loc.MtimeNs && peerID > m.myID)
	merged := MergeClocks(loc.Version, rem.Version)

	if remoteWins {
		conflictCreated := false
		if !loc.Deleted && !loc.IsDir {
			src, err := safeJoin(f.LocalPath, loc.RelPath)
			if err != nil {
				return nil, false, err
			}
			m.handles.ClosePath(src)
			if _, statErr := os.Stat(src); statErr == nil {
				conflictRel := ConflictRelPath(loc.RelPath, m.myID, loc.MtimeNs)
				conflictDst, err2 := safeJoin(f.LocalPath, conflictRel)
				if err2 != nil {
					return nil, false, err2
				}
				if mkErr := os.MkdirAll(filepath.Dir(conflictDst), 0755); mkErr != nil {
					return nil, false, mkErr
				}
				m.markRecentWrite(f.FolderID, src)
				m.markRecentWrite(f.FolderID, conflictDst)
				if renErr := os.Rename(src, conflictDst); renErr != nil {
					return nil, false, fmt.Errorf("preserve conflict copy %s: %w", conflictRel, renErr)
				}
				conflictCreated = true
			}
		}
		dstPath, err := safeJoin(f.LocalPath, rem.RelPath)
		if err != nil {
			return nil, false, err
		}
		out := rem
		if rem.Deleted {
			m.handles.ClosePath(dstPath)
			m.markRecentWrite(f.FolderID, dstPath)
			if err := ArchiveFile(f.LocalPath, rem.RelPath, f.Versioning, f.KeepVersions); err != nil {
				return nil, false, err
			}
		} else if rem.IsDir {
			if info, sErr := os.Stat(dstPath); sErr == nil && !info.IsDir() {
				m.handles.ClosePath(dstPath)
				m.markRecentWrite(f.FolderID, dstPath)
				_ = ArchiveFile(f.LocalPath, rem.RelPath, f.Versioning, f.KeepVersions)
			}
			m.markRecentWrite(f.FolderID, dstPath)
			if err := os.MkdirAll(dstPath, 0755); err != nil {
				return nil, false, err
			}
		} else {
			hydrated, err := m.pullFileContent(ctx, f, peerID, loc, rem, dstPath, lookupHash)
			if err != nil {
				return nil, false, err
			}
			out = hydrated
		}
		out.Version = merged
		return &out, conflictCreated, nil
	}

	conflictCreated := false
	if !rem.Deleted && !rem.IsDir {
		conflictRel := ConflictRelPath(rem.RelPath, peerID, rem.MtimeNs)
		conflictDst, err := safeJoin(f.LocalPath, conflictRel)
		if err != nil {
			return nil, false, err
		}
		if _, statErr := os.Stat(conflictDst); os.IsNotExist(statErr) {
			if _, err := m.pullFileContent(ctx, f, peerID, protocol.FileMetadata{}, rem, conflictDst, lookupHash); err != nil {
				return nil, false, err
			}
			conflictCreated = true
		}
	}
	out := loc
	out.Version = merged
	return &out, conflictCreated, nil
}

func (m *Manager) pullFileContent(ctx context.Context, f store.FolderRecord, peerID string, loc, rem protocol.FileMetadata, dstPath string, lookupHash func(string) (string, bool)) (protocol.FileMetadata, error) {
	if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
		return rem, err
	}

	// Lazy-load remote blocks only for files we are actually pulling
	if rem.Size > 0 && len(rem.Blocks) == 0 {
		if blocks, bs, bErr := m.store.GetRemoteFileBlocks(f.FolderID, peerID, rem.RelPath); bErr == nil {
			rem.Blocks = blocks
			if rem.BlockSize == 0 {
				rem.BlockSize = bs
			}
		}
	}

	// Fast metadata-only convergence if local file already has exact identical content hash & size
	if rem.FileHash != "" && loc.FileHash == rem.FileHash && loc.Size == rem.Size {
		if info, sErr := os.Stat(dstPath); sErr == nil && !info.IsDir() && info.Size() == rem.Size {
			m.markRecentWrite(f.FolderID, dstPath)
			if runtime.GOOS != "windows" && rem.ModeBits != 0 {
				_ = os.Chmod(dstPath, os.FileMode(rem.ModeBits))
			}
			if rem.MtimeNs > 0 {
				t := time.Unix(0, rem.MtimeNs)
				_ = os.Chtimes(dstPath, t, t)
			}
			return rem, nil
		}
	}

	// Free disk space guard
	if f.MinFreeDiskMB > 0 && rem.Size > 0 {
		if avail, err := AvailableDiskBytes(f.LocalPath); err == nil {
			need := uint64(f.MinFreeDiskMB)*1024*1024 + uint64(rem.Size)
			if avail < need {
				return rem, fmt.Errorf("espace disque insuffisant pour %s (%d Mo libres, %d Mo requis)", rem.RelPath, avail/(1024*1024), need/(1024*1024))
			}
		}
	}

	tmpPath := filepath.Join(filepath.Dir(dstPath), fmt.Sprintf(".%s.gens.tmp.%s", filepath.Base(dstPath), randomHex(6)))
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return rem, err
	}
	cleanupTmp := true
	defer func() {
		_ = tmpFile.Close()
		if cleanupTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	copiedLocal := false
	if rem.FileHash != "" && rem.Size > 0 && lookupHash != nil {
		if srcRel, ok := lookupHash(rem.FileHash); ok {
			if srcAbs, sErr := safeJoin(f.LocalPath, srcRel); sErr == nil {
				if srcF, oErr := os.Open(srcAbs); oErr == nil {
					n, cErr := io.Copy(tmpFile, srcF)
					_ = srcF.Close()
					if cErr == nil && n == rem.Size {
						copiedLocal = true
					} else {
						_ = tmpFile.Truncate(0)
						_, _ = tmpFile.Seek(0, 0)
					}
				}
			}
		}
	}

	if !copiedLocal {
		if rem.Size > 0 && len(rem.Blocks) == 0 {
			return rem, fmt.Errorf("missing block metadata for %s", rem.RelPath)
		}
		if rem.Size > chunker.DefaultBlockSize {
			_ = tmpFile.Truncate(rem.Size)
		}

		localBlocks := make(map[string]protocol.BlockInfo)
		if info, sErr := os.Stat(dstPath); sErr == nil && !info.IsDir() {
			if len(loc.Blocks) == 0 && !loc.Deleted && !loc.IsDir && loc.Size == info.Size() && loc.Size > 0 {
				if loadedBlocks, bErr := m.store.GetLocalFileBlocks(f.FolderID, loc.RelPath); bErr == nil {
					loc.Blocks = loadedBlocks
				}
			}
			if len(loc.Blocks) > 0 && loc.Size == info.Size() {
				for _, b := range loc.Blocks {
					localBlocks[b.Hash] = b
				}
			} else if existingBlocks, _, _, hErr := chunker.HashFileBlocks(dstPath, rem.BlockSize); hErr == nil {
				for _, b := range existingBlocks {
					localBlocks[b.Hash] = b
				}
			}
		}

		var oldFile *os.File
		if len(localBlocks) > 0 {
			oldFile, _ = os.Open(dstPath)
			if oldFile != nil {
				defer oldFile.Close()
			}
		}

		var missing []protocol.BlockInfo
		for _, blk := range rem.Blocks {
			var reused bool
			if oldFile != nil {
				if lb, ok := localBlocks[blk.Hash]; ok && lb.Size == blk.Size {
					bufPtr := chunker.GetBufferSize(lb.Size)
					buf := (*bufPtr)[:lb.Size]
					if n, rErr := oldFile.ReadAt(buf, lb.Offset); (rErr == nil || rErr == io.EOF) && n == lb.Size && chunker.HashBytes(buf) == blk.Hash {
						if _, wErr := tmpFile.WriteAt(buf, blk.Offset); wErr != nil {
							chunker.PutBufferSized(bufPtr)
							return rem, wErr
						}
						reused = true
					}
					chunker.PutBufferSized(bufPtr)
				}
			}
			if !reused {
				missing = append(missing, blk)
			}
		}

		if len(missing) > 0 {
			// Cap batch payload budget around 4 MiB so large adaptive blocks (4-16 MiB) don't over-buffer
			blkSz := rem.BlockSize
			if blkSz <= 0 {
				blkSz = chunker.DefaultBlockSize
			}
			batchSize := (4 * 1024 * 1024) / blkSz
			if batchSize < 1 {
				batchSize = 1
			}
			if batchSize > 8 {
				batchSize = 8
			}

			var batches [][]protocol.BlockInfo
			for i := 0; i < len(missing); i += batchSize {
				end := i + batchSize
				if end > len(missing) {
					end = len(missing)
				}
				batches = append(batches, missing[i:end])
			}

			workers := 4
			if len(batches) < workers {
				workers = len(batches)
			}
			pullCtx, pullCancel := context.WithCancel(ctx)
			defer pullCancel()

			jobs := make(chan []protocol.BlockInfo, len(batches))
			for _, b := range batches {
				jobs <- b
			}
			close(jobs)

			var firstErr error
			var errOnce sync.Once
			var wg sync.WaitGroup
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for batch := range jobs {
						if pullCtx.Err() != nil {
							return
						}
						fErr := m.fetchRemoteBlockBatch(pullCtx, peerID, f.FolderID, rem.RelPath, batch, func(blk protocol.BlockInfo, data []byte, wireLen int) error {
							if _, wErr := tmpFile.WriteAt(data, blk.Offset); wErr != nil {
								return wErr
							}
							m.addFolderRx(f.FolderID, uint64(wireLen))
							return nil
						})
						if fErr != nil {
							errOnce.Do(func() {
								firstErr = fErr
								pullCancel()
							})
							return
						}
					}
				}()
			}
			wg.Wait()
			if firstErr != nil {
				return rem, firstErr
			}
		}

		if oldFile != nil {
			_ = oldFile.Close()
		}
	}

	// Zero-re-read Merkle verification: all blocks are already BLAKE3 verified on reuse/receipt
	if rem.Size > 0 && len(rem.Blocks) > 0 {
		var covered int64
		for _, b := range rem.Blocks {
			covered += int64(b.Size)
		}
		if covered != rem.Size {
			return rem, fmt.Errorf("block coverage mismatch for %s: %d != %d", rem.RelPath, covered, rem.Size)
		}
		if rem.FileHash != "" && chunker.ComputeMerkleFileHash(rem.Blocks) != rem.FileHash {
			return rem, fmt.Errorf("merkle file hash mismatch for %s", rem.RelPath)
		}
	}

	if err := tmpFile.Truncate(rem.Size); err != nil {
		return rem, err
	}
	if err := tmpFile.Sync(); err != nil {
		return rem, err
	}
	_ = tmpFile.Close()

	if runtime.GOOS != "windows" && rem.ModeBits != 0 {
		_ = os.Chmod(tmpPath, os.FileMode(rem.ModeBits))
	}
	if rem.MtimeNs > 0 {
		t := time.Unix(0, rem.MtimeNs)
		_ = os.Chtimes(tmpPath, t, t)
	}

	m.handles.ClosePath(dstPath)
	m.markRecentWrite(f.FolderID, dstPath)

	var bakPath string
	shouldArchive := dstPath == filepath.Join(f.LocalPath, filepath.FromSlash(rem.RelPath))
	if info, sErr := os.Stat(dstPath); sErr == nil {
		if info.IsDir() {
			// Respect versioning policy when a remote file replaces an existing directory
			if shouldArchive {
				_ = ArchiveFile(f.LocalPath, rem.RelPath, f.Versioning, f.KeepVersions)
			} else {
				_ = os.RemoveAll(dstPath)
			}
		} else if shouldArchive {
			bakPath = filepath.Join(filepath.Dir(dstPath), fmt.Sprintf(".%s.gens.bak.%s", filepath.Base(dstPath), randomHex(6)))
			if err := os.Rename(dstPath, bakPath); err != nil {
				return rem, err
			}
		}
	}
	if err := os.Rename(tmpPath, dstPath); err != nil {
		if bakPath != "" {
			_ = os.Rename(bakPath, dstPath)
		}
		return rem, err
	}
	cleanupTmp = false
	if bakPath != "" {
		_ = ArchiveSourceFile(f.LocalPath, rem.RelPath, bakPath, f.Versioning, f.KeepVersions)
	}
	return rem, nil
}

func (m *Manager) fetchRemoteBlockBatch(ctx context.Context, peerID, folderID, relPath string, batch []protocol.BlockInfo, onBlock func(protocol.BlockInfo, []byte, int) error) error {
	if len(batch) == 1 {
		data, ptr, wireLen, err := m.fetchRemoteBlock(ctx, peerID, folderID, relPath, batch[0])
		if err != nil {
			return err
		}
		wErr := onBlock(batch[0], data, wireLen)
		if ptr != nil {
			chunker.PutBufferSized(ptr)
		}
		return wErr
	}

	stream, release, err := m.p2p.OpenTrackedDataStream(ctx, peerID, protocol.DataReqSyncMultiBlock)
	if err != nil {
		return err
	}
	stopCancel := context.AfterFunc(ctx, func() {
		stream.CancelRead(1)
		stream.CancelWrite(1)
		_ = stream.Close()
	})
	defer stopCancel()
	defer release()
	defer stream.Close()

	reqs := make([]protocol.SyncBlockRequest, len(batch))
	for i, blk := range batch {
		reqs[i] = protocol.SyncBlockRequest{
			FolderID: folderID,
			RelPath:  relPath,
			Offset:   blk.Offset,
			Size:     blk.Size,
			Hash:     blk.Hash,
		}
	}
	_ = stream.SetDeadline(time.Now().Add(streamIOTimeout))
	if err := protocol.WriteFrame(stream, protocol.SyncMultiBlockRequest{
		FolderID:   folderID,
		RelPath:    relPath,
		AcceptZstd: m.p2p.CompressionEnabled(),
		Blocks:     reqs,
	}); err != nil {
		return err
	}

	var resp protocol.SyncBlockResponse
	_ = stream.SetDeadline(time.Now().Add(streamIOTimeout))
	if err := protocol.ReadFrame(stream, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}

	var hdr [5]byte
	for _, blk := range batch {
		_ = stream.SetDeadline(time.Now().Add(streamIOTimeout))
		if _, err := io.ReadFull(stream, hdr[:]); err != nil {
			return err
		}
		compressed := (hdr[0] & 0x01) != 0
		wireLen := int(binary.BigEndian.Uint32(hdr[1:5]))
		if wireLen <= 0 || wireLen > chunker.MaxBlockSize+1024*1024 {
			return fmt.Errorf("invalid multi-block wire size: %d", wireLen)
		}

		wirePtr := chunker.GetBufferSize(wireLen)
		wireBuf := (*wirePtr)[:wireLen]
		if _, err := m.p2p.LimitedReadFull(ctx, stream, wireBuf); err != nil {
			chunker.PutBufferSized(wirePtr)
			return err
		}

		var rawBuf []byte
		var rawPtr *[]byte
		if compressed {
			rawPtr = chunker.GetBufferSize(blk.Size)
			dec, dErr := chunker.DecompressBlock(wireBuf, (*rawPtr)[:0], blk.Size)
			chunker.PutBufferSized(wirePtr)
			if dErr != nil {
				chunker.PutBufferSized(rawPtr)
				return dErr
			}
			rawBuf = dec
		} else {
			if wireLen != blk.Size {
				chunker.PutBufferSized(wirePtr)
				return fmt.Errorf("uncompressed block size mismatch: %d != %d", wireLen, blk.Size)
			}
			rawPtr = wirePtr
			rawBuf = wireBuf
		}

		if blk.Hash != "" && chunker.HashBytes(rawBuf) != blk.Hash {
			chunker.PutBufferSized(rawPtr)
			return fmt.Errorf("corrupted block hash from %s", peerID)
		}
		err := onBlock(blk, rawBuf, wireLen)
		chunker.PutBufferSized(rawPtr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) fetchRemoteBlock(ctx context.Context, peerID, folderID, relPath string, blk protocol.BlockInfo) ([]byte, *[]byte, int, error) {
	stream, release, err := m.p2p.OpenTrackedDataStream(ctx, peerID, protocol.DataReqSyncBlock)
	if err != nil {
		return nil, nil, 0, err
	}
	stopCancel := context.AfterFunc(ctx, func() {
		stream.CancelRead(1)
		stream.CancelWrite(1)
		_ = stream.Close()
	})
	defer stopCancel()
	defer release()
	defer stream.Close()

	req := protocol.SyncBlockRequest{
		FolderID:   folderID,
		RelPath:    relPath,
		Offset:     blk.Offset,
		Size:       blk.Size,
		Hash:       blk.Hash,
		AcceptZstd: m.p2p.CompressionEnabled(),
	}
	_ = stream.SetDeadline(time.Now().Add(streamIOTimeout))
	if err := protocol.WriteFrame(stream, req); err != nil {
		return nil, nil, 0, err
	}

	var resp protocol.SyncBlockResponse
	_ = stream.SetDeadline(time.Now().Add(streamIOTimeout))
	if err := protocol.ReadFrame(stream, &resp); err != nil {
		return nil, nil, 0, err
	}
	if !resp.OK {
		return nil, nil, 0, errors.New(resp.Error)
	}

	wireLen := blk.Size
	if resp.Compressed && resp.WireSize > 0 {
		wireLen = resp.WireSize
	}
	if wireLen <= 0 || wireLen > chunker.MaxBlockSize+1024*1024 {
		return nil, nil, 0, fmt.Errorf("invalid wire size: %d", wireLen)
	}

	wirePtr := chunker.GetBufferSize(wireLen)
	wireBuf := (*wirePtr)[:wireLen]
	_ = stream.SetDeadline(time.Now().Add(streamIOTimeout))
	if _, err := m.p2p.LimitedReadFull(ctx, stream, wireBuf); err != nil {
		chunker.PutBufferSized(wirePtr)
		return nil, nil, 0, err
	}

	var rawBuf []byte
	var rawPtr *[]byte
	if resp.Compressed {
		rawPtr = chunker.GetBufferSize(blk.Size)
		dec, dErr := chunker.DecompressBlock(wireBuf, (*rawPtr)[:0], blk.Size)
		chunker.PutBufferSized(wirePtr)
		if dErr != nil {
			chunker.PutBufferSized(rawPtr)
			return nil, nil, 0, dErr
		}
		rawBuf = dec
	} else {
		rawPtr = wirePtr
		rawBuf = wireBuf
	}

	if blk.Hash != "" && chunker.HashBytes(rawBuf) != blk.Hash {
		chunker.PutBufferSized(rawPtr)
		return nil, nil, 0, fmt.Errorf("corrupted block hash from %s", peerID)
	}
	return rawBuf, rawPtr, wireLen, nil
}

// ServeBlockStream serves a peer's request for a single block using the pooled FileHandleCache.
func (m *Manager) ServeBlockStream(ctx context.Context, peerID string, stream *quic.Stream) {
	defer stream.Close()
	stopCancel := context.AfterFunc(ctx, func() {
		stream.CancelRead(1)
		stream.CancelWrite(1)
		_ = stream.Close()
	})
	defer stopCancel()

	_ = stream.SetDeadline(time.Now().Add(streamIOTimeout))
	var req protocol.SyncBlockRequest
	if err := protocol.ReadFrame(stream, &req); err != nil {
		return
	}

	f, err := m.store.GetFolder(req.FolderID)
	if err != nil || f.Paused || !containsPeer(f.Peers, peerID) {
		_ = protocol.WriteFrame(stream, protocol.SyncBlockResponse{OK: false, Error: "folder not shared or paused"})
		return
	}

	fullPath, err := safeJoin(f.LocalPath, req.RelPath)
	if err != nil {
		_ = protocol.WriteFrame(stream, protocol.SyncBlockResponse{OK: false, Error: "invalid path"})
		return
	}
	if req.Size <= 0 || req.Size > chunker.MaxBlockSize {
		_ = protocol.WriteFrame(stream, protocol.SyncBlockResponse{OK: false, Error: "invalid block size"})
		return
	}

	bufPtr := chunker.GetBufferSize(req.Size)
	defer chunker.PutBufferSized(bufPtr)
	data := (*bufPtr)[:req.Size]
	if err := chunker.ReadBlockInto(fullPath, req.Offset, req.Size, req.Hash, data); err != nil {
		_ = protocol.WriteFrame(stream, protocol.SyncBlockResponse{OK: false, Error: err.Error()})
		return
	}

	wireData := data
	compressed := false
	if req.AcceptZstd {
		wireData, compressed = chunker.CompressBlockIfWorthwhile(req.RelPath, data)
	}

	_ = stream.SetDeadline(time.Now().Add(streamIOTimeout))
	if err := protocol.WriteFrame(stream, protocol.SyncBlockResponse{
		OK:         true,
		Compressed: compressed,
		WireSize:   len(wireData),
	}); err != nil {
		return
	}
	_ = stream.SetDeadline(time.Now().Add(streamIOTimeout))
	if _, err := m.p2p.LimitedWrite(ctx, stream, wireData); err == nil {
		m.addFolderTx(req.FolderID, uint64(len(wireData)))
	}
}

// ServeMultiBlockStream serves multiple blocks sequentially over one QUIC stream using FileHandleCache + optional Zstd.
func (m *Manager) ServeMultiBlockStream(ctx context.Context, peerID string, stream *quic.Stream) {
	defer stream.Close()
	stopCancel := context.AfterFunc(ctx, func() {
		stream.CancelRead(1)
		stream.CancelWrite(1)
		_ = stream.Close()
	})
	defer stopCancel()

	_ = stream.SetDeadline(time.Now().Add(streamIOTimeout))
	var req protocol.SyncMultiBlockRequest
	if err := protocol.ReadFrame(stream, &req); err != nil {
		return
	}

	f, err := m.store.GetFolder(req.FolderID)
	if err != nil || f.Paused || !containsPeer(f.Peers, peerID) {
		_ = protocol.WriteFrame(stream, protocol.SyncBlockResponse{OK: false, Error: "folder not shared or paused"})
		return
	}

	fullPath, err := safeJoin(f.LocalPath, req.RelPath)
	if err != nil {
		_ = protocol.WriteFrame(stream, protocol.SyncBlockResponse{OK: false, Error: "invalid path"})
		return
	}

	if len(req.Blocks) == 0 || len(req.Blocks) > 64 {
		_ = protocol.WriteFrame(stream, protocol.SyncBlockResponse{OK: false, Error: "invalid block batch size"})
		return
	}

	srcFile, err := os.Open(fullPath)
	if err != nil {
		_ = protocol.WriteFrame(stream, protocol.SyncBlockResponse{OK: false, Error: err.Error()})
		return
	}
	defer srcFile.Close()

	_ = stream.SetDeadline(time.Now().Add(streamIOTimeout))
	if err := protocol.WriteFrame(stream, protocol.SyncBlockResponse{OK: true}); err != nil {
		return
	}

	var hdr [5]byte
	for _, blk := range req.Blocks {
		if blk.Size <= 0 || blk.Size > chunker.MaxBlockSize {
			return
		}
		bufPtr := chunker.GetBufferSize(blk.Size)
		data := (*bufPtr)[:blk.Size]
		n, rErr := srcFile.ReadAt(data, blk.Offset)
		if (rErr != nil && rErr != io.EOF) || n != blk.Size {
			chunker.PutBufferSized(bufPtr)
			return
		}
		if blk.Hash != "" && chunker.HashBytes(data) != blk.Hash {
			chunker.PutBufferSized(bufPtr)
			return
		}

		wireData := data
		compressed := false
		if req.AcceptZstd {
			wireData, compressed = chunker.CompressBlockIfWorthwhile(req.RelPath, data)
		}

		hdr[0] = 0
		if compressed {
			hdr[0] = 1
		}
		binary.BigEndian.PutUint32(hdr[1:5], uint32(len(wireData)))

		_ = stream.SetDeadline(time.Now().Add(streamIOTimeout))
		if _, err := stream.Write(hdr[:]); err != nil {
			chunker.PutBufferSized(bufPtr)
			return
		}
		if _, err := m.p2p.LimitedWrite(ctx, stream, wireData); err != nil {
			chunker.PutBufferSized(bufPtr)
			return
		}
		chunker.PutBufferSized(bufPtr)
		m.addFolderTx(req.FolderID, uint64(len(wireData)))
	}
}

// OverrideChanges forces remote peers to match local state on a send_only folder.
func (m *Manager) OverrideChanges(folderID string) error {
	mu := m.folderPullMutex(folderID)
	mu.Lock()
	defer mu.Unlock()

	f, err := m.store.GetFolder(folderID)
	if err != nil {
		return err
	}
	if f.Mode != protocol.ModeSendOnly {
		return errors.New("override only applies to send_only folders")
	}

	localIdx, err := m.store.GetLocalIndex(folderID)
	if err != nil {
		return err
	}

	// Merge clocks across all remote peers per RelPath so 3+ peer meshes don't emit duplicate updates
	mergedRemoteClocks := make(map[string]protocol.FileMetadata)
	for _, peerID := range f.Peers {
		remoteIdx, _ := m.store.GetRemoteScanIndex(folderID, peerID)
		for rel, rem := range remoteIdx {
			cur, exists := mergedRemoteClocks[rel]
			if !exists {
				mergedRemoteClocks[rel] = rem
				continue
			}
			cur.Version = MergeClocks(cur.Version, rem.Version)
			mergedRemoteClocks[rel] = cur
		}
	}

	var updates []protocol.FileMetadata
	for rel, rem := range mergedRemoteClocks {
		loc, ok := localIdx[rel]
		if !ok {
			vc := BumpClock(rem.Version, m.myID)
			updates = append(updates, protocol.FileMetadata{
				RelPath: rel,
				IsDir:   rem.IsDir,
				Deleted: true,
				Version: vc,
			})
			continue
		}
		cmp := CompareClocks(loc.Version, rem.Version)
		if cmp == RelOlder || cmp == RelConflict {
			loc.Version = BumpClock(MergeClocks(loc.Version, rem.Version), m.myID)
			updates = append(updates, loc)
		}
	}
	if len(updates) > 0 {
		if err := m.store.UpsertLocalFiles(folderID, updates); err != nil {
			return err
		}
		m.broadcastIndexDelta(*f, updates)
	}
	m.TriggerScan(folderID)
	return nil
}

// RevertLocalChanges reverts unexpected local modifications and extra local files on a receive_only folder.
func (m *Manager) RevertLocalChanges(ctx context.Context, folderID string) error {
	mu := m.folderPullMutex(folderID)
	mu.Lock()
	defer mu.Unlock()

	f, err := m.store.GetFolder(folderID)
	if err != nil {
		return err
	}
	if f.Mode != protocol.ModeReceiveOnly {
		return errors.New("revert only applies to receive_only folders")
	}
	localIdx, err := m.store.GetLocalIndex(folderID)
	if err != nil {
		return err
	}

	remoteUnion := make(map[string]protocol.FileMetadata)
	for _, peerID := range f.Peers {
		remoteIdx, _ := m.store.GetRemoteScanIndex(folderID, peerID)
		for rel, rem := range remoteIdx {
			cur, exists := remoteUnion[rel]
			if !exists || CompareClocks(cur.Version, rem.Version) == RelOlder {
				remoteUnion[rel] = rem
			}
		}
	}

	var reset []protocol.FileMetadata
	var extraPaths []string
	for rel, loc := range localIdx {
		if rem, ok := remoteUnion[rel]; ok {
			cmp := CompareClocks(loc.Version, rem.Version)
			if cmp == RelNewer || cmp == RelConflict {
				loc.Version = protocol.VectorClock{}
				reset = append(reset, loc)
			}
		} else if !loc.Deleted {
			extraPaths = append(extraPaths, rel)
		}
	}

	// Remove extra local files deepest-first so nested files archive cleanly before parent dirs
	sort.Slice(extraPaths, func(i, j int) bool {
		di := strings.Count(extraPaths[i], "/")
		dj := strings.Count(extraPaths[j], "/")
		if di != dj {
			return di > dj
		}
		return extraPaths[i] > extraPaths[j]
	})
	for _, rel := range extraPaths {
		if abs, jErr := safeJoin(f.LocalPath, rel); jErr == nil {
			m.handles.ClosePath(abs)
			m.markRecentWrite(f.FolderID, abs)
			_ = ArchiveFile(f.LocalPath, rel, f.Versioning, f.KeepVersions)
		}
	}
	if len(extraPaths) > 0 {
		_ = m.store.DeleteLocalFiles(folderID, extraPaths)
	}
	if len(reset) > 0 {
		_ = m.store.UpsertLocalFiles(folderID, reset)
	}
	m.reconcileFolderLocked(ctx, *f)
	return nil
}

func (m *Manager) Statuses() []FolderStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]FolderStatus, 0, len(m.statuses))
	for _, st := range m.statuses {
		cp := *st
		if len(st.CaseCollisions) > 0 {
			cp.CaseCollisions = append([]string(nil), st.CaseCollisions...)
		}
		m.folderCntMu.RLock()
		if fc, ok := m.folderCnts[st.FolderID]; ok {
			cp.RxBps = fc.rxBps.Load()
			cp.TxBps = fc.txBps.Load()
		}
		m.folderCntMu.RUnlock()
		out = append(out, cp)
	}
	return out
}

func (m *Manager) markRecentWrite(folderID, absPath string) {
	now := time.Now().UnixNano()
	key := folderID + "|" + filepath.Clean(absPath)
	m.recentMu.Lock()
	m.recentWrites[key] = now
	if len(m.recentWrites) > 256 {
		cutoff := now - int64(3*time.Second)
		for k, ts := range m.recentWrites {
			if ts < cutoff {
				delete(m.recentWrites, k)
			}
		}
	}
	m.recentMu.Unlock()
}

func (m *Manager) isRecentWrite(folderID, absPath string) bool {
	key := folderID + "|" + filepath.Clean(absPath)
	now := time.Now().UnixNano()
	m.recentMu.Lock()
	ts, ok := m.recentWrites[key]
	if ok && now-ts > int64(2*time.Second) {
		delete(m.recentWrites, key)
		ok = false
	}
	m.recentMu.Unlock()
	return ok
}

func (m *Manager) ensureWatcher(f store.FolderRecord) {
	m.mu.Lock()
	if spec, exists := m.watchers[f.FolderID]; exists {
		if spec.localPath == f.LocalPath && spec.ignorePatterns == f.IgnorePatterns {
			m.mu.Unlock()
			return
		}
		spec.cancel()
		delete(m.watchers, f.FolderID)
	}
	wCtx, cancel := context.WithCancel(m.rootCtx)
	m.watchers[f.FolderID] = watcherSpec{
		localPath:      f.LocalPath,
		ignorePatterns: f.IgnorePatterns,
		cancel:         cancel,
	}
	m.mu.Unlock()

	go func() {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			log.Printf("[syncer] fsnotify init failed for %s: %v", f.FolderID, err)
			return
		}
		defer watcher.Close()

		_ = os.MkdirAll(f.LocalPath, 0755)
		matcher := LoadIgnoreMatcher(f.LocalPath, f.IgnorePatterns)
		addWatchDirs(watcher, f.LocalPath, f.LocalPath, matcher)

		var dirtyMu sync.Mutex
		dirtySet := make(map[string]struct{})
		var forceFull bool
		var debounceTimer *time.Timer
		defer func() {
			dirtyMu.Lock()
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			dirtyMu.Unlock()
		}()

		flushDirty := func() {
			dirtyMu.Lock()
			full := forceFull
			forceFull = false
			paths := make([]string, 0, len(dirtySet))
			for p := range dirtySet {
				paths = append(paths, p)
			}
			dirtySet = make(map[string]struct{})
			dirtyMu.Unlock()

			if full || len(paths) == 0 {
				m.TriggerScan(f.FolderID)
			} else {
				m.TriggerPathsScan(f.FolderID, paths)
			}
		}

		scheduleDirty := func(rel string, full bool) {
			dirtyMu.Lock()
			if full {
				forceFull = true
			} else if rel != "" {
				dirtySet[rel] = struct{}{}
			}
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			debounceTimer = time.AfterFunc(500*time.Millisecond, flushDirty)
			dirtyMu.Unlock()
		}

		for {
			select {
			case <-wCtx.Done():
				return
			case ev, ok := <-watcher.Events:
				if !ok {
					return
				}
				base := filepath.Base(ev.Name)
				if strings.EqualFold(base, ".gensignore") {
					matcher = LoadIgnoreMatcher(f.LocalPath, f.IgnorePatterns)
					scheduleDirty("", true)
					continue
				}
				if IsInternalPath(f.LocalPath, ev.Name) || m.isRecentWrite(f.FolderID, ev.Name) {
					continue
				}
				if ev.Has(fsnotify.Create) || ev.Has(fsnotify.Rename) {
					if info, err := os.Stat(ev.Name); err == nil && info.IsDir() {
						addWatchDirs(watcher, f.LocalPath, ev.Name, matcher)
					}
				}
				rel := ""
				if r, rErr := filepath.Rel(f.LocalPath, ev.Name); rErr == nil {
					rel = filepath.ToSlash(r)
				}
				scheduleDirty(rel, rel == "" || rel == ".")
			case wErr, ok := <-watcher.Errors:
				if !ok {
					return
				}
				if wErr != nil {
					log.Printf("[syncer] fsnotify watcher error (%s): %v", f.FolderID, wErr)
				}
			}
		}
	}()
}

func addWatchDirs(w *fsnotify.Watcher, rootDir, startDir string, matcher *IgnoreMatcher) {
	warned := false
	_ = filepath.WalkDir(startDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return nil
		}
		if path != rootDir {
			rel, err := filepath.Rel(rootDir, path)
			if err == nil && matcher.Match(filepath.ToSlash(rel), true) {
				return filepath.SkipDir
			}
		}
		if err := w.Add(path); err != nil && !warned {
			warned = true
			log.Printf("[syncer] fsnotify watch failed for %s (%v) — falling back to periodic rescan", path, err)
		}
		return nil
	})
}

func (m *Manager) broadcastIndexDelta(f store.FolderRecord, files []protocol.FileMetadata) {
	if len(files) == 0 {
		return
	}
	_, _, epoch, _ := m.store.GetLocalIndexSummaryWithEpoch(f.FolderID)
	for _, peerID := range f.Peers {
		if m.p2p.IsConnected(peerID) {
			m.sendFolderIndexBatched(peerID, f.FolderID, false, epoch, files)
		}
	}
}

func (m *Manager) getOrCreateStatus(folderID string) *FolderStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getOrCreateStatusLocked(folderID)
}

func (m *Manager) getOrCreateStatusLocked(folderID string) *FolderStatus {
	st, ok := m.statuses[folderID]
	if !ok {
		st = &FolderStatus{FolderID: folderID, State: "idle"}
		m.statuses[folderID] = st
	}
	return st
}

func (m *Manager) setStatusState(folderID, state, lastErr string) {
	m.mu.Lock()
	st := m.getOrCreateStatusLocked(folderID)
	st.State = state
	st.LastError = lastErr
	m.mu.Unlock()
	m.emitStatuses()
}

func (m *Manager) emitStatuses() {
	if m.onEmit != nil {
		m.onEmit("folders_updated", m.Statuses())
	}
}

func (m *Manager) folderPullMutex(folderID string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	mu, ok := m.pullMu[folderID]
	if !ok {
		mu = &sync.Mutex{}
		m.pullMu[folderID] = mu
	}
	return mu
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
		return "", fmt.Errorf("path escapes folder root: %s", rel)
	}
	return full, nil
}

func containsPeer(peers []string, peerID string) bool {
	norm := protocol.NormalizeDeviceID(peerID)
	for _, p := range peers {
		if protocol.NormalizeDeviceID(p) == norm {
			return true
		}
	}
	return false
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// --- Conflicts & Versions API Helpers ---

type ConflictItem struct {
	RelPath      string `json:"rel_path"`
	OriginalPath string `json:"original_path"`
	Size         int64  `json:"size"`
	ModTime      int64  `json:"mod_time"`
}

type VersionItem struct {
	ArchivePath string `json:"archive_path"`
	OrigRelPath string `json:"orig_rel_path"`
	Size        int64  `json:"size"`
	ModTime     int64  `json:"mod_time"`
}

func (m *Manager) ListConflicts(folderID string) ([]ConflictItem, error) {
	f, err := m.store.GetFolder(folderID)
	if err != nil {
		return nil, err
	}
	var out []ConflictItem
	_ = filepath.WalkDir(f.LocalPath, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() && d.Name() == ".gensversions" {
			return filepath.SkipDir
		}
		if !d.IsDir() && strings.Contains(d.Name(), ".sync-conflict-") {
			rel, rErr := filepath.Rel(f.LocalPath, p)
			if rErr != nil {
				return nil
			}
			info, sErr := d.Info()
			if sErr != nil {
				return nil
			}
			out = append(out, ConflictItem{
				RelPath:      filepath.ToSlash(rel),
				OriginalPath: stripConflictSuffix(filepath.ToSlash(rel)),
				Size:         info.Size(),
				ModTime:      info.ModTime().Unix(),
			})
		}
		return nil
	})
	return out, nil
}

func (m *Manager) ResolveConflict(folderID, conflictRel, action string) error {
	f, err := m.store.GetFolder(folderID)
	if err != nil {
		return err
	}
	conflictAbs, err := safeJoin(f.LocalPath, conflictRel)
	if err != nil {
		return err
	}
	if !strings.Contains(filepath.Base(conflictAbs), ".sync-conflict-") {
		return errors.New("not a sync-conflict file")
	}

	switch action {
	case "delete":
		if err := os.Remove(conflictAbs); err != nil {
			return err
		}
	case "keep_conflict":
		origRel := stripConflictSuffix(conflictRel)
		origAbs, err := safeJoin(f.LocalPath, origRel)
		if err != nil {
			return err
		}
		_ = ArchiveFile(f.LocalPath, origRel, f.Versioning, f.KeepVersions)
		if err := os.Rename(conflictAbs, origAbs); err != nil {
			return err
		}
	default:
		return errors.New("action must be 'keep_conflict' or 'delete'")
	}
	m.TriggerScan(folderID)
	return nil
}

func (m *Manager) ListVersions(folderID string) ([]VersionItem, error) {
	f, err := m.store.GetFolder(folderID)
	if err != nil {
		return nil, err
	}
	vRoot := filepath.Join(f.LocalPath, ".gensversions")
	if _, err := os.Stat(vRoot); os.IsNotExist(err) {
		return []VersionItem{}, nil
	}
	var out []VersionItem
	_ = filepath.WalkDir(vRoot, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, rErr := filepath.Rel(vRoot, p)
		if rErr != nil {
			return nil
		}
		info, sErr := d.Info()
		if sErr != nil {
			return nil
		}
		slashRel := filepath.ToSlash(rel)
		out = append(out, VersionItem{
			ArchivePath: slashRel,
			OrigRelPath: stripVersionStamp(slashRel),
			Size:        info.Size(),
			ModTime:     info.ModTime().Unix(),
		})
		return nil
	})
	return out, nil
}

func (m *Manager) RestoreVersion(folderID, archivePath string) error {
	f, err := m.store.GetFolder(folderID)
	if err != nil {
		return err
	}
	vRoot := filepath.Join(f.LocalPath, ".gensversions")
	srcAbs, err := safeJoin(vRoot, archivePath)
	if err != nil {
		return err
	}
	origRel := stripVersionStamp(archivePath)
	dstAbs, err := safeJoin(f.LocalPath, origRel)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dstAbs), 0755); err != nil {
		return err
	}
	if _, err := os.Stat(dstAbs); err == nil {
		_ = ArchiveFile(f.LocalPath, origRel, "trashcan", f.KeepVersions)
	}
	if err := moveFileFallback(srcAbs, dstAbs); err != nil {
		return err
	}
	m.TriggerScan(folderID)
	return nil
}

func stripConflictSuffix(rel string) string {
	idx := strings.Index(rel, ".sync-conflict-")
	if idx < 0 {
		return rel
	}
	rest := rel[idx+len(".sync-conflict-"):]
	dotIdx := strings.Index(rest, ".")
	if dotIdx < 0 {
		return rel[:idx]
	}
	return rel[:idx] + rest[dotIdx:]
}

func stripVersionStamp(rel string) string {
	dir := filepath.Dir(filepath.FromSlash(rel))
	base := filepath.Base(rel)
	tildeIdx := strings.LastIndex(base, "~")
	if tildeIdx < 0 {
		return rel
	}
	ext := filepath.Ext(base)
	origBase := base[:tildeIdx] + ext
	if dir == "." {
		return origBase
	}
	return filepath.ToSlash(filepath.Join(dir, origBase))
}
