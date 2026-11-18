package syncer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
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

type FolderStatus struct {
	FolderID       string `json:"folder_id"`
	State          string `json:"state"` // "idle" | "scanning" | "syncing" | "paused" | "error"
	LocalFiles     int    `json:"local_files"`
	LocalBytes     int64  `json:"local_bytes"`
	NeedFiles      int    `json:"need_files"`
	NeedBytes      int64  `json:"need_bytes"`
	RemoteDiverged int    `json:"remote_diverged"` // send_only: remote ahead
	LocalChanges   int    `json:"local_changes"`   // receive_only: local modified
	RxBps          int64  `json:"rx_bps"`
	TxBps          int64  `json:"tx_bps"`
	LastScanAt     int64  `json:"last_scan_at"`
	LastError      string `json:"last_error,omitempty"`
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
	watchers       map[string]watcherSpec
	pullMu         map[string]*sync.Mutex
	pendingBatches map[string]*indexBatchBuffer
	retryTimers    map[string]*time.Timer
	retryAttempts  map[string]int
	rootCtx        context.Context

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
		watchers:       make(map[string]watcherSpec),
		pullMu:         make(map[string]*sync.Mutex),
		pendingBatches: make(map[string]*indexBatchBuffer),
		retryTimers:    make(map[string]*time.Timer),
		retryAttempts:  make(map[string]int),
		recentWrites:   make(map[string]int64),
		rootCtx:        context.Background(),
	}
}

func (m *Manager) TransferCounters() (rx, tx uint64) {
	return m.bytesRx.Load(), m.bytesTx.Load()
}

func (m *Manager) Start(ctx context.Context) {
	m.rootCtx = ctx
	m.ReloadFolders()

	go func() {
		<-ctx.Done()
		m.handles.CloseAll()
	}()

	// Sample sync throughput every 1s for live UI speed badges
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		prevRx := m.bytesRx.Load()
		prevTx := m.bytesTx.Load()
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
				if dRx != oldRx || dTx != oldTx {
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
		st := m.getOrCreateStatus(f.FolderID)
		if f.Paused {
			m.setStatusState(f.FolderID, "paused", "")
			continue
		}
		if st.State == "paused" {
			m.setStatusState(f.FolderID, "idle", "")
		}
		m.ensureWatcher(f)
		m.TriggerScan(f.FolderID)
	}
}

func (m *Manager) TriggerScan(folderID string) {
	m.mu.Lock()
	ch, ok := m.scanCh[folderID]
	if !ok {
		ch = make(chan struct{}, 1)
		wCtx, cancel := context.WithCancel(m.rootCtx)
		m.scanCh[folderID] = ch
		m.scanStop[folderID] = cancel
		go m.folderWorker(wCtx, folderID, ch)
	}
	m.mu.Unlock()

	select {
	case ch <- struct{}{}:
	default:
	}
}

func (m *Manager) folderWorker(wCtx context.Context, folderID string, ch chan struct{}) {
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

	res, err := ScanFolder(f.LocalPath, m.myID, f.IgnorePatterns, prevIdx)
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
	m.mu.Unlock()

	if len(res.Changed) > 0 {
		m.broadcastIndexDelta(*f, res.Changed)
	}

	m.reconcileFolderLocked(ctx, *f)
}

func (m *Manager) sendFolderIndexBatched(peerID, folderID string, full bool, list []protocol.FileMetadata) {
	const maxFilesPerBatch = 350
	if len(list) == 0 {
		_ = m.p2p.SendControl(peerID, protocol.CtrlFolderIndex, protocol.FolderIndexMsg{
			FolderID:     folderID,
			Full:         full,
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

func (m *Manager) OnPeerConnected(ctx context.Context, peerID string) {
	folders, err := m.store.ListFolders()
	if err != nil {
		return
	}
	for _, f := range folders {
		if f.Paused || !containsPeer(f.Peers, peerID) {
			continue
		}
		knownRemoteSeq, _ := m.store.GetPeerRemoteMaxSeq(f.FolderID, peerID)
		localMaxSeq, localCount, _ := m.store.GetLocalIndexSummary(f.FolderID)
		_ = m.p2p.SendControl(peerID, protocol.CtrlFolderSummary, protocol.FolderSummaryMsg{
			FolderID:    f.FolderID,
			KnownMaxSeq: knownRemoteSeq,
			LocalMaxSeq: localMaxSeq,
			FileCount:   localCount,
		})
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

	localMaxSeq, localCount, err := m.store.GetLocalIndexSummary(f.FolderID)
	if err != nil {
		return
	}
	if msg.KnownMaxSeq > 0 && msg.KnownMaxSeq <= localMaxSeq && msg.FileCount == localCount {
		if msg.KnownMaxSeq == localMaxSeq {
			// Remote peer is already completely up to date; trigger local reconcile in case we need remote files.
			go m.reconcileFolder(ctx, *f)
			return
		}
		delta, err := m.store.GetLocalIndexSince(f.FolderID, msg.KnownMaxSeq)
		if err == nil {
			m.sendFolderIndexBatched(peerID, f.FolderID, false, delta)
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
	m.sendFolderIndexBatched(peerID, f.FolderID, true, list)
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

	filesToApply := msg.Files
	if msg.TotalBatches > 1 && msg.BatchID != "" {
		if msg.TotalBatches > 100000 || msg.BatchIndex < 0 || msg.BatchIndex >= msg.TotalBatches {
			return
		}
		key := fmt.Sprintf("%s|%s|%s", msg.FolderID, protocol.NormalizeDeviceID(peerID), msg.BatchID)
		m.mu.Lock()
		// Prune stale incomplete batches older than 5 minutes
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

	if err := m.store.UpsertRemoteFiles(msg.FolderID, peerID, msg.Full, filesToApply); err != nil {
		return
	}
	if !f.Paused {
		go m.reconcileFolder(ctx, *f)
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

	// Build best remote candidate per RelPath across all peers so 3+ peer meshes don't overwrite or duplicate pulls.
	type remoteCandidate struct {
		peerID string
		meta   protocol.FileMetadata
	}
	bestRemote := make(map[string]remoteCandidate)
	for _, peerID := range f.Peers {
		remoteIdx, err := m.store.GetRemoteIndex(f.FolderID, peerID)
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

	var tasks []pullTask
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
			relCmp = RelOlder // remote has file we never saw
		} else {
			relCmp = CompareClocks(loc.Version, rem.Version)
		}

		switch relCmp {
		case RelOlder: // remote strictly newer than local
			if f.Mode == protocol.ModeSendOnly {
				remoteDiverged++
				continue
			}
			tasks = append(tasks, pullTask{peerID: peerID, remote: rem})
			if !rem.Deleted && !rem.IsDir {
				needBytes += rem.Size
			}

		case RelConflict:
			if f.Mode == protocol.ModeSendOnly {
				remoteDiverged++
				continue
			}
			if f.Mode == protocol.ModeReceiveOnly {
				localChanges++
				continue
			}
			tasks = append(tasks, pullTask{peerID: peerID, remote: rem, conflict: true})
			if !rem.Deleted && !rem.IsDir {
				needBytes += rem.Size
			}

		case RelNewer:
			if f.Mode == protocol.ModeReceiveOnly && hasLoc {
				localChanges++
			}
		}
	}

	m.mu.Lock()
	st := m.getOrCreateStatusLocked(f.FolderID)
	st.NeedFiles = len(tasks)
	st.NeedBytes = needBytes
	st.RemoteDiverged = remoteDiverged
	st.LocalChanges = localChanges
	if len(tasks) > 0 {
		st.State = "syncing"
	} else {
		st.State = "idle"
		st.LastError = ""
		m.retryAttempts[f.FolderID] = 0
	}
	m.mu.Unlock()
	m.emitStatuses()

	if len(tasks) == 0 {
		return
	}

	var stateMu sync.Mutex
	var pulled []protocol.FileMetadata
	var conflictCreated bool
	var pullErrors int
	var lastPullErr error

	lookupHash := func(hash string) (string, bool) {
		stateMu.Lock()
		rel, ok := localByHash[hash]
		stateMu.Unlock()
		return rel, ok
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
			toSave := []protocol.FileMetadata{*updated}
			if uErr := m.store.UpsertLocalFiles(f.FolderID, toSave); uErr != nil {
				stateMu.Lock()
				pullErrors++
				lastPullErr = uErr
				stateMu.Unlock()
				return
			}
			saved := toSave[0]
			stateMu.Lock()
			pulled = append(pulled, saved)
			localIdx[t.remote.RelPath] = saved
			if !saved.Deleted && !saved.IsDir && saved.FileHash != "" && saved.Size > 0 {
				localByHash[saved.FileHash] = saved.RelPath
			}
			if madeConflict {
				conflictCreated = true
			}
			stateMu.Unlock()
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

	// Parallelize file pulls (up to 4 concurrent files) so small-file sync isn't serialized per-RTT/fsync
	workers := 4
	if len(tasks) < workers {
		workers = len(tasks)
	}
	if workers <= 1 {
		for _, t := range tasks {
			processTask(t)
		}
	} else {
		taskCh := make(chan pullTask, len(tasks))
		for _, t := range tasks {
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

	if err := m.pullFileContent(ctx, f, peerID, loc, rem, dstPath, lookupHash); err != nil {
		return nil, err
	}
	res := rem
	return &res, nil
}

func (m *Manager) resolveAndPullConflict(ctx context.Context, f store.FolderRecord, peerID string, loc, rem protocol.FileMetadata, lookupHash func(string) (string, bool)) (*protocol.FileMetadata, bool, error) {
	remoteWins := rem.MtimeNs > loc.MtimeNs || (rem.MtimeNs == loc.MtimeNs && peerID > m.myID)
	merged := MergeClocks(loc.Version, rem.Version)

	if remoteWins {
		// Preserve our losing local version as a conflict file, then replace main path with winner
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
			if err := m.pullFileContent(ctx, f, peerID, loc, rem, dstPath, lookupHash); err != nil {
				return nil, false, err
			}
		}
		out := rem
		// Converge deterministically to merged clock without mutating peerID's counter
		out.Version = merged
		return &out, conflictCreated, nil
	}

	// Local wins: preserve remote's losing edit as a conflict copy before converging clock,
	// so conflict copies are never lost regardless of which peer reconciles first.
	conflictCreated := false
	if !rem.Deleted && !rem.IsDir {
		conflictRel := ConflictRelPath(rem.RelPath, peerID, rem.MtimeNs)
		conflictDst, err := safeJoin(f.LocalPath, conflictRel)
		if err != nil {
			return nil, false, err
		}
		if _, statErr := os.Stat(conflictDst); os.IsNotExist(statErr) {
			if err := m.pullFileContent(ctx, f, peerID, protocol.FileMetadata{}, rem, conflictDst, lookupHash); err != nil {
				return nil, false, err
			}
			conflictCreated = true
		}
	}
	out := loc
	out.Version = merged
	return &out, conflictCreated, nil
}

func (m *Manager) pullFileContent(ctx context.Context, f store.FolderRecord, peerID string, loc, rem protocol.FileMetadata, dstPath string, lookupHash func(string) (string, bool)) error {
	if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
		return err
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
			return nil
		}
	}

	tmpPath := filepath.Join(filepath.Dir(dstPath), fmt.Sprintf(".%s.gens.tmp.%s", filepath.Base(dstPath), randomHex(6)))
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	cleanupTmp := true
	defer func() {
		_ = tmpFile.Close()
		if cleanupTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	// Rename/move local deduplication: if exact FileHash already exists locally, copy locally without network transfer.
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
		// Pre-allocate destination size for multi-block files to minimize NTFS/ext4/ZFS fragmentation
		if rem.Size > chunker.DefaultBlockSize {
			_ = tmpFile.Truncate(rem.Size)
		}

		// Build reusable local block map (lazy-loading blocks_json only when pulling a modified file)
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
			} else if existingBlocks, _, _, hErr := chunker.HashFileBlocks(dstPath, chunker.DefaultBlockSize); hErr == nil {
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
		bufPtr := chunker.GetBuffer()
		for _, blk := range rem.Blocks {
			var reused bool
			if oldFile != nil {
				if lb, ok := localBlocks[blk.Hash]; ok && lb.Size == blk.Size && lb.Size <= len(*bufPtr) {
					buf := (*bufPtr)[:lb.Size]
					if n, rErr := oldFile.ReadAt(buf, lb.Offset); rErr == nil && n == lb.Size && chunker.HashBytes(buf) == blk.Hash {
						if _, wErr := tmpFile.WriteAt(buf, blk.Offset); wErr != nil {
							chunker.PutBuffer(bufPtr)
							return wErr
						}
						reused = true
					}
				}
			}
			if !reused {
				missing = append(missing, blk)
			}
		}
		chunker.PutBuffer(bufPtr)

		if len(missing) > 0 {
			// Batch up to 8 blocks per QUIC stream to eliminate per-block RTT/open-stream overhead
			const batchSize = 8
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
						fErr := m.fetchRemoteBlockBatch(pullCtx, peerID, f.FolderID, rem.RelPath, batch, func(blk protocol.BlockInfo, data []byte) error {
							if _, wErr := tmpFile.WriteAt(data, blk.Offset); wErr != nil {
								return wErr
							}
							m.bytesRx.Add(uint64(len(data)))
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
				return firstErr
			}
		}

		if oldFile != nil {
			_ = oldFile.Close()
		}
	}

	if err := tmpFile.Truncate(rem.Size); err != nil {
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		return err
	}
	_ = tmpFile.Close()

	if rem.Size > 0 {
		_, actualHash, _, err := chunker.HashFileBlocks(tmpPath, chunker.DefaultBlockSize)
		if err != nil {
			return err
		}
		if rem.FileHash != "" && actualHash != rem.FileHash {
			return fmt.Errorf("file hash mismatch after transfer for %s", rem.RelPath)
		}
	}

	if runtime.GOOS != "windows" && rem.ModeBits != 0 {
		_ = os.Chmod(tmpPath, os.FileMode(rem.ModeBits))
	}
	if rem.MtimeNs > 0 {
		t := time.Unix(0, rem.MtimeNs)
		_ = os.Chtimes(tmpPath, t, t)
	}

	m.handles.ClosePath(dstPath)
	m.markRecentWrite(f.FolderID, dstPath)

	// Transactional replacement: back up existing dstPath before rename so dstPath is never lost if rename fails.
	var bakPath string
	shouldArchive := dstPath == filepath.Join(f.LocalPath, filepath.FromSlash(rem.RelPath))
	if info, sErr := os.Stat(dstPath); sErr == nil {
		if info.IsDir() {
			_ = os.RemoveAll(dstPath)
		} else if shouldArchive {
			bakPath = filepath.Join(filepath.Dir(dstPath), fmt.Sprintf(".%s.gens.bak.%s", filepath.Base(dstPath), randomHex(6)))
			if err := os.Rename(dstPath, bakPath); err != nil {
				return err
			}
		}
	}
	if err := os.Rename(tmpPath, dstPath); err != nil {
		if bakPath != "" {
			_ = os.Rename(bakPath, dstPath)
		}
		return err
	}
	cleanupTmp = false
	if bakPath != "" {
		_ = ArchiveSourceFile(f.LocalPath, rem.RelPath, bakPath, f.Versioning, f.KeepVersions)
	}
	return nil
}

func (m *Manager) fetchRemoteBlockBatch(ctx context.Context, peerID, folderID, relPath string, batch []protocol.BlockInfo, onBlock func(protocol.BlockInfo, []byte) error) error {
	if len(batch) == 1 {
		data, ptr, err := m.fetchRemoteBlock(ctx, peerID, folderID, relPath, batch[0])
		if err != nil {
			return err
		}
		wErr := onBlock(batch[0], data)
		if ptr != nil {
			chunker.PutBuffer(ptr)
		}
		return wErr
	}

	stream, release, err := m.p2p.OpenTrackedDataStream(ctx, peerID, protocol.DataReqSyncMultiBlock)
	if err != nil {
		return err
	}
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
	if err := protocol.WriteFrame(stream, protocol.SyncMultiBlockRequest{
		FolderID: folderID,
		RelPath:  relPath,
		Blocks:   reqs,
	}); err != nil {
		return err
	}

	var resp protocol.SyncBlockResponse
	if err := protocol.ReadFrame(stream, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}

	bufPtr := chunker.GetBuffer()
	defer chunker.PutBuffer(bufPtr)

	for _, blk := range batch {
		var buf []byte
		if blk.Size <= chunker.DefaultBlockSize {
			buf = (*bufPtr)[:blk.Size]
		} else {
			buf = make([]byte, blk.Size)
		}
		if _, err := m.p2p.LimitedReadFull(ctx, stream, buf); err != nil {
			return err
		}
		if blk.Hash != "" && chunker.HashBytes(buf) != blk.Hash {
			return fmt.Errorf("corrupted block hash from %s", peerID)
		}
		if err := onBlock(blk, buf); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) fetchRemoteBlock(ctx context.Context, peerID, folderID, relPath string, blk protocol.BlockInfo) ([]byte, *[]byte, error) {
	stream, release, err := m.p2p.OpenTrackedDataStream(ctx, peerID, protocol.DataReqSyncBlock)
	if err != nil {
		return nil, nil, err
	}
	defer release()
	defer stream.Close()

	req := protocol.SyncBlockRequest{
		FolderID: folderID,
		RelPath:  relPath,
		Offset:   blk.Offset,
		Size:     blk.Size,
		Hash:     blk.Hash,
	}
	if err := protocol.WriteFrame(stream, req); err != nil {
		return nil, nil, err
	}

	var resp protocol.SyncBlockResponse
	if err := protocol.ReadFrame(stream, &resp); err != nil {
		return nil, nil, err
	}
	if !resp.OK {
		return nil, nil, errors.New(resp.Error)
	}

	var bufPtr *[]byte
	var buf []byte
	if blk.Size <= chunker.DefaultBlockSize {
		bufPtr = chunker.GetBuffer()
		buf = (*bufPtr)[:blk.Size]
	} else {
		buf = make([]byte, blk.Size)
	}
	if _, err := m.p2p.LimitedReadFull(ctx, stream, buf); err != nil {
		if bufPtr != nil {
			chunker.PutBuffer(bufPtr)
		}
		return nil, nil, err
	}
	if blk.Hash != "" && chunker.HashBytes(buf) != blk.Hash {
		if bufPtr != nil {
			chunker.PutBuffer(bufPtr)
		}
		return nil, nil, fmt.Errorf("corrupted block hash from %s", peerID)
	}
	return buf, bufPtr, nil
}

// ServeBlockStream serves a peer's request for a single block of a synced folder.
func (m *Manager) ServeBlockStream(ctx context.Context, peerID string, stream *quic.Stream) {
	defer stream.Close()

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

	var bufPtr *[]byte
	var data []byte
	if req.Size > 0 && req.Size <= chunker.DefaultBlockSize {
		bufPtr = chunker.GetBuffer()
		defer chunker.PutBuffer(bufPtr)
		data = (*bufPtr)[:req.Size]
		err = chunker.ReadBlockInto(fullPath, req.Offset, req.Size, req.Hash, data)
	} else {
		data, err = chunker.ReadBlock(fullPath, req.Offset, req.Size, req.Hash)
	}
	if err != nil {
		_ = protocol.WriteFrame(stream, protocol.SyncBlockResponse{OK: false, Error: err.Error()})
		return
	}

	if err := protocol.WriteFrame(stream, protocol.SyncBlockResponse{OK: true}); err != nil {
		return
	}
	if _, err := m.p2p.LimitedWrite(ctx, stream, data); err == nil {
		m.bytesTx.Add(uint64(len(data)))
	}
}

// ServeMultiBlockStream serves multiple blocks of a file sequentially over one QUIC stream.
func (m *Manager) ServeMultiBlockStream(ctx context.Context, peerID string, stream *quic.Stream) {
	defer stream.Close()

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

	bufPtr := chunker.GetBuffer()
	defer chunker.PutBuffer(bufPtr)

	if err := protocol.WriteFrame(stream, protocol.SyncBlockResponse{OK: true}); err != nil {
		return
	}

	for _, blk := range req.Blocks {
		var data []byte
		if blk.Size > 0 && blk.Size <= chunker.DefaultBlockSize {
			data = (*bufPtr)[:blk.Size]
		} else {
			data = make([]byte, blk.Size)
		}
		n, rErr := srcFile.ReadAt(data, blk.Offset)
		if rErr != nil && rErr != io.EOF {
			return
		}
		if n != blk.Size {
			return
		}
		if blk.Hash != "" && chunker.HashBytes(data) != blk.Hash {
			return
		}
		if _, err := m.p2p.LimitedWrite(ctx, stream, data); err != nil {
			return
		}
		m.bytesTx.Add(uint64(len(data)))
	}
}

// OverrideChanges forces remote peers to match local state on a send_only folder.
func (m *Manager) OverrideChanges(folderID string) error {
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
	var updates []protocol.FileMetadata
	for _, peerID := range f.Peers {
		remoteIdx, _ := m.store.GetRemoteIndex(folderID, peerID)
		for rel, rem := range remoteIdx {
			loc, ok := localIdx[rel]
			if !ok {
				// File exists remotely but not locally -> push deletion dominating remote clock
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

// RevertLocalChanges reverts unexpected local modifications on a receive_only folder.
func (m *Manager) RevertLocalChanges(ctx context.Context, folderID string) error {
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
	var reset []protocol.FileMetadata
	for _, peerID := range f.Peers {
		remoteIdx, _ := m.store.GetRemoteIndex(folderID, peerID)
		for rel, loc := range localIdx {
			if rem, ok := remoteIdx[rel]; ok {
				cmp := CompareClocks(loc.Version, rem.Version)
				if cmp == RelNewer || cmp == RelConflict {
					// Reset local clock below remote so reconcileFolder pulls remote cleanly
					loc.Version = protocol.VectorClock{}
					reset = append(reset, loc)
				}
			}
		}
	}
	if len(reset) > 0 {
		_ = m.store.UpsertLocalFiles(folderID, reset)
	}
	go m.reconcileFolder(ctx, *f)
	return nil
}

func (m *Manager) Statuses() []FolderStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rx := m.rxBps.Load()
	tx := m.txBps.Load()
	out := make([]FolderStatus, 0, len(m.statuses))
	for _, st := range m.statuses {
		cp := *st
		cp.RxBps = rx
		cp.TxBps = tx
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

		var debounceTimer *time.Timer
		defer func() {
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
		}()
		triggerDebounced := func() {
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			debounceTimer = time.AfterFunc(500*time.Millisecond, func() {
				m.TriggerScan(f.FolderID)
			})
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
					triggerDebounced()
					continue
				}
				if IsInternalPath(f.LocalPath, ev.Name) || m.isRecentWrite(f.FolderID, ev.Name) {
					continue
				}
				if ev.Has(fsnotify.Create) {
					if info, err := os.Stat(ev.Name); err == nil && info.IsDir() {
						addWatchDirs(watcher, f.LocalPath, ev.Name, matcher)
					}
				}
				triggerDebounced()
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
	for _, peerID := range f.Peers {
		if m.p2p.IsConnected(peerID) {
			m.sendFolderIndexBatched(peerID, f.FolderID, false, files)
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
	// format: YYYYMMDD-HHMMSS-DEVICE.ext
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
