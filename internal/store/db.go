package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
	_ "modernc.org/sqlite"
)

type DeviceRecord struct {
	DeviceID       string `json:"device_id"`
	Name           string `json:"name"`
	AutoAcceptDrop bool   `json:"auto_accept_drop"`
	AddedAt        int64  `json:"added_at"`
	LastSeen       int64  `json:"last_seen"`
}

type PairRequestRecord struct {
	DeviceID     string   `json:"device_id"`
	Direction    string   `json:"direction"` // "incoming" | "outgoing"
	Name         string   `json:"name"`
	Message      string   `json:"message,omitempty"`
	Endpoint     string   `json:"endpoint,omitempty"`
	LANAddrs     []string `json:"lan_addrs,omitempty"`
	ObservedAddr string   `json:"observed_addr,omitempty"`
	CreatedAt    int64    `json:"created_at"`
}

type FolderRecord struct {
	FolderID       string              `json:"folder_id"`
	Label          string              `json:"label"`
	LocalPath      string              `json:"local_path"`
	Mode           protocol.FolderMode `json:"mode"`
	Versioning     string              `json:"versioning"`
	KeepVersions   int                 `json:"keep_versions"`
	RescanSeconds  int                 `json:"rescan_seconds"`
	Paused         bool                `json:"paused"`
	IgnorePatterns string              `json:"ignore_patterns"`
	Peers          []string            `json:"peers"`
}

type DropTransferRecord struct {
	TransferID       string                   `json:"transfer_id"`
	Direction        string                   `json:"direction"` // "send" | "receive"
	Kind             string                   `json:"kind"`      // "files" | "text"
	PeerID           string                   `json:"peer_id"`
	PeerName         string                   `json:"peer_name"`
	Title            string                   `json:"title"`
	TextContent      string                   `json:"text_content,omitempty"`
	SourceRoot       string                   `json:"source_root,omitempty"`
	DestDir          string                   `json:"dest_dir,omitempty"`
	TotalSize        int64                    `json:"total_size"`
	TransferredBytes int64                    `json:"transferred_bytes"`
	Status           string                   `json:"status"` // "pending_accept" | "transferring" | "paused" | "completed" | "rejected" | "failed"
	Files            []protocol.DropFileEntry `json:"files"`
	CompletedBlocks  map[string][]int         `json:"completed_blocks"`
	ErrorMsg         string                   `json:"error_msg,omitempty"`
	CreatedAt        int64                    `json:"created_at"`
	UpdatedAt        int64                    `json:"updated_at"`
}

type Store struct {
	db *sql.DB
}

func Open(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dataDir, "gens-relay.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)

	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA busy_timeout=5000;",
		"PRAGMA foreign_keys=ON;",
		"PRAGMA cache_size=-16384;",
		"PRAGMA temp_store=MEMORY;",
	} {
		if _, err := db.Exec(pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("pragma %s: %w", pragma, err)
		}
	}

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS devices (
		device_id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		auto_accept_drop INTEGER NOT NULL DEFAULT 0,
		added_at INTEGER NOT NULL,
		last_seen INTEGER NOT NULL DEFAULT 0
	);

	CREATE TABLE IF NOT EXISTS folders (
		folder_id TEXT PRIMARY KEY,
		label TEXT NOT NULL,
		local_path TEXT NOT NULL,
		mode TEXT NOT NULL DEFAULT 'send_receive',
		versioning TEXT NOT NULL DEFAULT 'trashcan',
		keep_versions INTEGER NOT NULL DEFAULT 5,
		rescan_seconds INTEGER NOT NULL DEFAULT 60,
		paused INTEGER NOT NULL DEFAULT 0,
		ignore_patterns TEXT NOT NULL DEFAULT ''
	);

	CREATE TABLE IF NOT EXISTS folder_peers (
		folder_id TEXT NOT NULL,
		device_id TEXT NOT NULL,
		PRIMARY KEY (folder_id, device_id),
		FOREIGN KEY (folder_id) REFERENCES folders(folder_id) ON DELETE CASCADE,
		FOREIGN KEY (device_id) REFERENCES devices(device_id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS local_files (
		folder_id TEXT NOT NULL,
		rel_path TEXT NOT NULL,
		size INTEGER NOT NULL,
		mtime_ns INTEGER NOT NULL,
		mode_bits INTEGER NOT NULL,
		is_dir INTEGER NOT NULL,
		deleted INTEGER NOT NULL,
		version_json TEXT NOT NULL,
		blocks_json TEXT NOT NULL,
		file_hash TEXT NOT NULL,
		seq INTEGER NOT NULL DEFAULT 0,
		updated_at INTEGER NOT NULL,
		PRIMARY KEY (folder_id, rel_path),
		FOREIGN KEY (folder_id) REFERENCES folders(folder_id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS remote_files (
		folder_id TEXT NOT NULL,
		device_id TEXT NOT NULL,
		rel_path TEXT NOT NULL,
		size INTEGER NOT NULL,
		mtime_ns INTEGER NOT NULL,
		mode_bits INTEGER NOT NULL,
		is_dir INTEGER NOT NULL,
		deleted INTEGER NOT NULL,
		version_json TEXT NOT NULL,
		blocks_json TEXT NOT NULL,
		file_hash TEXT NOT NULL,
		seq INTEGER NOT NULL DEFAULT 0,
		updated_at INTEGER NOT NULL,
		PRIMARY KEY (folder_id, device_id, rel_path),
		FOREIGN KEY (folder_id) REFERENCES folders(folder_id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS peer_folder_state (
		folder_id TEXT NOT NULL,
		device_id TEXT NOT NULL,
		last_remote_seq INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (folder_id, device_id),
		FOREIGN KEY (folder_id) REFERENCES folders(folder_id) ON DELETE CASCADE,
		FOREIGN KEY (device_id) REFERENCES devices(device_id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS drop_transfers (
		transfer_id TEXT PRIMARY KEY,
		direction TEXT NOT NULL,
		kind TEXT NOT NULL DEFAULT 'files',
		peer_id TEXT NOT NULL,
		peer_name TEXT NOT NULL,
		title TEXT NOT NULL,
		text_content TEXT NOT NULL DEFAULT '',
		source_root TEXT NOT NULL DEFAULT '',
		dest_dir TEXT NOT NULL DEFAULT '',
		total_size INTEGER NOT NULL,
		transferred_bytes INTEGER NOT NULL DEFAULT 0,
		status TEXT NOT NULL,
		files_json TEXT NOT NULL,
		completed_blocks_json TEXT NOT NULL DEFAULT '{}',
		error_msg TEXT NOT NULL DEFAULT '',
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL
	);

	CREATE TABLE IF NOT EXISTS drop_completed_blocks (
		transfer_id TEXT NOT NULL,
		rel_path TEXT NOT NULL,
		block_idx INTEGER NOT NULL,
		PRIMARY KEY (transfer_id, rel_path, block_idx),
		FOREIGN KEY (transfer_id) REFERENCES drop_transfers(transfer_id) ON DELETE CASCADE
	);
	CREATE TABLE IF NOT EXISTS pair_requests (
		device_id TEXT PRIMARY KEY,
		direction TEXT NOT NULL,
		name TEXT NOT NULL,
		message TEXT NOT NULL DEFAULT '',
		endpoint TEXT NOT NULL DEFAULT '',
		lan_addrs_json TEXT NOT NULL DEFAULT '[]',
		observed_addr TEXT NOT NULL DEFAULT '',
		created_at INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_local_files_hash ON local_files(folder_id, file_hash);
	`
	if _, err := s.db.Exec(schema); err != nil {
		return err
	}

	// Safe additive migrations for existing databases
	for _, col := range []struct {
		table string
		name  string
		def   string
	}{
		{"local_files", "seq", "INTEGER NOT NULL DEFAULT 0"},
		{"remote_files", "seq", "INTEGER NOT NULL DEFAULT 0"},
		{"drop_transfers", "kind", "TEXT NOT NULL DEFAULT 'files'"},
		{"drop_transfers", "text_content", "TEXT NOT NULL DEFAULT ''"},
	} {
		if err := s.ensureColumn(col.table, col.name, col.def); err != nil {
			return err
		}
	}
	_, err := s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_local_files_seq ON local_files(folder_id, seq);`)
	return err
}

func (s *Store) ensureColumn(table, column, colDef string) error {
	rows, err := s.db.Query(fmt.Sprintf("PRAGMA table_info(%s);", table))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = s.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", table, column, colDef))
	return err
}

// --- Devices ---

func (s *Store) UpsertDevice(d DeviceRecord) error {
	d.DeviceID = protocol.NormalizeDeviceID(d.DeviceID)
	if d.AddedAt == 0 {
		d.AddedAt = time.Now().Unix()
	}
	_, err := s.db.Exec(`
		INSERT INTO devices (device_id, name, auto_accept_drop, added_at, last_seen)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(device_id) DO UPDATE SET
			name = excluded.name,
			auto_accept_drop = excluded.auto_accept_drop,
			last_seen = CASE WHEN excluded.last_seen > devices.last_seen THEN excluded.last_seen ELSE devices.last_seen END
	`, d.DeviceID, d.Name, boolToInt(d.AutoAcceptDrop), d.AddedAt, d.LastSeen)
	return err
}

func (s *Store) TouchDeviceSeen(deviceID string) error {
	_, err := s.db.Exec(`UPDATE devices SET last_seen = ? WHERE device_id = ?`, time.Now().Unix(), protocol.NormalizeDeviceID(deviceID))
	return err
}

func (s *Store) DeleteDevice(deviceID string) error {
	_, err := s.db.Exec(`DELETE FROM devices WHERE device_id = ?`, protocol.NormalizeDeviceID(deviceID))
	return err
}

func (s *Store) IsDeviceAllowed(deviceID string) bool {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(1) FROM devices WHERE device_id = ?`, protocol.NormalizeDeviceID(deviceID)).Scan(&count)
	return err == nil && count > 0
}

func (s *Store) GetDevice(deviceID string) (*DeviceRecord, error) {
	var d DeviceRecord
	var autoInt int
	err := s.db.QueryRow(`SELECT device_id, name, auto_accept_drop, added_at, last_seen FROM devices WHERE device_id = ?`, protocol.NormalizeDeviceID(deviceID)).
		Scan(&d.DeviceID, &d.Name, &autoInt, &d.AddedAt, &d.LastSeen)
	if err != nil {
		return nil, err
	}
	d.AutoAcceptDrop = autoInt != 0
	return &d, nil
}

func (s *Store) ListDevices() ([]DeviceRecord, error) {
	rows, err := s.db.Query(`SELECT device_id, name, auto_accept_drop, added_at, last_seen FROM devices ORDER BY name ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DeviceRecord
	for rows.Next() {
		var d DeviceRecord
		var autoInt int
		if err := rows.Scan(&d.DeviceID, &d.Name, &autoInt, &d.AddedAt, &d.LastSeen); err != nil {
			return nil, err
		}
		d.AutoAcceptDrop = autoInt != 0
		out = append(out, d)
	}
	return out, rows.Err()
}

// --- Folders ---

func (s *Store) UpsertFolder(f FolderRecord) error {
	if f.Mode == "" {
		f.Mode = protocol.ModeSendReceive
	}
	if f.Versioning == "" {
		f.Versioning = "trashcan"
	}
	if f.KeepVersions <= 0 {
		f.KeepVersions = 5
	}
	if f.RescanSeconds <= 0 {
		f.RescanSeconds = 60
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.Exec(`
		INSERT INTO folders (folder_id, label, local_path, mode, versioning, keep_versions, rescan_seconds, paused, ignore_patterns)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(folder_id) DO UPDATE SET
			label = excluded.label,
			local_path = excluded.local_path,
			mode = excluded.mode,
			versioning = excluded.versioning,
			keep_versions = excluded.keep_versions,
			rescan_seconds = excluded.rescan_seconds,
			paused = excluded.paused,
			ignore_patterns = excluded.ignore_patterns
	`, f.FolderID, f.Label, f.LocalPath, string(f.Mode), f.Versioning, f.KeepVersions, f.RescanSeconds, boolToInt(f.Paused), f.IgnorePatterns)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(`DELETE FROM folder_peers WHERE folder_id = ?`, f.FolderID); err != nil {
		return err
	}
	for _, peerID := range f.Peers {
		pid := protocol.NormalizeDeviceID(peerID)
		if pid == "" {
			continue
		}
		if _, err := tx.Exec(`INSERT OR IGNORE INTO folder_peers (folder_id, device_id) VALUES (?, ?)`, f.FolderID, pid); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) DeleteFolder(folderID string) error {
	_, err := s.db.Exec(`DELETE FROM folders WHERE folder_id = ?`, folderID)
	return err
}

func (s *Store) GetFolder(folderID string) (*FolderRecord, error) {
	var f FolderRecord
	var modeStr string
	var pausedInt int
	err := s.db.QueryRow(`
		SELECT folder_id, label, local_path, mode, versioning, keep_versions, rescan_seconds, paused, ignore_patterns
		FROM folders WHERE folder_id = ?
	`, folderID).Scan(&f.FolderID, &f.Label, &f.LocalPath, &modeStr, &f.Versioning, &f.KeepVersions, &f.RescanSeconds, &pausedInt, &f.IgnorePatterns)
	if err != nil {
		return nil, err
	}
	f.Mode = protocol.FolderMode(modeStr)
	f.Paused = pausedInt != 0
	peers, err := s.getFolderPeers(f.FolderID)
	if err != nil {
		return nil, err
	}
	f.Peers = peers
	return &f, nil
}

func (s *Store) ListFolders() ([]FolderRecord, error) {
	rows, err := s.db.Query(`
		SELECT folder_id, label, local_path, mode, versioning, keep_versions, rescan_seconds, paused, ignore_patterns
		FROM folders ORDER BY label ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []FolderRecord
	for rows.Next() {
		var f FolderRecord
		var modeStr string
		var pausedInt int
		if err := rows.Scan(&f.FolderID, &f.Label, &f.LocalPath, &modeStr, &f.Versioning, &f.KeepVersions, &f.RescanSeconds, &pausedInt, &f.IgnorePatterns); err != nil {
			return nil, err
		}
		f.Mode = protocol.FolderMode(modeStr)
		f.Paused = pausedInt != 0
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		peers, err := s.getFolderPeers(out[i].FolderID)
		if err != nil {
			return nil, err
		}
		out[i].Peers = peers
	}
	return out, nil
}

func (s *Store) getFolderPeers(folderID string) ([]string, error) {
	rows, err := s.db.Query(`SELECT device_id FROM folder_peers WHERE folder_id = ? ORDER BY device_id ASC`, folderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var peers []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		peers = append(peers, id)
	}
	return peers, rows.Err()
}

// --- File Index (Local & Remote) ---

func (s *Store) GetLocalIndex(folderID string) (map[string]protocol.FileMetadata, error) {
	rows, err := s.db.Query(`
		SELECT rel_path, size, mtime_ns, mode_bits, is_dir, deleted, version_json, blocks_json, file_hash, seq
		FROM local_files WHERE folder_id = ?
	`, folderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanFileMap(rows)
}

// GetLocalScanIndex loads the local folder index without parsing blocks_json, dramatically reducing RAM/CPU during scans.
func (s *Store) GetLocalScanIndex(folderID string) (map[string]protocol.FileMetadata, error) {
	rows, err := s.db.Query(`
		SELECT rel_path, size, mtime_ns, mode_bits, is_dir, deleted, version_json, file_hash, seq
		FROM local_files WHERE folder_id = ?
	`, folderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]protocol.FileMetadata)
	for rows.Next() {
		var f protocol.FileMetadata
		var isDirInt, deletedInt int
		var vJSON string
		if err := rows.Scan(&f.RelPath, &f.Size, &f.MtimeNs, &f.ModeBits, &isDirInt, &deletedInt, &vJSON, &f.FileHash, &f.Seq); err != nil {
			return nil, err
		}
		f.IsDir = isDirInt != 0
		f.Deleted = deletedInt != 0
		_ = json.Unmarshal([]byte(vJSON), &f.Version)
		out[f.RelPath] = f
	}
	return out, rows.Err()
}

func (s *Store) GetLocalFileBlocks(folderID, relPath string) ([]protocol.BlockInfo, error) {
	var bJSON string
	err := s.db.QueryRow(`SELECT blocks_json FROM local_files WHERE folder_id = ? AND rel_path = ?`, folderID, relPath).Scan(&bJSON)
	if err != nil {
		return nil, err
	}
	var blocks []protocol.BlockInfo
	_ = json.Unmarshal([]byte(bJSON), &blocks)
	return blocks, nil
}

func (s *Store) DeleteLocalFiles(folderID string, relPaths []string) error {
	if len(relPaths) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.Prepare(`DELETE FROM local_files WHERE folder_id = ? AND rel_path = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, rel := range relPaths {
		if _, err := stmt.Exec(folderID, rel); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) GetLocalIndexSummary(folderID string) (maxSeq int64, count int, err error) {
	err = s.db.QueryRow(`SELECT COALESCE(MAX(seq), 0), COUNT(1) FROM local_files WHERE folder_id = ?`, folderID).Scan(&maxSeq, &count)
	return maxSeq, count, err
}

func (s *Store) GetLocalIndexSince(folderID string, sinceSeq int64) ([]protocol.FileMetadata, error) {
	rows, err := s.db.Query(`
		SELECT rel_path, size, mtime_ns, mode_bits, is_dir, deleted, version_json, blocks_json, file_hash, seq
		FROM local_files WHERE folder_id = ? AND seq > ? ORDER BY seq ASC
	`, folderID, sinceSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanFileSlice(rows)
}

func (s *Store) GetPeerRemoteMaxSeq(folderID, deviceID string) (int64, error) {
	var seq int64
	err := s.db.QueryRow(`SELECT COALESCE(MAX(seq), 0) FROM remote_files WHERE folder_id = ? AND device_id = ?`,
		folderID, protocol.NormalizeDeviceID(deviceID)).Scan(&seq)
	return seq, err
}

func (s *Store) UpsertLocalFiles(folderID string, files []protocol.FileMetadata) error {
	if len(files) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var maxSeq int64
	if err := tx.QueryRow(`SELECT COALESCE(MAX(seq), 0) FROM local_files WHERE folder_id = ?`, folderID).Scan(&maxSeq); err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
		INSERT INTO local_files (folder_id, rel_path, size, mtime_ns, mode_bits, is_dir, deleted, version_json, blocks_json, file_hash, seq, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(folder_id, rel_path) DO UPDATE SET
			size = excluded.size,
			mtime_ns = excluded.mtime_ns,
			mode_bits = excluded.mode_bits,
			is_dir = excluded.is_dir,
			deleted = excluded.deleted,
			version_json = excluded.version_json,
			blocks_json = excluded.blocks_json,
			file_hash = excluded.file_hash,
			seq = excluded.seq,
			updated_at = excluded.updated_at
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now().Unix()
	for i := range files {
		maxSeq++
		files[i].Seq = maxSeq
		f := files[i]
		vJSON, _ := json.Marshal(f.Version)
		bJSON, _ := json.Marshal(f.Blocks)
		if _, err := stmt.Exec(folderID, f.RelPath, f.Size, f.MtimeNs, f.ModeBits, boolToInt(f.IsDir), boolToInt(f.Deleted), string(vJSON), string(bJSON), f.FileHash, f.Seq, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) GetRemoteIndex(folderID, deviceID string) (map[string]protocol.FileMetadata, error) {
	rows, err := s.db.Query(`
		SELECT rel_path, size, mtime_ns, mode_bits, is_dir, deleted, version_json, blocks_json, file_hash, seq
		FROM remote_files WHERE folder_id = ? AND device_id = ?
	`, folderID, protocol.NormalizeDeviceID(deviceID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanFileMap(rows)
}

func (s *Store) UpsertRemoteFiles(folderID, deviceID string, full bool, files []protocol.FileMetadata) error {
	deviceID = protocol.NormalizeDeviceID(deviceID)
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if full {
		if _, err := tx.Exec(`DELETE FROM remote_files WHERE folder_id = ? AND device_id = ?`, folderID, deviceID); err != nil {
			return err
		}
	}

	stmt, err := tx.Prepare(`
		INSERT INTO remote_files (folder_id, device_id, rel_path, size, mtime_ns, mode_bits, is_dir, deleted, version_json, blocks_json, file_hash, seq, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(folder_id, device_id, rel_path) DO UPDATE SET
			size = excluded.size,
			mtime_ns = excluded.mtime_ns,
			mode_bits = excluded.mode_bits,
			is_dir = excluded.is_dir,
			deleted = excluded.deleted,
			version_json = excluded.version_json,
			blocks_json = excluded.blocks_json,
			file_hash = excluded.file_hash,
			seq = excluded.seq,
			updated_at = excluded.updated_at
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now().Unix()
	for _, f := range files {
		vJSON, _ := json.Marshal(f.Version)
		bJSON, _ := json.Marshal(f.Blocks)
		if _, err := stmt.Exec(folderID, deviceID, f.RelPath, f.Size, f.MtimeNs, f.ModeBits, boolToInt(f.IsDir), boolToInt(f.Deleted), string(vJSON), string(bJSON), f.FileHash, f.Seq, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func scanFileMap(rows *sql.Rows) (map[string]protocol.FileMetadata, error) {
	out := make(map[string]protocol.FileMetadata)
	for rows.Next() {
		var f protocol.FileMetadata
		var isDirInt, deletedInt int
		var vJSON, bJSON string
		if err := rows.Scan(&f.RelPath, &f.Size, &f.MtimeNs, &f.ModeBits, &isDirInt, &deletedInt, &vJSON, &bJSON, &f.FileHash, &f.Seq); err != nil {
			return nil, err
		}
		f.IsDir = isDirInt != 0
		f.Deleted = deletedInt != 0
		_ = json.Unmarshal([]byte(vJSON), &f.Version)
		_ = json.Unmarshal([]byte(bJSON), &f.Blocks)
		out[f.RelPath] = f
	}
	return out, rows.Err()
}

func scanFileSlice(rows *sql.Rows) ([]protocol.FileMetadata, error) {
	var out []protocol.FileMetadata
	for rows.Next() {
		var f protocol.FileMetadata
		var isDirInt, deletedInt int
		var vJSON, bJSON string
		if err := rows.Scan(&f.RelPath, &f.Size, &f.MtimeNs, &f.ModeBits, &isDirInt, &deletedInt, &vJSON, &bJSON, &f.FileHash, &f.Seq); err != nil {
			return nil, err
		}
		f.IsDir = isDirInt != 0
		f.Deleted = deletedInt != 0
		_ = json.Unmarshal([]byte(vJSON), &f.Version)
		_ = json.Unmarshal([]byte(bJSON), &f.Blocks)
		out = append(out, f)
	}
	return out, rows.Err()
}

// --- Drop Transfers ---

func (s *Store) UpsertDropTransfer(t DropTransferRecord) error {
	now := time.Now().Unix()
	if t.CreatedAt == 0 {
		t.CreatedAt = now
	}
	if t.Kind == "" {
		t.Kind = "files"
	}
	t.UpdatedAt = now
	filesJSON, _ := json.Marshal(t.Files)
	if t.CompletedBlocks == nil {
		t.CompletedBlocks = map[string][]int{}
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.Exec(`
		INSERT INTO drop_transfers (transfer_id, direction, kind, peer_id, peer_name, title, text_content, source_root, dest_dir, total_size, transferred_bytes, status, files_json, completed_blocks_json, error_msg, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, ?, ?)
		ON CONFLICT(transfer_id) DO UPDATE SET
			dest_dir = excluded.dest_dir,
			transferred_bytes = excluded.transferred_bytes,
			status = excluded.status,
			error_msg = excluded.error_msg,
			updated_at = excluded.updated_at
	`, t.TransferID, t.Direction, t.Kind, protocol.NormalizeDeviceID(t.PeerID), t.PeerName, t.Title, t.TextContent, t.SourceRoot, t.DestDir, t.TotalSize, t.TransferredBytes, t.Status, string(filesJSON), t.ErrorMsg, t.CreatedAt, t.UpdatedAt)
	if err != nil {
		return err
	}
	if len(t.CompletedBlocks) > 0 {
		stmt, err := tx.Prepare(`INSERT OR IGNORE INTO drop_completed_blocks (transfer_id, rel_path, block_idx) VALUES (?, ?, ?)`)
		if err != nil {
			return err
		}
		for rel, idxs := range t.CompletedBlocks {
			for _, idx := range idxs {
				if _, err := stmt.Exec(t.TransferID, rel, idx); err != nil {
					_ = stmt.Close()
					return err
				}
			}
		}
		_ = stmt.Close()
	}
	return tx.Commit()
}

func (s *Store) GetDropTransfer(transferID string) (*DropTransferRecord, error) {
	var t DropTransferRecord
	var filesJSON, blocksJSON string
	err := s.db.QueryRow(`
		SELECT transfer_id, direction, kind, peer_id, peer_name, title, text_content, source_root, dest_dir, total_size, transferred_bytes, status, files_json, completed_blocks_json, error_msg, created_at, updated_at
		FROM drop_transfers WHERE transfer_id = ?
	`, transferID).Scan(&t.TransferID, &t.Direction, &t.Kind, &t.PeerID, &t.PeerName, &t.Title, &t.TextContent, &t.SourceRoot, &t.DestDir, &t.TotalSize, &t.TransferredBytes, &t.Status, &filesJSON, &blocksJSON, &t.ErrorMsg, &t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(filesJSON), &t.Files)
	blocks, err := s.GetDropCompletedBlocks(transferID)
	if err == nil && len(blocks) > 0 {
		t.CompletedBlocks = blocks
	} else {
		_ = json.Unmarshal([]byte(blocksJSON), &t.CompletedBlocks)
	}
	if t.CompletedBlocks == nil {
		t.CompletedBlocks = map[string][]int{}
	}
	return &t, nil
}

func (s *Store) GetDropCompletedBlocks(transferID string) (map[string][]int, error) {
	rows, err := s.db.Query(`SELECT rel_path, block_idx FROM drop_completed_blocks WHERE transfer_id = ?`, transferID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string][]int)
	for rows.Next() {
		var rel string
		var idx int
		if err := rows.Scan(&rel, &idx); err != nil {
			return nil, err
		}
		out[rel] = append(out[rel], idx)
	}
	return out, rows.Err()
}

type DropBlockCompletion struct {
	RelPath  string
	BlockIdx int
	Size     int64
}

// MarkDropBlocksCompleted persists a batch of completed block indices in O(batch) without rewriting JSON.
func (s *Store) MarkDropBlocksCompleted(transferID string, items []DropBlockCompletion) (int64, error) {
	if len(items) == 0 {
		var cur int64
		_ = s.db.QueryRow(`SELECT transferred_bytes FROM drop_transfers WHERE transfer_id = ?`, transferID).Scan(&cur)
		return cur, nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(`INSERT OR IGNORE INTO drop_completed_blocks (transfer_id, rel_path, block_idx) VALUES (?, ?, ?)`)
	if err != nil {
		return 0, err
	}
	var addedBytes int64
	for _, it := range items {
		res, err := stmt.Exec(transferID, it.RelPath, it.BlockIdx)
		if err != nil {
			_ = stmt.Close()
			return 0, err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			addedBytes += it.Size
		}
	}
	_ = stmt.Close()

	now := time.Now().Unix()
	if addedBytes > 0 {
		if _, err := tx.Exec(`UPDATE drop_transfers SET transferred_bytes = MIN(total_size, transferred_bytes + ?), updated_at = ? WHERE transfer_id = ?`,
			addedBytes, now, transferID); err != nil {
			return 0, err
		}
	}
	var total int64
	_ = tx.QueryRow(`SELECT transferred_bytes FROM drop_transfers WHERE transfer_id = ?`, transferID).Scan(&total)
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return total, nil
}

func (s *Store) ListDropTransfers() ([]DropTransferRecord, error) {
	rows, err := s.db.Query(`
		SELECT transfer_id, direction, kind, peer_id, peer_name, title, text_content, source_root, dest_dir, total_size, transferred_bytes, status, files_json, completed_blocks_json, error_msg, created_at, updated_at
		FROM drop_transfers ORDER BY created_at DESC LIMIT 100
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DropTransferRecord
	for rows.Next() {
		var t DropTransferRecord
		var filesJSON, blocksJSON string
		if err := rows.Scan(&t.TransferID, &t.Direction, &t.Kind, &t.PeerID, &t.PeerName, &t.Title, &t.TextContent, &t.SourceRoot, &t.DestDir, &t.TotalSize, &t.TransferredBytes, &t.Status, &filesJSON, &blocksJSON, &t.ErrorMsg, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(filesJSON), &t.Files)
		_ = json.Unmarshal([]byte(blocksJSON), &t.CompletedBlocks)
		if t.CompletedBlocks == nil {
			t.CompletedBlocks = map[string][]int{}
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ListDropTransfersSummary returns lightweight transfer records without files_json or completed_blocks_json.
func (s *Store) ListDropTransfersSummary() ([]DropTransferRecord, error) {
	rows, err := s.db.Query(`
		SELECT transfer_id, direction, kind, peer_id, peer_name, title, text_content, dest_dir, total_size, transferred_bytes, status, error_msg, created_at, updated_at
		FROM drop_transfers ORDER BY created_at DESC LIMIT 100
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DropTransferRecord
	for rows.Next() {
		var t DropTransferRecord
		if err := rows.Scan(&t.TransferID, &t.Direction, &t.Kind, &t.PeerID, &t.PeerName, &t.Title, &t.TextContent, &t.DestDir, &t.TotalSize, &t.TransferredBytes, &t.Status, &t.ErrorMsg, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) ListActiveDropTransferIDs() (map[string]struct{}, error) {
	rows, err := s.db.Query(`SELECT transfer_id FROM drop_transfers WHERE status IN ('pending_accept', 'transferring', 'paused')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]struct{})
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = struct{}{}
	}
	return out, rows.Err()
}

func (s *Store) ListResumableSendDropIDs(peerID string) ([]string, error) {
	rows, err := s.db.Query(`
		SELECT transfer_id FROM drop_transfers
		WHERE direction = 'send' AND peer_id = ? AND status = 'transferring'
		ORDER BY created_at ASC
	`, protocol.NormalizeDeviceID(peerID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) UpdateDropStatusOnly(transferID, status, errorMsg string) error {
	_, err := s.db.Exec(`UPDATE drop_transfers SET status = ?, error_msg = ?, updated_at = ? WHERE transfer_id = ?`,
		status, errorMsg, time.Now().Unix(), transferID)
	return err
}

// UpdateDropProgress updates only progress and status fields without rewriting files_json.
func (s *Store) UpdateDropProgress(transferID, status, errorMsg string, transferredBytes int64, completedBlocks map[string][]int) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`
		UPDATE drop_transfers
		SET status = ?, error_msg = ?, transferred_bytes = ?, updated_at = ?
		WHERE transfer_id = ?
	`, status, errorMsg, transferredBytes, time.Now().Unix(), transferID); err != nil {
		return err
	}
	if len(completedBlocks) > 0 {
		stmt, err := tx.Prepare(`INSERT OR IGNORE INTO drop_completed_blocks (transfer_id, rel_path, block_idx) VALUES (?, ?, ?)`)
		if err != nil {
			return err
		}
		for rel, idxs := range completedBlocks {
			for _, idx := range idxs {
				if _, err := stmt.Exec(transferID, rel, idx); err != nil {
					_ = stmt.Close()
					return err
				}
			}
		}
		_ = stmt.Close()
	}
	return tx.Commit()
}

// UpdateDropBlocksOnly persists completed blocks and byte count without altering status.
func (s *Store) UpdateDropBlocksOnly(transferID string, transferredBytes int64, completedBlocks map[string][]int) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`
		UPDATE drop_transfers
		SET transferred_bytes = ?, updated_at = ?
		WHERE transfer_id = ?
	`, transferredBytes, time.Now().Unix(), transferID); err != nil {
		return err
	}
	if len(completedBlocks) > 0 {
		stmt, err := tx.Prepare(`INSERT OR IGNORE INTO drop_completed_blocks (transfer_id, rel_path, block_idx) VALUES (?, ?, ?)`)
		if err != nil {
			return err
		}
		for rel, idxs := range completedBlocks {
			for _, idx := range idxs {
				if _, err := stmt.Exec(transferID, rel, idx); err != nil {
					_ = stmt.Close()
					return err
				}
			}
		}
		_ = stmt.Close()
	}
	return tx.Commit()
}

func (s *Store) DeleteDropTransfer(transferID string) error {
	_, err := s.db.Exec(`DELETE FROM drop_transfers WHERE transfer_id = ?`, transferID)
	return err
}

func (s *Store) GetDropTransferStatus(transferID string) (string, error) {
	var status string
	err := s.db.QueryRow(`SELECT status FROM drop_transfers WHERE transfer_id = ?`, transferID).Scan(&status)
	return status, err
}

func (s *Store) PruneOldDropTransfers(olderThan time.Duration) error {
	cutoff := time.Now().Add(-olderThan).Unix()
	_, err := s.db.Exec(`
		DELETE FROM drop_transfers
		WHERE status IN ('completed', 'rejected', 'canceled', 'failed')
		  AND updated_at < ?
		  AND transfer_id NOT IN (
			SELECT transfer_id FROM drop_transfers ORDER BY created_at DESC LIMIT 200
		  )
	`, cutoff)
	return err
}

// --- Pair Requests ---

func (s *Store) UpsertPairRequest(r PairRequestRecord) error {
	r.DeviceID = protocol.NormalizeDeviceID(r.DeviceID)
	if r.CreatedAt == 0 {
		r.CreatedAt = time.Now().Unix()
	}
	lanJSON, _ := json.Marshal(r.LANAddrs)
	_, err := s.db.Exec(`
		INSERT INTO pair_requests (device_id, direction, name, message, endpoint, lan_addrs_json, observed_addr, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(device_id) DO UPDATE SET
			direction = excluded.direction,
			name = CASE WHEN excluded.name != '' THEN excluded.name ELSE pair_requests.name END,
			message = CASE WHEN excluded.message != '' THEN excluded.message ELSE pair_requests.message END,
			endpoint = CASE WHEN excluded.endpoint != '' THEN excluded.endpoint ELSE pair_requests.endpoint END,
			lan_addrs_json = excluded.lan_addrs_json,
			observed_addr = CASE WHEN excluded.observed_addr != '' THEN excluded.observed_addr ELSE pair_requests.observed_addr END,
			created_at = excluded.created_at
	`, r.DeviceID, r.Direction, r.Name, r.Message, r.Endpoint, string(lanJSON), r.ObservedAddr, r.CreatedAt)
	return err
}

func (s *Store) GetPairRequest(deviceID string) (*PairRequestRecord, error) {
	var r PairRequestRecord
	var lanJSON string
	err := s.db.QueryRow(`
		SELECT device_id, direction, name, message, endpoint, lan_addrs_json, observed_addr, created_at
		FROM pair_requests WHERE device_id = ?
	`, protocol.NormalizeDeviceID(deviceID)).Scan(&r.DeviceID, &r.Direction, &r.Name, &r.Message, &r.Endpoint, &lanJSON, &r.ObservedAddr, &r.CreatedAt)
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(lanJSON), &r.LANAddrs)
	return &r, nil
}

func (s *Store) DeletePairRequest(deviceID string) error {
	_, err := s.db.Exec(`DELETE FROM pair_requests WHERE device_id = ?`, protocol.NormalizeDeviceID(deviceID))
	return err
}

func (s *Store) ListPairRequests() ([]PairRequestRecord, error) {
	rows, err := s.db.Query(`
		SELECT device_id, direction, name, message, endpoint, lan_addrs_json, observed_addr, created_at
		FROM pair_requests ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PairRequestRecord
	for rows.Next() {
		var r PairRequestRecord
		var lanJSON string
		if err := rows.Scan(&r.DeviceID, &r.Direction, &r.Name, &r.Message, &r.Endpoint, &lanJSON, &r.ObservedAddr, &r.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(lanJSON), &r.LANAddrs)
		out = append(out, r)
	}
	return out, rows.Err()
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
