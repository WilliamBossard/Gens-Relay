package protocol

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// StreamKind tags the first byte of each QUIC stream so control and bulk data streams never block each other.
type StreamKind byte

const (
	StreamControl StreamKind = 1
	StreamData    StreamKind = 2
)

// FolderMode defines synchronization direction on a node.
type FolderMode string

const (
	ModeSendReceive FolderMode = "send_receive"
	ModeSendOnly    FolderMode = "send_only"
	ModeReceiveOnly FolderMode = "receive_only"
)

// MaxFrameSize caps a single JSON/RPC frame at 16 MiB to guard memory.
const MaxFrameSize = 16 * 1024 * 1024

// frameCompressedFlag is set on the high bit of the 4-byte frame length when payload is DEFLATE-compressed.
const frameCompressedFlag uint32 = 0x80000000

// ControlEnvelope is sent on StreamControl.
type ControlEnvelope struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

const (
	CtrlHello              = "hello"
	CtrlFolderIndex        = "folder_index"
	CtrlFolderSummary      = "folder_summary"
	CtrlFolderInvite       = "folder_invite"
	CtrlFolderInviteReject = "folder_invite_reject"
	CtrlDropOffer          = "drop_offer"
	CtrlDropResponse       = "drop_response"
	CtrlDropPause          = "drop_pause"
	CtrlDropResume         = "drop_resume"
	CtrlDropCancel         = "drop_cancel"
	CtrlDropComplete       = "drop_complete"
	CtrlPing               = "ping"
	CtrlPong               = "pong"
)

type HelloMsg struct {
	DeviceID string   `json:"device_id"`
	Name     string   `json:"name"`
	Folders  []string `json:"folders"`
}

type VectorClock map[string]uint64

type BlockInfo struct {
	Offset int64  `json:"offset"`
	Size   int    `json:"size"`
	Hash   string `json:"hash"` // hex BLAKE3
}

type FileMetadata struct {
	RelPath   string      `json:"rel_path"`
	Size      int64       `json:"size"`
	MtimeNs   int64       `json:"mtime_ns"`
	ModeBits  uint32      `json:"mode_bits"`
	IsDir     bool        `json:"is_dir"`
	Deleted   bool        `json:"deleted"`
	Version   VectorClock `json:"version"`
	BlockSize int         `json:"block_size,omitempty"`
	Blocks    []BlockInfo `json:"blocks,omitempty"`
	FileHash  string      `json:"file_hash,omitempty"`
	Seq       int64       `json:"seq,omitempty"`
}

type FolderIndexMsg struct {
	FolderID     string         `json:"folder_id"`
	Full         bool           `json:"full"`
	MaxSeq       int64          `json:"max_seq,omitempty"`
	IndexEpoch   uint64         `json:"index_epoch,omitempty"`
	BatchID      string         `json:"batch_id,omitempty"`
	BatchIndex   int            `json:"batch_index,omitempty"`
	TotalBatches int            `json:"total_batches,omitempty"`
	Files        []FileMetadata `json:"files"`
}

// FolderSummaryMsg is exchanged on reconnect so peers only send deltas when epochs & counts match.
type FolderSummaryMsg struct {
	FolderID         string `json:"folder_id"`
	KnownMaxSeq      int64  `json:"known_max_seq"`
	KnownRemoteCount int    `json:"known_remote_count"`
	KnownRemoteEpoch uint64 `json:"known_remote_epoch"`
	LocalMaxSeq      int64  `json:"local_max_seq"`
	LocalCount       int    `json:"local_count"`
	LocalIndexEpoch  uint64 `json:"local_index_epoch"`
}

type FolderInviteMsg struct {
	FolderID      string `json:"folder_id"`
	Label         string `json:"label"`
	SenderName    string `json:"sender_name"`
	SuggestedMode string `json:"suggested_mode"`
	FileCount     int    `json:"file_count"`
	TotalBytes    int64  `json:"total_bytes"`
}

type FolderInviteRejectMsg struct {
	FolderID string `json:"folder_id"`
}

// DataRequestKind distinguishes block downloads from Drop pushes on StreamData.
type DataRequestKind byte

const (
	DataReqSyncBlock      DataRequestKind = 1
	DataReqDropChunk      DataRequestKind = 2
	DataReqSyncMultiBlock DataRequestKind = 3
)

type SyncBlockRequest struct {
	FolderID   string `json:"folder_id"`
	RelPath    string `json:"rel_path"`
	Offset     int64  `json:"offset"`
	Size       int    `json:"size"`
	Hash       string `json:"hash"`
	AcceptZstd bool   `json:"accept_zstd,omitempty"`
}

type SyncMultiBlockRequest struct {
	FolderID   string             `json:"folder_id"`
	RelPath    string             `json:"rel_path"`
	AcceptZstd bool               `json:"accept_zstd,omitempty"`
	Blocks     []SyncBlockRequest `json:"blocks"`
}

type SyncBlockResponse struct {
	OK         bool   `json:"ok"`
	Error      string `json:"error,omitempty"`
	Compressed bool   `json:"compressed,omitempty"`
	WireSize   int    `json:"wire_size,omitempty"`
}

type DropFileEntry struct {
	RelPath   string      `json:"rel_path"`
	Size      int64       `json:"size"`
	IsDir     bool        `json:"is_dir"`
	BlockSize int         `json:"block_size,omitempty"`
	Blocks    []BlockInfo `json:"blocks,omitempty"`
	FileHash  string      `json:"file_hash,omitempty"`
}

type DropOfferMsg struct {
	TransferID  string          `json:"transfer_id"`
	SenderName  string          `json:"sender_name"`
	Kind        string          `json:"kind,omitempty"` // "files" (default) | "text"
	Title       string          `json:"title"`
	TextContent string          `json:"text_content,omitempty"`
	MimeType    string          `json:"mime_type,omitempty"`
	TotalSize   int64           `json:"total_size"`
	FileCount   int             `json:"file_count"`
	Files       []DropFileEntry `json:"files"`
}

type DropResponseMsg struct {
	TransferID string `json:"transfer_id"`
	Accepted   bool   `json:"accepted"`
	Reason     string `json:"reason,omitempty"`
}

type DropChunkHeader struct {
	TransferID string `json:"transfer_id"`
	RelPath    string `json:"rel_path"`
	BlockIdx   int    `json:"block_idx"`
	Offset     int64  `json:"offset"`
	Size       int    `json:"size"`
	Hash       string `json:"hash"`
	Compressed bool   `json:"compressed,omitempty"`
	WireSize   int    `json:"wire_size,omitempty"`
}

type DropChunkAck struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type DropCompleteMsg struct {
	TransferID string `json:"transfer_id"`
}

type DropCancelMsg struct {
	TransferID string `json:"transfer_id"`
	Reason     string `json:"reason,omitempty"`
}

type DropPauseMsg struct {
	TransferID string `json:"transfer_id"`
	Reason     string `json:"reason,omitempty"`
}

var flateWriterPool = sync.Pool{
	New: func() any {
		fw, _ := flate.NewWriter(io.Discard, flate.BestSpeed)
		return fw
	},
}

var frameBufPool = sync.Pool{
	New: func() any {
		b := new(bytes.Buffer)
		b.Grow(4096)
		return b
	},
}

// WriteFrame writes a length-prefixed JSON frame (DEFLATE-compressed when > 1 KiB) in a single Write call.
func WriteFrame(w io.Writer, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(data) > MaxFrameSize {
		return fmt.Errorf("frame too large: %d", len(data))
	}

	outBuf := frameBufPool.Get().(*bytes.Buffer)
	outBuf.Reset()
	defer func() {
		if outBuf.Cap() <= 4*1024*1024 {
			frameBufPool.Put(outBuf)
		}
	}()

	headerLen := uint32(len(data))
	payload := data
	var compBuf *bytes.Buffer
	if len(data) > 1024 {
		compBuf = frameBufPool.Get().(*bytes.Buffer)
		compBuf.Reset()
		fw := flateWriterPool.Get().(*flate.Writer)
		fw.Reset(compBuf)
		_, wErr := fw.Write(data)
		cErr := fw.Close()
		fw.Reset(io.Discard)
		flateWriterPool.Put(fw)
		if wErr == nil && cErr == nil && compBuf.Len() < len(data) {
			payload = compBuf.Bytes()
			headerLen = uint32(len(payload)) | frameCompressedFlag
		}
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], headerLen)
	outBuf.Grow(4 + len(payload))
	outBuf.Write(lenBuf[:])
	outBuf.Write(payload)
	if compBuf != nil {
		if compBuf.Cap() <= 4*1024*1024 {
			frameBufPool.Put(compBuf)
		}
	}
	_, err = w.Write(outBuf.Bytes())
	return err
}

// ReadFrame reads a length-prefixed JSON frame into v.
func ReadFrame(r io.Reader, v any) error {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return err
	}
	rawLen := binary.BigEndian.Uint32(lenBuf[:])
	compressed := (rawLen & frameCompressedFlag) != 0
	n := rawLen & ^frameCompressedFlag
	if n > MaxFrameSize {
		return fmt.Errorf("incoming frame exceeds max size: %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	if compressed {
		fr := flate.NewReader(bytes.NewReader(buf))
		defer fr.Close()
		decompressed, err := io.ReadAll(io.LimitReader(fr, MaxFrameSize+1))
		if err != nil {
			return fmt.Errorf("decompress frame: %w", err)
		}
		if len(decompressed) > MaxFrameSize {
			return fmt.Errorf("decompressed frame exceeds max size")
		}
		buf = decompressed
	}
	return json.Unmarshal(buf, v)
}

// WriteControlEnvelope wraps a typed payload into ControlEnvelope and writes it.
func WriteControlEnvelope(w io.Writer, msgType string, payload any) error {
	var raw json.RawMessage
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		raw = b
	}
	return WriteFrame(w, ControlEnvelope{Type: msgType, Payload: raw})
}
