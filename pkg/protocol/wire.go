package protocol

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
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
	CtrlHello          = "hello"
	CtrlFolderIndex    = "folder_index"
	CtrlFolderSummary  = "folder_summary"
	CtrlDropOffer      = "drop_offer"
	CtrlDropResponse   = "drop_response"
	CtrlDropPause      = "drop_pause"
	CtrlDropResume     = "drop_resume"
	CtrlDropCancel     = "drop_cancel"
	CtrlDropComplete   = "drop_complete"
	CtrlPing           = "ping"
	CtrlPong           = "pong"
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
	RelPath  string      `json:"rel_path"`
	Size     int64       `json:"size"`
	MtimeNs  int64       `json:"mtime_ns"`
	ModeBits uint32      `json:"mode_bits"`
	IsDir    bool        `json:"is_dir"`
	Deleted  bool        `json:"deleted"`
	Version  VectorClock `json:"version"`
	Blocks   []BlockInfo `json:"blocks,omitempty"`
	FileHash string      `json:"file_hash,omitempty"`
	Seq      int64       `json:"seq,omitempty"`
}

type FolderIndexMsg struct {
	FolderID     string         `json:"folder_id"`
	Full         bool           `json:"full"`
	MaxSeq       int64          `json:"max_seq,omitempty"`
	BatchID      string         `json:"batch_id,omitempty"`
	BatchIndex   int            `json:"batch_index,omitempty"`
	TotalBatches int            `json:"total_batches,omitempty"`
	Files        []FileMetadata `json:"files"`
}

// FolderSummaryMsg is exchanged on reconnect so peers only send deltas since KnownSeq.
type FolderSummaryMsg struct {
	FolderID    string `json:"folder_id"`
	KnownMaxSeq int64  `json:"known_max_seq"`
	LocalMaxSeq int64  `json:"local_max_seq"`
	FileCount   int    `json:"file_count"`
}

// DataRequestKind distinguishes block downloads from Drop pushes on StreamData.
type DataRequestKind byte

const (
	DataReqSyncBlock      DataRequestKind = 1
	DataReqDropChunk      DataRequestKind = 2
	DataReqSyncMultiBlock DataRequestKind = 3
)

type SyncBlockRequest struct {
	FolderID string `json:"folder_id"`
	RelPath  string `json:"rel_path"`
	Offset   int64  `json:"offset"`
	Size     int    `json:"size"`
	Hash     string `json:"hash"`
}

type SyncMultiBlockRequest struct {
	FolderID string             `json:"folder_id"`
	RelPath  string             `json:"rel_path"`
	Blocks   []SyncBlockRequest `json:"blocks"`
}

type SyncBlockResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type DropFileEntry struct {
	RelPath  string      `json:"rel_path"`
	Size     int64       `json:"size"`
	IsDir    bool        `json:"is_dir"`
	Blocks   []BlockInfo `json:"blocks,omitempty"`
	FileHash string      `json:"file_hash,omitempty"`
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

// WriteFrame writes a length-prefixed JSON frame (DEFLATE-compressed when > 1 KiB).
func WriteFrame(w io.Writer, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(data) > MaxFrameSize {
		return fmt.Errorf("frame too large: %d", len(data))
	}

	headerLen := uint32(len(data))
	payload := data
	if len(data) > 1024 {
		var compBuf bytes.Buffer
		fw, err := flate.NewWriter(&compBuf, flate.BestSpeed)
		if err == nil {
			if _, wErr := fw.Write(data); wErr == nil && fw.Close() == nil {
				if compBuf.Len() < len(data) {
					payload = compBuf.Bytes()
					headerLen = uint32(len(payload)) | frameCompressedFlag
				}
			}
		}
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], headerLen)
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err = w.Write(payload)
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
