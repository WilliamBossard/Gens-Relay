package store

import (
	"testing"

	"github.com/WilliamBossard/Gens-Relay/pkg/protocol"
)

func TestDropProgressAndSummaryQueries(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	rec := DropTransferRecord{
		TransferID: "tr1",
		Direction:  "send",
		PeerID:     "AAAA-BBBB",
		PeerName:   "Peer",
		Title:      "bundle",
		TotalSize:  1024,
		Status:     "transferring",
		Files: []protocol.DropFileEntry{
			{RelPath: "a.bin", Size: 1024},
		},
		CompletedBlocks: map[string][]int{},
	}
	if err := st.UpsertDropTransfer(rec); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateDropBlocksOnly("tr1", 512, map[string][]int{"a.bin": {0}}); err != nil {
		t.Fatal(err)
	}
	summaries, err := st.ListDropTransfersSummary()
	if err != nil || len(summaries) != 1 {
		t.Fatalf("unexpected summaries: %+v err=%v", summaries, err)
	}
	if summaries[0].TransferredBytes != 512 || len(summaries[0].Files) != 0 {
		t.Fatalf("expected lightweight summary with 512 bytes and omitted files, got %+v", summaries[0])
	}
	if err := st.UpdateDropProgress("tr1", "paused", "", 512, map[string][]int{"a.bin": {0}}); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetDropTransfer("tr1")
	if err != nil || got.Status != "paused" || len(got.Files) != 1 {
		t.Fatalf("unexpected full record: %+v err=%v", got, err)
	}
	if err := st.DeleteDropTransfer("tr1"); err != nil {
		t.Fatal(err)
	}
	summaries, _ = st.ListDropTransfersSummary()
	if len(summaries) != 0 {
		t.Fatalf("expected empty list after delete, got %d", len(summaries))
	}
}

func TestDropCompletedBlocksAndLocalIndexSeq(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// Test O(1) drop_completed_blocks tracking & active IDs
	rec := DropTransferRecord{
		TransferID: "tr-batch",
		Direction:  "send",
		Kind:       "files",
		PeerID:     "PEER-1",
		PeerName:   "Peer",
		Title:      "video.mkv",
		TotalSize:  2048,
		Status:     "transferring",
		Files: []protocol.DropFileEntry{
			{RelPath: "video.mkv", Size: 2048},
		},
	}
	if err := st.UpsertDropTransfer(rec); err != nil {
		t.Fatal(err)
	}
	total, err := st.MarkDropBlocksCompleted("tr-batch", []DropBlockCompletion{
		{RelPath: "video.mkv", BlockIdx: 0, Size: 1024},
		{RelPath: "video.mkv", BlockIdx: 0, Size: 1024}, // duplicate ignored
		{RelPath: "video.mkv", BlockIdx: 1, Size: 1024},
	})
	if err != nil || total != 2048 {
		t.Fatalf("expected 2048 bytes after deduplicated batch, got %d err=%v", total, err)
	}
	active, err := st.ListActiveDropTransferIDs()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := active["tr-batch"]; !ok {
		t.Fatal("expected tr-batch in active drop IDs")
	}

	// Test sequence numbers for delta folder index sync
	_ = st.UpsertFolder(FolderRecord{FolderID: "f1", Label: "F1", LocalPath: t.TempDir()})
	files := []protocol.FileMetadata{
		{RelPath: "a.txt", Size: 10},
		{RelPath: "b.txt", Size: 20},
	}
	if err := st.UpsertLocalFiles("f1", files); err != nil {
		t.Fatal(err)
	}
	maxSeq, count, err := st.GetLocalIndexSummary("f1")
	if err != nil || maxSeq != 2 || count != 2 {
		t.Fatalf("expected maxSeq=2 count=2, got %d %d err=%v", maxSeq, count, err)
	}
	since, err := st.GetLocalIndexSince("f1", 1)
	if err != nil || len(since) != 1 || since[0].RelPath != "b.txt" {
		t.Fatalf("expected only b.txt since seq 1, got %+v err=%v", since, err)
	}
}
