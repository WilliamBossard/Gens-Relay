package store

import (
	"path/filepath"
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
		StagingDir: filepath.Join(t.TempDir(), "stage1"),
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
	stages, err := st.ListActiveDropStagingDirs()
	if err != nil || len(stages) != 1 {
		t.Fatalf("expected active staging dir tracked, got %+v err=%v", stages, err)
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
	if err := st.PruneCompletedDropBlocks("tr-batch"); err != nil {
		t.Fatal(err)
	}
	blocksAfterPrune, _ := st.GetDropCompletedBlocks("tr-batch")
	if len(blocksAfterPrune) != 0 {
		t.Fatalf("expected blocks pruned, got %+v", blocksAfterPrune)
	}

	active, err := st.ListActiveDropTransferIDs()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := active["tr-batch"]; !ok {
		t.Fatal("expected tr-batch in active drop IDs")
	}

	// Test sequence numbers, epoch bump, and lazy remote scan index
	_ = st.UpsertDevice(DeviceRecord{DeviceID: "PEER-1", Name: "Peer 1"})
	if !st.IsDeviceAllowed("peer-1") {
		t.Fatal("expected IsDeviceAllowed memory cache hit")
	}

	_ = st.UpsertFolder(FolderRecord{FolderID: "f1", Label: "F1", LocalPath: t.TempDir(), Peers: []string{"PEER-1"}})
	files := []protocol.FileMetadata{
		{RelPath: "a.txt", Size: 10, BlockSize: 256 * 1024, Blocks: []protocol.BlockInfo{{Offset: 0, Size: 10, Hash: "abc"}}},
		{RelPath: "b.txt", Size: 20, BlockSize: 256 * 1024, Blocks: []protocol.BlockInfo{{Offset: 0, Size: 20, Hash: "def"}}},
	}
	if err := st.UpsertLocalFiles("f1", files); err != nil {
		t.Fatal(err)
	}
	maxSeq, count, epoch, err := st.GetLocalIndexSummaryWithEpoch("f1")
	if err != nil || maxSeq != 2 || count != 2 || epoch != 1 {
		t.Fatalf("expected maxSeq=2 count=2 epoch=1, got %d %d %d err=%v", maxSeq, count, epoch, err)
	}
	since, err := st.GetLocalIndexSince("f1", 1)
	if err != nil || len(since) != 1 || since[0].RelPath != "b.txt" {
		t.Fatalf("expected only b.txt since seq 1, got %+v err=%v", since, err)
	}
	if err := st.DeleteLocalFiles("f1", []string{"a.txt"}); err != nil {
		t.Fatal(err)
	}
	_, count2, epoch2, _ := st.GetLocalIndexSummaryWithEpoch("f1")
	if count2 != 1 || epoch2 != 2 {
		t.Fatalf("expected epoch bumped to 2 and count=1 after DeleteLocalFiles, got count=%d epoch=%d", count2, epoch2)
	}

	// Remote scan index omits blocks_json while GetRemoteFileBlocks loads lazily
	if err := st.UpsertRemoteFilesWithEpoch("f1", "PEER-1", true, 3, files); err != nil {
		t.Fatal(err)
	}
	rScan, err := st.GetRemoteScanIndex("f1", "PEER-1")
	if err != nil || len(rScan) != 2 || len(rScan["a.txt"].Blocks) != 0 {
		t.Fatalf("expected GetRemoteScanIndex without blocks loaded, got %+v err=%v", rScan, err)
	}
	rBlocks, bs, err := st.GetRemoteFileBlocks("f1", "PEER-1", "a.txt")
	if err != nil || len(rBlocks) != 1 || bs != 256*1024 {
		t.Fatalf("expected GetRemoteFileBlocks to load block list, got len=%d bs=%d err=%v", len(rBlocks), bs, err)
	}
	rMax, rCnt, rEpoch, err := st.GetPeerRemoteSummary("f1", "PEER-1")
	if err != nil || rMax != 2 || rCnt != 2 || rEpoch != 3 {
		t.Fatalf("unexpected GetPeerRemoteSummary: %d %d %d err=%v", rMax, rCnt, rEpoch, err)
	}

	// Folder invitations CRUD
	_ = st.UpsertFolderInvitation(FolderInvitationRecord{
		FolderID: "f-inv", SenderID: "PEER-1", SenderName: "Peer 1", Label: "Photos", SuggestedMode: "send_receive", FileCount: 5, TotalBytes: 1000,
	})
	invs, err := st.ListFolderInvitations()
	if err != nil || len(invs) != 1 || invs[0].Label != "Photos" {
		t.Fatalf("expected 1 invitation, got %+v err=%v", invs, err)
	}

	// DeleteDevice must cascade remote_files, invitations, and invalidate cache
	if err := st.DeleteDevice("PEER-1"); err != nil {
		t.Fatal(err)
	}
	if st.IsDeviceAllowed("PEER-1") {
		t.Fatal("expected IsDeviceAllowed false after DeleteDevice")
	}
	rScanAfter, _ := st.GetRemoteScanIndex("f1", "PEER-1")
	if len(rScanAfter) != 0 {
		t.Fatalf("expected remote_files deleted when device removed")
	}
	invsAfter, _ := st.ListFolderInvitations()
	if len(invsAfter) != 0 {
		t.Fatalf("expected invitations deleted when device removed")
	}
}
