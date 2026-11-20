package protocol

import (
	"bytes"
	"strings"
	"testing"
)

func TestWriteAndReadFrameCompressionRoundTrip(t *testing.T) {
	// Small payload (uncompressed)
	small := ControlEnvelope{Type: CtrlPing}
	var buf bytes.Buffer
	if err := WriteFrame(&buf, small); err != nil {
		t.Fatal(err)
	}
	var gotSmall ControlEnvelope
	if err := ReadFrame(&buf, &gotSmall); err != nil {
		t.Fatal(err)
	}
	if gotSmall.Type != CtrlPing {
		t.Fatalf("expected ping, got %q", gotSmall.Type)
	}

	// Large compressible payload (> 1 KiB)
	large := FolderIndexMsg{
		FolderID: "docs",
		Full:     true,
		Files: []FileMetadata{
			{RelPath: strings.Repeat("subdir/", 40) + "file.txt", Size: 4096, FileHash: strings.Repeat("a", 64)},
			{RelPath: strings.Repeat("subdir/", 40) + "file2.txt", Size: 4096, FileHash: strings.Repeat("a", 64)},
			{RelPath: strings.Repeat("subdir/", 40) + "file3.txt", Size: 4096, FileHash: strings.Repeat("a", 64)},
		},
	}
	buf.Reset()
	if err := WriteFrame(&buf, large); err != nil {
		t.Fatal(err)
	}
	var gotLarge FolderIndexMsg
	if err := ReadFrame(&buf, &gotLarge); err != nil {
		t.Fatal(err)
	}
	if gotLarge.FolderID != "docs" || len(gotLarge.Files) != 3 {
		t.Fatalf("unexpected decoded large frame: %+v", gotLarge)
	}
}

func TestUDPRelayPacketRoundTrip(t *testing.T) {
	var tok [16]byte
	for i := range tok {
		tok[i] = byte(i + 1)
	}
	payload := []byte("quic-encrypted-packet-bytes")
	pkt := EncodeUDPRelayPacket(nil, tok, "abcd-efgh", payload)

	gotTok, gotPeer, gotPayload, ok := DecodeUDPRelayPacket(pkt)
	if !ok {
		t.Fatal("DecodeUDPRelayPacket failed")
	}
	if gotTok != tok {
		t.Fatalf("token mismatch")
	}
	if gotPeer != "ABCD-EFGH" {
		t.Fatalf("expected normalized peer ID, got %q", gotPeer)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("payload mismatch")
	}

	// Empty peerID keepalive probe
	probe := EncodeUDPRelayPacket(nil, tok, "", nil)
	gotTok, gotPeer, gotPayload, ok = DecodeUDPRelayPacket(probe)
	if !ok || gotTok != tok || gotPeer != "" || len(gotPayload) != 0 {
		t.Fatalf("probe decode mismatch: ok=%v peer=%q len=%d", ok, gotPeer, len(gotPayload))
	}
}
