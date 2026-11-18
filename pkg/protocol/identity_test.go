package protocol

import (
	"testing"
)

func TestIdentityPersistenceAndChallenge(t *testing.T) {
	dir := t.TempDir()

	id1, err := LoadOrCreateIdentity(dir)
	if err != nil {
		t.Fatalf("LoadOrCreateIdentity failed: %v", err)
	}
	if id1.DeviceID == "" {
		t.Fatal("expected non-empty DeviceID")
	}

	id2, err := LoadOrCreateIdentity(dir)
	if err != nil {
		t.Fatalf("reload failed: %v", err)
	}
	if id1.DeviceID != id2.DeviceID {
		t.Fatalf("DeviceID changed on reload: %s vs %s", id1.DeviceID, id2.DeviceID)
	}

	challenge := []byte("random-challenge-32-bytes-123456")
	sig := id1.SignChallenge(challenge)
	if err := VerifyChallenge(id1.DeviceID, id1.PubKeyDER, challenge, sig); err != nil {
		t.Fatalf("VerifyChallenge failed: %v", err)
	}

	if err := VerifyChallenge(id1.DeviceID, id1.PubKeyDER, []byte("tampered"), sig); err == nil {
		t.Fatal("expected tampered challenge to fail verification")
	}
	if err := VerifyChallenge("AAAA-BBBB-CCCC-DDDD", id1.PubKeyDER, challenge, sig); err == nil {
		t.Fatal("expected mismatched DeviceID to fail verification")
	}
}
