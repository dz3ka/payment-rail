package signer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// writeKeyFile writes a freshly generated hex private key to dir/name at the
// given mode and returns the key's address. No secret is committed: the key is
// generated in-test and lives only in the temp dir.
func writeKeyFile(t *testing.T, dir, name string, mode os.FileMode) common.Address {
	t.Helper()
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() = %v", err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(hex.EncodeToString(crypto.FromECDSA(priv))), 0o600); err != nil {
		t.Fatalf("WriteFile(%s) = %v", path, err)
	}
	// Chmod explicitly so the permission gate is tested against a known mode,
	// independent of the process umask.
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("Chmod(%s) = %v", path, err)
	}
	return crypto.PubkeyToAddress(priv.PublicKey)
}

// writeManifest marshals entries to dir/manifest.json (world-readable is fine —
// the manifest holds no secrets) and returns its path.
func writeManifest(t *testing.T, dir string, entries []manifestEntry) string {
	t.Helper()
	raw, err := json.Marshal(entries)
	if err != nil {
		t.Fatalf("Marshal(manifest) = %v", err)
	}
	path := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("WriteFile(manifest) = %v", err)
	}
	return path
}

func TestLoadKeyring_Succeeds(t *testing.T) {
	dir := t.TempDir()
	addr := writeKeyFile(t, dir, "hot.key", 0o600)
	path := writeManifest(t, dir, []manifestEntry{
		{KeyID: "hot", KeyFile: "hot.key", ChainID: 1, SpendLimit: "1000000"},
	})

	kr, err := LoadKeyring(path)
	if err != nil {
		t.Fatalf("LoadKeyring() = %v, want nil", err)
	}
	key, ok := kr.lookup("hot")
	if !ok {
		t.Fatal("lookup(hot) not found")
	}
	if key.address != addr {
		t.Fatalf("loaded address = %s, want %s", key.address, addr)
	}
	if key.chainID != 1 {
		t.Fatalf("chainID = %d, want 1", key.chainID)
	}
	if key.bucket.limit.Cmp(big.NewInt(1_000_000)) != 0 {
		t.Fatalf("limit = %s, want 1000000", key.bucket.limit)
	}
}

func TestLoadKeyring_RejectsGroupOrWorldReadableKeyFile(t *testing.T) {
	dir := t.TempDir()
	writeKeyFile(t, dir, "hot.key", 0o644) // group/world readable => must be refused
	path := writeManifest(t, dir, []manifestEntry{
		{KeyID: "hot", KeyFile: "hot.key", ChainID: 1, SpendLimit: "1000000"},
	})

	if _, err := LoadKeyring(path); err == nil {
		t.Fatal("LoadKeyring() = nil, want error for world-readable key file")
	}
}

func TestLoadKeyring_RejectsBadHex(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.key")
	if err := os.WriteFile(path, []byte("not-hex-at-all"), 0o600); err != nil {
		t.Fatalf("WriteFile = %v", err)
	}
	manifest := writeManifest(t, dir, []manifestEntry{
		{KeyID: "hot", KeyFile: "bad.key", ChainID: 1, SpendLimit: "1000000"},
	})

	if _, err := LoadKeyring(manifest); err == nil {
		t.Fatal("LoadKeyring() = nil, want error for malformed hex key")
	}
}

func TestLoadKeyring_RejectsDuplicateKeyID(t *testing.T) {
	dir := t.TempDir()
	writeKeyFile(t, dir, "a.key", 0o600)
	writeKeyFile(t, dir, "b.key", 0o600)
	path := writeManifest(t, dir, []manifestEntry{
		{KeyID: "dup", KeyFile: "a.key", ChainID: 1, SpendLimit: "10"},
		{KeyID: "dup", KeyFile: "b.key", ChainID: 1, SpendLimit: "20"},
	})

	if _, err := LoadKeyring(path); err == nil {
		t.Fatal("LoadKeyring() = nil, want error for duplicate key_id")
	}
}

// TestLoadKeyring_PerKeyIsolation loads two keys and proves their spend limits
// are independent: exhausting key A's limit leaves key B fully usable.
func TestLoadKeyring_PerKeyIsolation(t *testing.T) {
	const chainID = uint64(1)
	dir := t.TempDir()
	writeKeyFile(t, dir, "a.key", 0o600)
	writeKeyFile(t, dir, "b.key", 0o600)
	path := writeManifest(t, dir, []manifestEntry{
		{KeyID: "A", KeyFile: "a.key", ChainID: chainID, SpendLimit: "100"},
		{KeyID: "B", KeyFile: "b.key", ChainID: chainID, SpendLimit: "1000000"},
	})

	kr, err := LoadKeyring(path)
	if err != nil {
		t.Fatalf("LoadKeyring() = %v", err)
	}
	s := NewSigner(kr)
	ctx := context.Background()

	// Spend key A up to its limit of 100.
	reqA := validNativeReq("A", chainID)
	reqA.Value = big.NewInt(100)
	if _, err := s.Sign(ctx, reqA); err != nil {
		t.Fatalf("Sign(A, 100) = %v, want nil", err)
	}
	// The next charge on A must be refused — A is exhausted.
	reqA2 := validNativeReq("A", chainID)
	reqA2.Value = big.NewInt(1)
	if _, err := s.Sign(ctx, reqA2); !errors.Is(err, ErrSpendLimitExceeded) {
		t.Fatalf("Sign(A, 1) after exhaustion = %v, want ErrSpendLimitExceeded", err)
	}
	// Key B, isolated, is still fully usable.
	reqB := validNativeReq("B", chainID)
	reqB.Value = big.NewInt(500)
	if _, err := s.Sign(ctx, reqB); err != nil {
		t.Fatalf("Sign(B, 500) = %v, want nil (B must be unaffected by A)", err)
	}
}
