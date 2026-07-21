package reconcile

import (
	"os"
	"path/filepath"
	"testing"
)

// writeManifest drops a manifest fixture into a fresh temp dir and returns its path.
func writeManifest(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "treasuries.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return path
}

const validManifest = `[
  {"address": "0x1111111111111111111111111111111111111111", "asset": "USDC", "token": "0x2222222222222222222222222222222222222222"},
  {"address": "0x3333333333333333333333333333333333333333", "asset": "USDT", "token": "0x4444444444444444444444444444444444444444"}
]`

func TestLoadRegistryValid(t *testing.T) {
	reg, err := LoadRegistry(writeManifest(t, validManifest))
	if err != nil {
		t.Fatalf("LoadRegistry() error = %v, want nil", err)
	}
	if len(reg.Entries) != 2 {
		t.Fatalf("len(Entries) = %d, want 2", len(reg.Entries))
	}
	got := reg.Entries[0]
	if got.Address != "0x1111111111111111111111111111111111111111" ||
		got.Asset != "USDC" ||
		got.Token != "0x2222222222222222222222222222222222222222" {
		t.Errorf("Entries[0] = %+v, unexpected", got)
	}
	if reg.Entries[1].Asset != "USDT" {
		t.Errorf("Entries[1].Asset = %q, want USDT", reg.Entries[1].Asset)
	}
}

func TestLoadRegistryMissingFile(t *testing.T) {
	if _, err := LoadRegistry(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatal("LoadRegistry() = nil error, want error for missing file")
	}
}

func TestLoadRegistryEmptyPath(t *testing.T) {
	// An empty path is not a legal manifest — reconcile derives the single-entry
	// registry via SingleEntry, so LoadRegistry("") must fail closed.
	if _, err := LoadRegistry(""); err == nil {
		t.Fatal("LoadRegistry(\"\") = nil error, want error")
	}
}

func TestLoadRegistryMalformedJSON(t *testing.T) {
	if _, err := LoadRegistry(writeManifest(t, `{not json`)); err == nil {
		t.Fatal("LoadRegistry() = nil error, want error for malformed JSON")
	}
}

func TestLoadRegistryEmptyEntries(t *testing.T) {
	if _, err := LoadRegistry(writeManifest(t, `[]`)); err == nil {
		t.Fatal("LoadRegistry() = nil error, want error for zero entries")
	}
}

func TestLoadRegistryBadEntry(t *testing.T) {
	cases := map[string]string{
		"bad address": `[{"address": "0xzz", "asset": "USDC", "token": "0x2222222222222222222222222222222222222222"}]`,
		"bad token":   `[{"address": "0x1111111111111111111111111111111111111111", "asset": "USDC", "token": "nothex"}]`,
		"empty asset": `[{"address": "0x1111111111111111111111111111111111111111", "asset": "", "token": "0x2222222222222222222222222222222222222222"}]`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadRegistry(writeManifest(t, body)); err == nil {
				t.Fatalf("LoadRegistry() = nil error, want error for %s", name)
			}
		})
	}
}

func TestLoadRegistryDuplicatePair(t *testing.T) {
	// Same asset, same address differing only in hex case ⇒ rejected.
	body := `[
	  {"address": "0x1111111111111111111111111111111111111111", "asset": "USDC", "token": "0x2222222222222222222222222222222222222222"},
	  {"address": "0x1111111111111111111111111111111111111111", "asset": "USDC", "token": "0x9999999999999999999999999999999999999999"}
	]`
	if _, err := LoadRegistry(writeManifest(t, body)); err == nil {
		t.Fatal("LoadRegistry() = nil error, want error for duplicate (address,asset)")
	}

	caseBody := `[
	  {"address": "0xAAAAaaaaAAAAaaaaAAAAaaaaAAAAaaaaAAAAaaaa", "asset": "USDC", "token": "0x2222222222222222222222222222222222222222"},
	  {"address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "asset": "USDC", "token": "0x9999999999999999999999999999999999999999"}
	]`
	if _, err := LoadRegistry(writeManifest(t, caseBody)); err == nil {
		t.Fatal("LoadRegistry() = nil error, want error for case-insensitive duplicate")
	}
}

func TestLoadRegistrySameAddressDifferentAsset(t *testing.T) {
	// Same holder address across two different assets is legal.
	body := `[
	  {"address": "0x1111111111111111111111111111111111111111", "asset": "USDC", "token": "0x2222222222222222222222222222222222222222"},
	  {"address": "0x1111111111111111111111111111111111111111", "asset": "USDT", "token": "0x4444444444444444444444444444444444444444"}
	]`
	reg, err := LoadRegistry(writeManifest(t, body))
	if err != nil {
		t.Fatalf("LoadRegistry() error = %v, want nil", err)
	}
	if len(reg.Entries) != 2 {
		t.Fatalf("len(Entries) = %d, want 2", len(reg.Entries))
	}
}

func TestSingleEntry(t *testing.T) {
	reg, err := SingleEntry(
		"0x1111111111111111111111111111111111111111",
		"USDC",
		"0x2222222222222222222222222222222222222222",
	)
	if err != nil {
		t.Fatalf("SingleEntry() error = %v, want nil", err)
	}
	if len(reg.Entries) != 1 {
		t.Fatalf("len(Entries) = %d, want 1", len(reg.Entries))
	}
	if reg.Entries[0].Asset != "USDC" {
		t.Errorf("Asset = %q, want USDC", reg.Entries[0].Asset)
	}
}

func TestSingleEntryInvalid(t *testing.T) {
	cases := map[string]struct{ address, asset, token string }{
		"empty address": {"", "USDC", "0x2222222222222222222222222222222222222222"},
		"empty token":   {"0x1111111111111111111111111111111111111111", "USDC", ""},
		"empty asset":   {"0x1111111111111111111111111111111111111111", "", "0x2222222222222222222222222222222222222222"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := SingleEntry(c.address, c.asset, c.token); err == nil {
				t.Fatalf("SingleEntry() = nil error, want error for %s", name)
			}
		})
	}
}

func TestTreasuryEntryValidate(t *testing.T) {
	valid := TreasuryEntry{
		Address: "0x1111111111111111111111111111111111111111",
		Asset:   "USDC",
		Token:   "0x2222222222222222222222222222222222222222",
	}
	tests := []struct {
		name    string
		mutate  func(TreasuryEntry) TreasuryEntry
		wantErr bool
	}{
		{"valid", func(e TreasuryEntry) TreasuryEntry { return e }, false},
		{"checksum-cased address ok", func(e TreasuryEntry) TreasuryEntry {
			e.Address = "0xAbCdEf0123456789aBcDeF0123456789AbCdEf01"
			return e
		}, false},
		{"empty asset", func(e TreasuryEntry) TreasuryEntry { e.Asset = ""; return e }, true},
		{"no 0x prefix", func(e TreasuryEntry) TreasuryEntry {
			e.Address = "1111111111111111111111111111111111111111"
			return e
		}, true},
		{"address too short", func(e TreasuryEntry) TreasuryEntry { e.Address = "0x1111"; return e }, true},
		{"address too long", func(e TreasuryEntry) TreasuryEntry {
			e.Address = "0x11111111111111111111111111111111111111112"
			return e
		}, true},
		{"non-hex address", func(e TreasuryEntry) TreasuryEntry {
			e.Address = "0xzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
			return e
		}, true},
		{"bad token", func(e TreasuryEntry) TreasuryEntry { e.Token = "0xnope"; return e }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.mutate(valid).Validate()
			if tt.wantErr && err == nil {
				t.Fatal("Validate() = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}
