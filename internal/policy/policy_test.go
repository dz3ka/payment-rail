package policy

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeManifest drops a manifest file into a fresh temp dir and returns its
// path, keeping each test hermetic.
func writeManifest(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "denylist.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return path
}

func TestScreen(t *testing.T) {
	// Sanctioned address stored checksummed; screened later in lower case to
	// prove case/checksum-insensitive matching.
	const (
		listedChecksummed = "0xAbC0000000000000000000000000000000000001"
		listedLower       = "0xabc0000000000000000000000000000000000001"
		notListed         = "0x0000000000000000000000000000000000000002"
		reason            = "OFAC SDN 12345"
	)
	manifest := `[{"address":"` + listedChecksummed + `","reason":"` + reason + `"}]`

	tests := []struct {
		name       string
		address    string
		wantDenied bool
	}{
		{name: "not listed is allowed", address: notListed, wantDenied: false},
		{name: "listed is denied", address: listedChecksummed, wantDenied: true},
		{name: "case/checksum normalized match", address: listedLower, wantDenied: true},
	}

	d, err := Load(writeManifest(t, manifest))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := d.Screen(context.Background(), tc.address)
			if !tc.wantDenied {
				if err != nil {
					t.Fatalf("Screen(%s) = %v, want nil (allow)", tc.address, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Screen(%s) = nil, want denial", tc.address)
			}
			if !errors.Is(err, ErrDenied) {
				t.Errorf("Screen(%s) error not ErrDenied: %v", tc.address, err)
			}
			if !strings.Contains(err.Error(), reason) {
				t.Errorf("Screen(%s) error %q missing reason %q", tc.address, err, reason)
			}
		})
	}
}

func TestLoadEmptyPathDisablesScreening(t *testing.T) {
	// Load("") is the legacy no-op path: no file read, never errors, allow-all.
	d, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\") error = %v, want nil", err)
	}
	if d == nil {
		t.Fatal("Load(\"\") = nil Denylist, want allow-all instance")
	}
	// An empty denylist allows any address, including a well-formed one that
	// would otherwise be a plausible screening target.
	if err := d.Screen(context.Background(), "0x0000000000000000000000000000000000000009"); err != nil {
		t.Errorf("Screen on disabled screener = %v, want nil", err)
	}
}

func TestLoadEmptyManifestAllowsAll(t *testing.T) {
	d, err := Load(writeManifest(t, `[]`))
	if err != nil {
		t.Fatalf("Load(empty manifest) error = %v", err)
	}
	if err := d.Screen(context.Background(), "0x0000000000000000000000000000000000000003"); err != nil {
		t.Errorf("Screen against empty manifest = %v, want nil", err)
	}
}

func TestLoadFailClosed(t *testing.T) {
	tests := []struct {
		name string
		// setup returns the path to Load; body "" means "point at a missing file".
		path func(t *testing.T) string
	}{
		{
			name: "missing file",
			path: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "does-not-exist.json")
			},
		},
		{
			name: "malformed json",
			path: func(t *testing.T) string {
				return writeManifest(t, `{not json`)
			},
		},
		{
			name: "invalid address entry",
			path: func(t *testing.T) string {
				return writeManifest(t, `[{"address":"0xzzz","reason":"bad"}]`)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, err := Load(tc.path(t))
			if err == nil {
				t.Fatalf("Load() = nil error, want fail-closed error")
			}
			if d != nil {
				t.Errorf("Load() = %v Denylist on error, want nil", d)
			}
		})
	}
}

// TestDenialDistinctFromOperationalError pins the contract that a denial is
// distinguishable from an operational error: a denial wraps ErrDenied, an
// operational failure (a bad load) does not.
func TestDenialDistinctFromOperationalError(t *testing.T) {
	d, err := Load(writeManifest(t, `[{"address":"0x0000000000000000000000000000000000000001","reason":"blocked"}]`))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	denial := d.Screen(context.Background(), "0x0000000000000000000000000000000000000001")
	if denial == nil {
		t.Fatal("expected a denial error")
	}
	if !errors.Is(denial, ErrDenied) {
		t.Errorf("denial not ErrDenied: %v", denial)
	}

	_, opErr := Load(filepath.Join(t.TempDir(), "missing.json"))
	if opErr == nil {
		t.Fatal("expected an operational load error")
	}
	if errors.Is(opErr, ErrDenied) {
		t.Errorf("operational error must not be ErrDenied: %v", opErr)
	}
}
