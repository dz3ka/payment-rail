package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dz3ka/payment-rail/internal/policy"
)

// These tests prove the destination-screening seam WP3 wires into runSubmit,
// and — critically — the ORDERING invariant: the screen runs before any signer
// or chain dial. They are fully hermetic (no network, no Postgres): every case
// returns before the first dial, so no *_TEST_DSN gate or bufconn is needed.
// Each writes a denylist manifest to t.TempDir() and points
// PAYMENT_RAIL_POLICY_DENYLIST at it via t.Setenv.

// writeDenylist drops a JSON denylist manifest into a fresh temp dir and returns
// its path, keeping each test self-contained.
func writeDenylist(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "denylist.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write denylist: %v", err)
	}
	return path
}

// deniedAddress is an arbitrary well-formed destination used across the cases.
const deniedAddress = "0x00000000000000000000000000000000000000AB"

// TestSubmit_DeniedDestination_FailsClosedBeforeDial proves a denied --to is
// rejected with an ErrDenied-wrapping error EVEN THOUGH no chain/signer env is
// set. If the screen ran after the config validation or a dial, the command
// would fail with a "PAYMENT_RAIL_CHAIN_* required" error instead; that it fails
// at policy proves the screen runs first and no dial ever happens.
func TestSubmit_DeniedDestination_FailsClosedBeforeDial(t *testing.T) {
	manifest := `[{"address":"` + deniedAddress + `","reason":"ofac-sdn"}]`
	t.Setenv("PAYMENT_RAIL_POLICY_DENYLIST", writeDenylist(t, manifest))

	err := runSubmit([]string{"--to", deniedAddress, "--amount", "1"})
	if err == nil {
		t.Fatal("runSubmit() = nil, want an ErrDenied error for a denylisted --to")
	}
	if !errors.Is(err, policy.ErrDenied) {
		t.Fatalf("runSubmit() error = %v, want errors.Is policy.ErrDenied", err)
	}
	// The denial must beat every downstream config/dial failure: no chain env is
	// set, so any non-policy failure would mention PAYMENT_RAIL_CHAIN_*.
	if strings.Contains(err.Error(), "PAYMENT_RAIL_CHAIN") {
		t.Fatalf("runSubmit() error = %v; screen must run before chain-config validation", err)
	}
}

// TestSubmit_MalformedTo_FailsBeforeConfigLoad proves the IsHexAddress guard
// rejects a non-address --to up front — before config load or screening — so a
// malformed destination never reaches policy.Load or a dial. The error names the
// bad value and is NOT an ErrDenied (this is a validation failure, not a denial).
func TestSubmit_MalformedTo_FailsBeforeConfigLoad(t *testing.T) {
	// Point the denylist at a nonexistent path: policy.Load would fail CLOSED if
	// reached, so if the error is about the invalid address (not a load failure),
	// the --to guard fired before config load / screening.
	t.Setenv("PAYMENT_RAIL_POLICY_DENYLIST", filepath.Join(t.TempDir(), "does-not-exist.json"))

	err := runSubmit([]string{"--to", "not-an-address", "--amount", "1"})
	if err == nil {
		t.Fatal("runSubmit() = nil, want an error for a malformed --to")
	}
	if errors.Is(err, policy.ErrDenied) {
		t.Fatalf("runSubmit() error = %v, want a validation error, not ErrDenied", err)
	}
	if !strings.Contains(err.Error(), "not a valid address") {
		t.Fatalf("runSubmit() error = %v, want it to report an invalid address", err)
	}
}

// TestSubmit_AllowedDestination_PassesScreenFailsDownstream proves an allowed
// address is NOT blocked: with a denylist that does not contain --to, the screen
// passes and the command proceeds to the chain-config validation, failing there
// (no RPC URL set) rather than at policy. The error is NOT ErrDenied and names a
// downstream chain concern — proving screening let the payment through.
func TestSubmit_AllowedDestination_PassesScreenFailsDownstream(t *testing.T) {
	// A different address is on the list; --to below is not, so it is allowed.
	manifest := `[{"address":"0x000000000000000000000000000000000000dEaD","reason":"test"}]`
	t.Setenv("PAYMENT_RAIL_POLICY_DENYLIST", writeDenylist(t, manifest))
	// Ensure no chain RPC is configured so the first post-screen failure is the
	// chain-config switch, which is what we assert on.
	t.Setenv("PAYMENT_RAIL_CHAIN_RPC_URL", "")

	err := runSubmit([]string{"--to", deniedAddress, "--amount", "1"})
	if err == nil {
		t.Fatal("runSubmit() = nil, want a downstream chain-config error")
	}
	if errors.Is(err, policy.ErrDenied) {
		t.Fatalf("runSubmit() error = %v; an allowed address must not be denied", err)
	}
	if !strings.Contains(err.Error(), "PAYMENT_RAIL_CHAIN_RPC_URL") {
		t.Fatalf("runSubmit() error = %v, want it to fail past screening at chain config", err)
	}
}
