// registry.go loads the treasury manifest the M6 reconcile command reads on-chain
// balances for. Like the rest of internal/reconcile it stays dependency-light:
// stdlib only (no go-ethereum common). The manifest mirrors the policy denylist /
// signer keyring convention — a bare top-level JSON array of entries — and the
// loader fails CLOSED: any read, parse, or validate error returns a zero Registry
// and a non-nil error rather than a partial or empty registry that would silently
// reconcile against nothing.
//
// Address validation here is a deliberately lightweight stdlib format check
// ("0x" prefix + 40 hex chars). Canonical address validation is the on-chain
// BalanceReader's job (WP1) — it rejects anything this loose check lets through.

package reconcile

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// TreasuryEntry is one holder/asset row of the manifest: the on-chain address
// whose ERC-20 balance is read, the ledger asset symbol it maps to, and that
// asset's token contract.
type TreasuryEntry struct {
	Address string `json:"address"` // 0x-hex treasury/holder address whose on-chain balance is read
	Asset   string `json:"asset"`   // ledger asset symbol, e.g. "USDC"
	Token   string `json:"token"`   // 0x-hex ERC-20 contract address for that asset
}

// Registry is the validated set of treasuries the reconcile command reads.
type Registry struct {
	Entries []TreasuryEntry
}

// LoadRegistry reads, parses, and validates a JSON treasury manifest. It fails
// CLOSED: an unreadable file, malformed JSON, zero entries, any entry failing
// Validate, or a duplicate (Address,Asset) pair (case-insensitive on the hex
// address) all return a zero Registry and a non-nil error. The manifest is a bare
// top-level JSON array, mirroring the policy denylist and signer keyring manifests.
//
// Unlike policy.Load, the empty path is NOT a legal no-op here: reconcile derives
// its single-entry fallback via SingleEntry, so LoadRegistry("") is a caller error
// (os.ReadFile reports it) and fails closed like any other bad path.
func LoadRegistry(path string) (Registry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Registry{}, fmt.Errorf("load treasury registry %q: %w", path, err)
	}

	var entries []TreasuryEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return Registry{}, fmt.Errorf("load treasury registry %q: %w", path, err)
	}
	if len(entries) == 0 {
		return Registry{}, fmt.Errorf("load treasury registry %q: no entries", path)
	}

	// Reject duplicate holder/asset pairs — two rows reconciling the same balance
	// against divergent tokens is a manifest bug, not a merge to silently resolve.
	seen := make(map[string]struct{}, len(entries))
	for i, e := range entries {
		if err := e.Validate(); err != nil {
			return Registry{}, fmt.Errorf("load treasury registry %q: entry %d: %w", path, i, err)
		}
		key := strings.ToLower(e.Address) + "\x00" + e.Asset
		if _, dup := seen[key]; dup {
			return Registry{}, fmt.Errorf("load treasury registry %q: duplicate (address,asset) for %s/%s", path, e.Address, e.Asset)
		}
		seen[key] = struct{}{}
	}

	return Registry{Entries: entries}, nil
}

// SingleEntry builds a validated one-entry Registry for the Chain* fallback — WP4
// calls it when the manifest path is "". It returns an error if the derived entry
// is invalid (e.g. unset ChainFromAddress/ChainUSDCAddress config).
func SingleEntry(address, asset, token string) (Registry, error) {
	e := TreasuryEntry{Address: address, Asset: asset, Token: token}
	if err := e.Validate(); err != nil {
		return Registry{}, fmt.Errorf("single-entry treasury registry: %w", err)
	}
	return Registry{Entries: []TreasuryEntry{e}}, nil
}

// Validate checks that Asset is non-empty and Address/Token are 0x-hex addresses.
// The address check is lightweight (stdlib only): "0x" prefix plus exactly 40 hex
// characters. Canonical/checksum validation is the BalanceReader's responsibility.
func (e TreasuryEntry) Validate() error {
	if e.Asset == "" {
		return fmt.Errorf("asset is empty")
	}
	if err := validateHexAddress(e.Address); err != nil {
		return fmt.Errorf("address %q: %w", e.Address, err)
	}
	if err := validateHexAddress(e.Token); err != nil {
		return fmt.Errorf("token %q: %w", e.Token, err)
	}
	return nil
}

// validateHexAddress accepts a "0x"-prefixed 20-byte (40 hex char) address. It
// decodes the trailing 40 chars via encoding/hex so mixed-case checksum addresses
// pass while non-hex or wrong-length strings fail.
func validateHexAddress(s string) error {
	if !strings.HasPrefix(s, "0x") && !strings.HasPrefix(s, "0X") {
		return fmt.Errorf("missing 0x prefix")
	}
	body := s[2:]
	if len(body) != 40 {
		return fmt.Errorf("want 40 hex chars, got %d", len(body))
	}
	if _, err := hex.DecodeString(body); err != nil {
		return fmt.Errorf("not hex: %w", err)
	}
	return nil
}
