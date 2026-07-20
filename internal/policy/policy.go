// Package policy screens a payment's on-chain destination address before the
// transaction is broadcast (PRD F8). It is a ports-and-adapters seam: callers
// depend on the Screener interface, and the file-backed Denylist here is the
// "mock included" first implementation. A real sanctions-screening provider
// (I/O-backed, hence the context.Context and error return on Screen) is the
// intended second implementation at this same seam.
//
// The whole package is fail-CLOSED. Load rejects a missing file, malformed
// manifest, or a bad address rather than starting with an empty allow-all set
// that would silently pass every payment; and callers treat ANY error from
// Screen — a denial (errors.Is ErrDenied) or an operational failure of the
// backing store — as a reason to block the payment. The one deliberate open
// door is the legacy no-op path: Load("") disables screening entirely and
// returns an allow-all Denylist without touching the filesystem.
package policy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/ethereum/go-ethereum/common"
)

// Screener screens a payment's on-chain destination address before broadcast.
// The file-backed Denylist below is the "mock included" (PRD F8); a real
// sanctions-screening provider (I/O-backed) is the intended second impl at this
// same seam — hence ctx and the error return.
type Screener interface {
	// Screen returns nil to ALLOW, an error wrapping ErrDenied to DENY (the
	// message carries the non-sensitive reason), or a non-ErrDenied error for an
	// operational failure of the backing store (callers fail CLOSED on any error).
	Screen(ctx context.Context, address string) error
}

// ErrDenied marks a policy denial so callers can errors.Is() it apart from an
// operational failure.
var ErrDenied = errors.New("policy: destination denied")

// Denylist is a file-manifest Screener: a set of denied addresses, each with a
// non-sensitive reason. Address matching is case/checksum-insensitive.
type Denylist struct {
	denied map[common.Address]string
}

var _ Screener = (*Denylist)(nil)

// denylistEntry mirrors one element of the JSON denylist manifest. reason is a
// non-sensitive, operator-facing explanation echoed back in a denial error.
type denylistEntry struct {
	Address string `json:"address"`
	Reason  string `json:"reason"`
}

// Load builds a Denylist from a JSON manifest of denied addresses. It fails
// CLOSED: a missing file, malformed JSON, or any entry whose address is not a
// valid hex address aborts the load rather than starting with a set that would
// silently allow those payments.
//
// The empty-path case is the one exception and the legacy no-op path: Load("")
// disables screening and returns an allow-all Denylist WITHOUT reading any file
// (and never errors). An empty manifest ("[]") is likewise valid and allow-all.
// Addresses are normalized via common.HexToAddress so matching is
// case/checksum-insensitive (0xAbC == 0xabc).
func Load(path string) (*Denylist, error) {
	if path == "" {
		// Screening disabled: allow-all, no file read. Not an error.
		return &Denylist{denied: map[common.Address]string{}}, nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("policy: load denylist %q: %w", path, err)
	}
	var entries []denylistEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("policy: load denylist %q: %w", path, err)
	}

	denied := make(map[common.Address]string, len(entries))
	for _, e := range entries {
		if !common.IsHexAddress(e.Address) {
			return nil, fmt.Errorf("policy: load denylist %q: invalid address %q", path, e.Address)
		}
		// Normalize so a checksummed manifest entry and a lower-case Screen call
		// for the same address match.
		denied[common.HexToAddress(e.Address)] = e.Reason
	}
	return &Denylist{denied: denied}, nil
}

// Screen returns nil when address is not on the denylist, or an error wrapping
// ErrDenied (carrying the entry's non-sensitive reason) when it is. The address
// is normalized via common.HexToAddress so the lookup is case/checksum-
// insensitive. The context is accepted for interface stability (an I/O-backed
// screener needs it); this in-memory lookup ignores it, hence the blank name.
func (d *Denylist) Screen(_ context.Context, address string) error {
	if reason, ok := d.denied[common.HexToAddress(address)]; ok {
		return fmt.Errorf("address on denylist (%s): %w", reason, ErrDenied)
	}
	return nil
}
