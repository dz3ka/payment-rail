package policy

import (
	"errors"
	"math/big"
)

// ErrSelfApproval marks a four-eyes rejection where the approver is the same
// identity that proposed the payment. Callers errors.Is() it apart from an
// unknown-approver rejection. Mirrors ErrDenied/ErrVelocityExceeded.
var ErrSelfApproval = errors.New("policy: approver must differ from proposer")

// ErrUnknownApprover marks a four-eyes rejection where the approver is not in
// the configured allowlist.
var ErrUnknownApprover = errors.New("policy: approver not in allowlist")

// Intent captures the full payment an approval will replay when broadcast. It
// is the frozen, proposed-at request: an approval authorizes exactly this
// intent, so the fields mirror what the submit path would otherwise carry.
type Intent struct {
	To        string
	Asset     string
	KeyID     string
	PaymentID string // "" = none
	Amount    *big.Int
}

// PendingApproval is a proposed-but-not-yet-approved payment as loaded from the
// store. Status is the store's lifecycle marker (e.g. pending/approved); the
// gate itself is stateless and does not interpret it.
type PendingApproval struct {
	ID       string
	Proposer string
	Status   string
	Intent   Intent
}

// ApprovalGate decides whether a payment needs four-eyes and whether an
// approver may approve it (PRD F8c). It is purely in-memory and deterministic:
// no I/O, no clock, no DB. Construct it with NewApprovalGate; a nil
// *ApprovalGate is a valid disabled gate.
type ApprovalGate struct {
	threshold *big.Int            // nil or <=0 => disabled (no amount is gated)
	approvers map[string]struct{} // allowlist of approver identities
}

// NewApprovalGate binds an at/above amount threshold to an approver allowlist.
// A nil or non-positive threshold yields a disabled gate whose Required always
// reports false. The approvers slice is copied into a lookup set; a nil/empty
// slice yields a gate that knows no approver (Authorize then always rejects).
func NewApprovalGate(threshold *big.Int, approvers []string) *ApprovalGate {
	set := make(map[string]struct{}, len(approvers))
	for _, a := range approvers {
		set[a] = struct{}{}
	}
	return &ApprovalGate{threshold: threshold, approvers: set}
}

// enabled reports whether the gate enforces four-eyes at all: a positive
// threshold. A nil receiver or nil/non-positive threshold is disabled.
func (g *ApprovalGate) enabled() bool {
	return g != nil && g.threshold != nil && g.threshold.Sign() > 0
}

// Required reports whether an amount must go through four-eyes. A disabled gate
// (nil receiver or nil/non-positive threshold) reports false for any amount.
// The threshold is "at or above": an amount EQUAL to the threshold IS gated
// (amount.Cmp(threshold) >= 0). Callers pass a validated non-nil amount, same
// as the submit path already guarantees via its SetString/Sign checks; a nil
// amount is treated as non-required (nothing to gate).
func (g *ApprovalGate) Required(amount *big.Int) bool {
	if !g.enabled() || amount == nil {
		return false
	}
	return amount.Cmp(g.threshold) >= 0
}

// KnownApprover reports whether id is in the allowlist. Used to validate the
// proposer at propose-time (so a proposal is never created by an identity that
// could never be counted as one of the two eyes). A nil receiver knows no one.
func (g *ApprovalGate) KnownApprover(id string) bool {
	if g == nil {
		return false
	}
	_, ok := g.approvers[id]
	return ok
}

// Authorize checks that approver may approve a proposal by proposer. The
// approver must be a known allowlisted identity AND distinct from the proposer.
// Precedence: the unknown-approver check runs FIRST, so an approver who is both
// unknown and equal to the proposer yields ErrUnknownApprover. Returns nil when
// the approval is allowed. (The proposer's own allowlist membership is
// validated separately at propose-time via KnownApprover; Authorize's job is
// the approver and the distinctness.)
func (g *ApprovalGate) Authorize(proposer, approver string) error {
	if !g.KnownApprover(approver) {
		return ErrUnknownApprover
	}
	if approver == proposer {
		return ErrSelfApproval
	}
	return nil
}
