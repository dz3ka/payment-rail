package ledger

import (
	"fmt"

	"github.com/google/uuid"
)

// Balanced reports whether a set of entry lines forms a valid, balanced
// double-entry posting. It is a pure function: it touches no I/O and no shared
// state, which is exactly why it can be exhaustively property-tested.
//
// It returns nil iff:
//   - there is at least one line,
//   - every line amount is strictly positive, and
//   - the sum of debits equals the sum of credits.
//
// Shape problems (no lines, non-positive amount, unknown direction) wrap
// ErrInvalidEntry; a well-formed but lopsided posting wraps ErrUnbalanced. The
// %w verb lets callers discriminate with errors.Is without string matching.
func Balanced(lines []Line) error {
	if len(lines) == 0 {
		return fmt.Errorf("ledger: entry has no lines: %w", ErrInvalidEntry)
	}

	var debits, credits int64
	for _, l := range lines {
		if l.Amount <= 0 {
			return fmt.Errorf("ledger: line amount must be positive, got %d: %w", l.Amount, ErrInvalidEntry)
		}
		switch l.Direction {
		case Debit:
			debits += l.Amount
		case Credit:
			credits += l.Amount
		default:
			return fmt.Errorf("ledger: unknown direction %q: %w", l.Direction, ErrInvalidEntry)
		}
	}

	if debits != credits {
		return fmt.Errorf("ledger: debits %d != credits %d: %w", debits, credits, ErrUnbalanced)
	}
	return nil
}

// ApplyToBalances applies each line to a COPY of cur and returns the resulting
// balances, following the same convention as the SQL GetAccountBalance query:
// a credit raises a balance, a debit lowers it. cur is never mutated.
//
// If any resulting balance would drop below zero it returns a wrapped
// ErrInsufficientFunds and a nil map, so the caller can abort the transaction
// before writing anything. Like Balanced, this is a pure function and the
// heart of the "no account goes negative" invariant.
func ApplyToBalances(cur map[uuid.UUID]int64, lines []Line) (map[uuid.UUID]int64, error) {
	next := make(map[uuid.UUID]int64, len(cur))
	for id, bal := range cur {
		next[id] = bal
	}

	for _, l := range lines {
		switch l.Direction {
		case Credit:
			next[l.AccountID] += l.Amount
		case Debit:
			next[l.AccountID] -= l.Amount
		default:
			return nil, fmt.Errorf("ledger: unknown direction %q: %w", l.Direction, ErrInvalidEntry)
		}
	}

	for id, bal := range next {
		if bal < 0 {
			return nil, fmt.Errorf("ledger: account %s balance %d < 0: %w", id, bal, ErrInsufficientFunds)
		}
	}
	return next, nil
}
