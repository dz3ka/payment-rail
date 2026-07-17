package signer

import (
	"fmt"
	"math/big"
	"sync"
)

// spendBucket is one key's cumulative spend limiter — the concurrency core of
// this package. Its mutex is the *serialization point* for a single key: every
// sign for that key runs inside charge, so the {check spend → sign → commit}
// sequence is atomic per key. Different keys hold different buckets and so sign
// in parallel; only requests contending for the *same* key serialize.
//
// The counter lives only in memory. A process restart constructs fresh buckets
// (via LoadKeyring), which zeroes every spent total — the documented fail-safe
// in ADR-0009: on restart we forget past spend rather than persist it, so a lost
// counter can never *raise* the effective limit.
type spendBucket struct {
	// mu guards spent. limit is set once at construction and never written
	// again, so it needs no locking.
	mu    sync.Mutex
	limit *big.Int // immutable after newSpendBucket
	spent *big.Int // guarded by mu; only ever increases, and only on a committed charge
}

// newSpendBucket builds a bucket with a private copy of limit so the caller
// cannot later mutate the ceiling out from under the invariant. spent starts at
// zero.
func newSpendBucket(limit *big.Int) *spendBucket {
	return &spendBucket{
		limit: new(big.Int).Set(limit),
		spent: new(big.Int),
	}
}

// charge admits a spend of amount for this key, runs sign under the key's lock,
// and commits the charge only if sign succeeds. It is the whole point of the
// per-key mutex: check, sign, and commit are one critical section, so no two
// concurrent signs for the same key can both observe room and then together
// overshoot the limit.
//
// Invariant enforced: for each key, the sum of amounts across *successful*
// charges never exceeds limit. A charge is rejected (ErrSpendLimitExceeded)
// before sign runs if it would cross the ceiling; a sign that returns an error
// leaves spent untouched, so a failed sign never consumes budget.
//
// sign is a callback rather than an argument so the signing work happens *inside*
// the guarded section — the lock is held across the entire check→sign→commit,
// not merely around a counter bump.
func (b *spendBucket) charge(amount *big.Int, sign func() (SignedTx, error)) (SignedTx, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// next is a fresh big.Int: we neither read nor store the caller's amount
	// pointer beyond this comparison, so committing next below can never alias
	// state the caller still holds.
	next := new(big.Int).Add(b.spent, amount)
	if next.Cmp(b.limit) > 0 {
		// Amount, spent, and limit are all sensitive monetary values, so the
		// message names the failure without echoing them (mirrors ledger's
		// logResult discipline). The caller gets the sentinel, not the numbers.
		return SignedTx{}, fmt.Errorf("signer: charge would exceed the key's spend limit: %w", ErrSpendLimitExceeded)
	}

	tx, err := sign()
	if err != nil {
		// Commit nothing: a failed sign must not advance the counter.
		return SignedTx{}, err
	}

	b.spent = next
	return tx, nil
}
