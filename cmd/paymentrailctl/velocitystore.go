package main

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"math/big"
	"time"

	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/policy"
)

// pgVelocityStore is the composition-root Postgres impl of policy.VelocityStore.
// It owns the transaction boundary — BEGIN/lock/SELECT/decide/INSERT/COMMIT — and
// mirrors the tx lifecycle of internal/ledger.SQLStore: BeginTx, defer Rollback,
// commit only on success. All ledger logic lives in db.Queries; this type only
// sequences it under a per-key advisory lock so the check-then-insert is atomic.
type pgVelocityStore struct {
	db *sql.DB
}

// newVelocityStore wraps an already-open *sql.DB; pool lifecycle (Open/Close)
// belongs to the caller that constructed the handle.
func newVelocityStore(sqlDB *sql.DB) *pgVelocityStore {
	return &pgVelocityStore{db: sqlDB}
}

// Compile-time proof that pgVelocityStore satisfies the seam the policy limiter
// depends on.
var _ policy.VelocityStore = (*pgVelocityStore)(nil)

// Charge atomically, for keyID: acquires a per-key advisory lock, computes the
// in-window Usage over (since, now], calls decide, and — only if decide returns
// nil — inserts one event before commit. If decide returns non-nil, nothing is
// recorded and that error propagates UNCHANGED (so ErrVelocityExceeded survives
// errors.Is); the deferred rollback discards the read. Operational failures are
// wrapped %w with context and are never the policy sentinel.
func (s *pgVelocityStore) Charge(ctx context.Context, keyID string, amount *big.Int, window time.Duration, now time.Time, decide func(policy.Usage) error) error {
	// Fail closed on an amount Postgres BIGINT can't hold: storing it would either
	// overflow or silently truncate the window sum, under-counting future checks.
	// The value itself is not surfaced (it may be sensitive); only that it overflows.
	if !amount.IsInt64() {
		return fmt.Errorf("velocity: amount exceeds int64 range for key %s", keyID)
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("velocity: begin tx: %w", err)
	}
	// Rollback is a no-op after a successful commit; the error is ignored to mirror
	// the SQLStore idiom (a benign ErrTxDone on the already-committed path).
	defer func() { _ = tx.Rollback() }()

	q := db.New(tx)

	// Serialize check-then-insert for this key. The advisory lock is xact-scoped, so
	// it releases automatically at commit or rollback — no explicit unlock needed.
	if err := q.AcquireVelocityLock(ctx, advisoryKey(keyID)); err != nil {
		return fmt.Errorf("velocity: acquire lock: %w", err)
	}

	since := now.Add(-window)
	row, err := q.SumVelocityWindow(ctx, db.SumVelocityWindowParams{KeyID: keyID, Since: since})
	if err != nil {
		return fmt.Errorf("velocity: sum window: %w", err)
	}
	usage := policy.Usage{Count: uint64(row.Count), Sum: big.NewInt(row.Sum)}

	if err := decide(usage); err != nil {
		return err // unchanged: ErrVelocityExceeded must survive errors.Is
	}

	if err := q.InsertVelocityEvent(ctx, db.InsertVelocityEventParams{
		KeyID:      keyID,
		Amount:     amount.Int64(),
		OccurredAt: now,
	}); err != nil {
		return fmt.Errorf("velocity: insert event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("velocity: commit tx: %w", err)
	}
	return nil
}

// advisoryKey maps a key-id string to the int64 an xact advisory lock is taken on.
// It fnv-64a-hashes the string and bit-reinterprets the uint64 as int64 (the lock
// space is the full 64-bit range; the sign is immaterial). A collision merely
// over-serializes two distinct keys — safe and only slightly slower — and can
// never under-count, since each key still reads and writes its own rows.
//
// An explicit fnv hash is used rather than Postgres' hashtext(): hashtext's
// algorithm is undocumented and has changed across major versions, so pinning the
// mapping here keeps it stable and reviewable independent of the server.
func advisoryKey(keyID string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(keyID))
	return int64(h.Sum64())
}
