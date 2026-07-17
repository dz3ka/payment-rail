package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/dz3ka/payment-rail/internal/db"
)

// SQLStore is the production Store: a thin transactor over a *sql.DB. It owns
// only the transaction boundary — BEGIN/COMMIT/ROLLBACK — and delegates all
// ledger logic to the fn it runs. The domain (PostEntry / PostWithin) never sees
// *sql.DB, so the same orchestration runs here and against the in-memory fake.
type SQLStore struct {
	db *sql.DB
}

// Compile-time proof that SQLStore satisfies the seam the domain depends on.
var _ Store = (*SQLStore)(nil)

// NewSQLStore wraps an already-open *sql.DB. Pool lifecycle (Open/Close/Ping)
// belongs to the caller that constructed the handle.
func NewSQLStore(db *sql.DB) *SQLStore {
	return &SQLStore{db: db}
}

// ExecTx runs fn inside a single READ COMMITTED transaction, binding a Querier
// to that tx. Per ADR-0004 the concurrency safety comes from READ COMMITTED plus
// the ordered SELECT ... FOR UPDATE already issued by GetAccountsForUpdate, not
// from a higher isolation level.
//
// If fn returns an error the tx is rolled back and that error is returned
// unchanged (so callers keep matching the ledger sentinels with errors.Is); a
// benign sql.ErrTxDone from the rollback is ignored, and any other rollback
// failure is joined onto the original so neither is lost. If fn succeeds the tx
// is committed and only a commit failure is surfaced (wrapped with %w).
func (s *SQLStore) ExecTx(ctx context.Context, fn func(q db.Querier) error) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("ledger: begin tx: %w", err)
	}

	if err := fn(db.New(tx)); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return errors.Join(err, fmt.Errorf("ledger: rollback: %w", rbErr))
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("ledger: commit tx: %w", err)
	}
	return nil
}
