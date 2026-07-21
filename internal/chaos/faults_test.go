//go:build chaos

package chaos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/ledger"
)

// errInjectedCrash is the sentinel a crash-before-commit fault returns so a
// scenario can assert (errors.Is) that the transaction died where it meant to,
// rather than for some incidental reason.
var errInjectedCrash = errors.New("chaos: injected crash before commit")

// faultMode selects which mid-transaction failure a faultStore injects. There is
// deliberately no "no fault" mode: non-fault phases run the real
// ledger.NewSQLStore directly, so a faultStore always fails.
type faultMode int

const (
	// faultCrashBeforeCommit models process death after the transaction's writes
	// have been applied in-tx but before COMMIT: the work is rolled back and
	// errInjectedCrash is returned. No row survives.
	faultCrashBeforeCommit faultMode = iota
)

// NOTE on connection-death faults: an earlier design had a faultKillPoolBeforeCommit
// mode that called sql.DB.Close() before COMMIT. That does NOT model a connection
// death — Close() shuts the pool but the transaction's already-checked-out
// connection still commits successfully, so the write persists. The DB-failover
// scenario (dbfailover_test.go) therefore kills the backend server-side with
// pg_terminate_backend, which is the only faithful in-process way to abort an
// in-flight transaction. Do not reintroduce a pool-close "fault".

// faultStore is a ledger.Store that runs fn inside a real transaction exactly like
// the production SQLStore — same READ COMMITTED isolation, same rollback-on-error
// discipline — but, once fn SUCCEEDS, injects its faultMode's failure in place of
// the commit. It lets a scenario drive any code path built on the ledger's
// transactor seam (payments, settlement) straight into a mid-transaction crash.
type faultStore struct {
	db   *sql.DB
	mode faultMode
}

// Compile-time proof faultStore is a drop-in for the seam the domain depends on.
var _ ledger.Store = (*faultStore)(nil)

// ExecTx mirrors SQLStore.ExecTx up to the point fn returns nil, then diverges to
// inject the fault. If fn returns an error the tx is rolled back and that error is
// returned unchanged (a benign sql.ErrTxDone from the rollback is ignored, any
// other rollback failure is joined on) — identical to production, so a fault only
// ever changes the SUCCESS path.
func (s *faultStore) ExecTx(ctx context.Context, fn func(q db.Querier) error) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("chaos: begin tx: %w", err)
	}

	if err := fn(db.New(tx)); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return errors.Join(err, fmt.Errorf("chaos: rollback: %w", rbErr))
		}
		return err
	}

	// fn succeeded — inject the fault in place of the commit.
	switch s.mode {
	case faultCrashBeforeCommit:
		// Process death after apply, before commit: discard the work and signal it.
		_ = tx.Rollback()
		return errInjectedCrash
	default:
		panic(fmt.Sprintf("chaos: unknown fault mode %d", s.mode))
	}
}
