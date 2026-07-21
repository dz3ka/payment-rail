//go:build chaos

package chaos

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/ledger"
)

// TestDBFailover proves the money-movement path is atomic across a genuine
// mid-transaction death of the DB CONNECTION: while a payment tx has applied its
// writes in-tx but not yet committed, the backend serving that connection is
// terminated at the server; the doomed COMMIT then fails, Postgres rolls the tx
// back, and NO partial state survives — and a reconnect (a fresh real store over a
// surviving pool) converges. The fault is driven synchronously — no timers or
// goroutines — so the scenario is deterministic under -race.
//
// The doomed transaction runs on its OWN throwaway pool (a second sql.Open on the
// same DSN), never the harness pool the surviving assertions read through.
//
// Why a server-side kill and not a pool close: closing the *sql.DB pool
// (sql.DB.Close()) does NOT abort the transaction's already-checked-out connection
// — the COMMIT still SUCCEEDS and the write PERSISTS (verified empirically). Closing
// a pool is not a connection death. Only terminating the backend server-side aborts
// an in-flight transaction, so that is what this scenario does. (See the NOTE in
// faults_test.go for why the harness has no pool-close fault mode.)
func TestDBFailover(t *testing.T) {
	dbh := requireChaosDB(t) // the SURVIVING pool: seeders + all assertions read it.
	ctx := context.Background()

	// A dedicated throwaway pool whose one connection we will kill. requireChaosDB
	// already skipped the test if the DSN is unset, so it is non-empty here.
	dsn := os.Getenv("PAYMENT_RAIL_TEST_DSN")
	doomed, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open(doomed): %v", err)
	}
	defer doomed.Close()

	// WP2 — payment create, the connection dies mid-transaction. As in WP1A the
	// payments.Service.Create tx is not injectable, so we compose its body directly
	// (ledger.PostWithin + InsertPayment, faithful to payments.go) and run it on a
	// dedicated connection we terminate at the server before COMMIT.
	t.Run("payment_create_connection_death_before_commit", func(t *testing.T) {
		asset := chaosAsset()
		src := seedFundedAccount(ctx, t, dbh, asset, 1000)
		dst := seedFundedAccount(ctx, t, dbh, asset, 0) // unfunded destination
		const amt = 400
		pid := uuid.New()

		txBody := func(q db.Querier) error {
			je, err := ledger.PostWithin(ctx, q, ledger.Entry{
				Kind:        "payment",
				ExternalRef: pid.String(),
				Asset:       asset,
				Lines: []ledger.Line{
					{AccountID: src, Direction: ledger.Debit, Amount: amt},
					{AccountID: dst, Direction: ledger.Credit, Amount: amt},
				},
			})
			if err != nil {
				return err
			}
			_, err = q.InsertPayment(ctx, db.InsertPaymentParams{
				ID:              pid,
				Status:          "completed",
				Asset:           asset,
				Amount:          amt,
				SourceAccountID: src,
				DestAccountID:   dst,
				JournalEntryID:  je.ID,
			})
			return err
		}

		// Pin one connection so we can identify and kill its exact backend.
		conn, err := doomed.Conn(ctx)
		if err != nil {
			t.Fatalf("acquire doomed connection: %v", err)
		}
		defer conn.Close() // no-op after the backend is gone; safe.

		var backendPID int
		if err := conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&backendPID); err != nil {
			t.Fatalf("read backend pid: %v", err)
		}

		// Apply the payment's writes in-tx on the doomed connection — they succeed,
		// they are just not committed yet.
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			t.Fatalf("begin doomed tx: %v", err)
		}
		if err := txBody(db.New(tx)); err != nil {
			t.Fatalf("apply payment writes: %v", err)
		}

		// The connection dies mid-transaction: terminate its backend from the
		// SURVIVING pool. Postgres aborts the uncommitted tx server-side.
		if _, err := dbh.ExecContext(ctx, `SELECT pg_terminate_backend($1)`, backendPID); err != nil {
			t.Fatalf("terminate doomed backend: %v", err)
		}

		// The doomed COMMIT now fails: the connection is gone.
		if err := tx.Commit(); err == nil {
			t.Fatal("COMMIT over a terminated connection = nil error, want it to fail")
		}

		// No partial state survives on the SURVIVING pool: no payment row, no journal
		// entry for this payment, balances untouched — Postgres rolled the doomed tx
		// back. This is the atomic-rollback proof under connection death.
		if n := paymentCount(ctx, t, dbh, pid); n != 0 {
			t.Fatalf("payment rows after connection death = %d, want 0", n)
		}
		if n := journalEntryCount(ctx, t, dbh, pid.String()); n != 0 {
			t.Fatalf("journal entries after connection death = %d, want 0", n)
		}
		assertBalance(ctx, t, dbh, src, 1000)
		assertBalance(ctx, t, dbh, dst, 0)

		// Reconnect / converge: a FRESH real store over the surviving pool re-runs the
		// SAME tx body and commits, and the ledger lands closed. (As in WP1A this is a
		// bare payment with no settlement, so convergence is the closed-ledger
		// invariant; there is no on-chain settled amount to reconcile against yet.)
		if err := ledger.NewSQLStore(dbh).ExecTx(ctx, txBody); err != nil {
			t.Fatalf("reconnect ExecTx: %v", err)
		}
		assertBalance(ctx, t, dbh, src, 1000-amt)
		assertBalance(ctx, t, dbh, dst, amt)
		if n := paymentCount(ctx, t, dbh, pid); n != 1 {
			t.Fatalf("payment rows after reconnect = %d, want 1", n)
		}
		assertLedgerClosed(ctx, t, dbh, asset)
	})
}
