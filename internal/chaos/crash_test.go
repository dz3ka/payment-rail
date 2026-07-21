//go:build chaos

package chaos

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/chain"
	"github.com/dz3ka/payment-rail/internal/chain/evm"
	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/ledger"
	"github.com/dz3ka/payment-rail/internal/settlement"
)

// quietLogger returns a logger that discards output, so scenario runs stay quiet
// even though the code under test logs each outcome.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// paymentCount returns how many payment rows exist for id (0 or 1) — a direct
// count so a crash's "no partial state" claim is checked against reality.
func paymentCount(ctx context.Context, t *testing.T, dbh *sql.DB, id uuid.UUID) int {
	t.Helper()
	var n int
	if err := dbh.QueryRowContext(ctx, `SELECT COUNT(*) FROM payments WHERE id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("paymentCount query: %v", err)
	}
	return n
}

// journalEntryCount returns how many journal entries exist for external_ref
// (the payment id is the payment entry's external reference).
func journalEntryCount(ctx context.Context, t *testing.T, dbh *sql.DB, externalRef string) int {
	t.Helper()
	var n int
	if err := dbh.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_entries WHERE external_ref = $1`, externalRef).Scan(&n); err != nil {
		t.Fatalf("journalEntryCount query: %v", err)
	}
	return n
}

// settlementStatus returns the current status of the settlement linked to txHash.
func settlementStatus(ctx context.Context, t *testing.T, dbh *sql.DB, txHash string) string {
	t.Helper()
	var status string
	if err := dbh.QueryRowContext(ctx, `SELECT status FROM settlements WHERE tx_hash = $1`, txHash).Scan(&status); err != nil {
		t.Fatalf("settlementStatus query: %v", err)
	}
	return status
}

// TestCrashTolerance proves the money-movement paths are crash-atomic: a failure
// after a transaction's writes are applied but before COMMIT leaves NO partial
// state, and a retry (the "restart") converges. Both subtests drive faults as
// synchronous returns and settlement via a direct Sink.OnStatus call — no timers,
// goroutines, or sleeps — so they are deterministic under -race.
func TestCrashTolerance(t *testing.T) {
	dbh := requireChaosDB(t)
	ctx := context.Background()

	// WP1A — payment create, crash before commit. payments.Service.Create builds
	// its own SQLStore internally and is not injectable, so we compose its tx body
	// directly (ledger.PostWithin + InsertPayment, faithful to payments.go:103-140)
	// and run it through a crash faultStore. The outbox.Emit/audit.Append writes are
	// omitted: they are irrelevant to whether the money movement + payment row are
	// atomic, which is all this scenario proves.
	t.Run("payment_create_crash_before_commit", func(t *testing.T) {
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

		// Crash after apply, before commit.
		crash := &faultStore{db: dbh, mode: faultCrashBeforeCommit}
		if err := crash.ExecTx(ctx, txBody); !errors.Is(err, errInjectedCrash) {
			t.Fatalf("crash ExecTx: got %v, want errInjectedCrash", err)
		}

		// No partial state survives on the real pool: no payment row, no journal
		// entry for this payment, balances untouched.
		if n := paymentCount(ctx, t, dbh, pid); n != 0 {
			t.Fatalf("payment rows after crash = %d, want 0", n)
		}
		if n := journalEntryCount(ctx, t, dbh, pid.String()); n != 0 {
			t.Fatalf("journal entries after crash = %d, want 0", n)
		}
		assertBalance(ctx, t, dbh, src, 1000)
		assertBalance(ctx, t, dbh, dst, 0)

		// Restart / retry: the SAME tx body through the real store commits.
		if err := ledger.NewSQLStore(dbh).ExecTx(ctx, txBody); err != nil {
			t.Fatalf("retry ExecTx: %v", err)
		}
		assertBalance(ctx, t, dbh, src, 1000-amt)
		assertBalance(ctx, t, dbh, dst, amt)
		if n := paymentCount(ctx, t, dbh, pid); n != 1 {
			t.Fatalf("payment rows after retry = %d, want 1", n)
		}
		assertLedgerClosed(ctx, t, dbh, asset)
	})

	// WP1B — settlement downstream crash. settlement.NewSink accepts a ledger.Store,
	// so we inject the crash faultStore directly. A confirmed status crashes the
	// settle tx (settlement still pending, no settle entry, provisional credit
	// intact); redelivering the SAME status through a real Sink settles it exactly
	// once and the asset converges — proving at-least-once redelivery + idempotent
	// settle.
	t.Run("settlement_downstream_crash", func(t *testing.T) {
		asset := chaosAsset()
		src := seedFundedAccount(ctx, t, dbh, asset, 1000)
		dst := seedFundedAccount(ctx, t, dbh, asset, 0)
		seedHouseAccount(ctx, t, dbh, asset)
		const amt = 600
		txHash := "0x" + strings.ReplaceAll(uuid.NewString(), "-", "")

		pid := seedPaymentAndLink(ctx, t, dbh, asset, src, dst, amt, txHash)
		// The payment moved the provisional credit to dst; settlement still pending.
		assertBalance(ctx, t, dbh, dst, amt)

		confirmed := evm.Status{
			TxHash:      chain.TxHash(txHash),
			Phase:       evm.PhaseConfirmed,
			BlockHash:   "0x" + strings.Repeat("ab", 32),
			BlockNumber: 100,
		}

		// Crash the settle tx.
		crashSink := settlement.NewSink(&faultStore{db: dbh, mode: faultCrashBeforeCommit}, quietLogger())
		if err := crashSink.OnStatus(ctx, confirmed); !errors.Is(err, errInjectedCrash) {
			t.Fatalf("crash OnStatus: got %v, want errInjectedCrash", err)
		}
		// Nothing moved: still pending, no settle entry, dst keeps its credit.
		if got := settlementStatus(ctx, t, dbh, txHash); got != "pending" {
			t.Fatalf("settlement status after crash = %q, want pending", got)
		}
		assertSettleEntryCount(ctx, t, dbh, pid, 0)
		assertBalance(ctx, t, dbh, dst, amt)

		// Redeliver the SAME status through a real Sink: settles exactly once.
		realSink := settlement.NewSink(ledger.NewSQLStore(dbh), quietLogger())
		if err := realSink.OnStatus(ctx, confirmed); err != nil {
			t.Fatalf("redeliver OnStatus: %v", err)
		}
		if got := settlementStatus(ctx, t, dbh, txHash); got != "settled" {
			t.Fatalf("settlement status after redeliver = %q, want settled", got)
		}
		assertSettleEntryCount(ctx, t, dbh, pid, 1)
		assertBalance(ctx, t, dbh, dst, 0)
		// End state: ledger closed and the asset reconciles against the on-chain
		// settled amount.
		assertConverged(ctx, t, dbh, asset, amt)
	})
}
