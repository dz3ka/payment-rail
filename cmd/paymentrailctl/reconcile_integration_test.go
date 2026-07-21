package main

import (
	"context"
	"database/sql"
	"math/big"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/chain"
	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/reconcile"
	"github.com/google/uuid"
)

// Reconcile integration test fixtures. The addresses only need to be stable keys
// for the fake BalanceReader (which never dials a chain) — reconcileReport passes
// them straight through — so any well-formed 0x-hex pair works.
const (
	reconTreasuryAddr = "0x00000000000000000000000000000000000000A1"
	reconTokenAddr    = "0x000000000000000000000000000000000000C0DE"
)

// fakeReconcileBalanceReader is a canned chain.BalanceReader: it returns a chosen
// on-chain balance per (token, holder) so the integration test can drive the whole
// reconcile core over a REAL ledger querier without a chain node. An unset holder
// reads as zero, matching an empty treasury.
type fakeReconcileBalanceReader struct {
	balances map[string]*big.Int // key: token + "|" + holder
}

var _ chain.BalanceReader = (*fakeReconcileBalanceReader)(nil)

func (f *fakeReconcileBalanceReader) BalanceOf(_ context.Context, token, holder string) (*big.Int, error) {
	if b, ok := f.balances[token+"|"+holder]; ok {
		return new(big.Int).Set(b), nil
	}
	return big.NewInt(0), nil
}

// TestReconcile_Integration is the headline end-to-end proof for the reconcile
// command's core: it drives the SAME reconcileReport that runReconcile drives —
// AggregateSettlements + SumNonHouseLiabilities over live Postgres, folded through
// BuildReport — with a real db.New(tx) querier and a fake chain, and exercises the
// three operator-facing outcomes: a clean reconciliation, an injected on-chain
// drift, and an undercollateralized proof-of-reserves.
//
// Each case runs inside its own transaction that is ROLLED BACK (t.Cleanup), and
// keys its ledger rows on a unique-per-run asset symbol, so the seeded data is both
// isolated from other cases and invisible after the test — the shared dev DB is
// left byte-identical and the test is re-runnable.
func TestReconcile_Integration(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// Case 1 — zero-discrepancy happy path. On-chain balance equals finalized +
	// confirmed exactly (discrepancy 0), and liabilities (400) are covered by the
	// 1000 on-chain, so the asset reconciles and is collateralized.
	t.Run("zero discrepancy reconciles clean", func(t *testing.T) {
		tx, asset := beginSeedTx(ctx, t, sqlDB)
		acct := seedReconAccount(ctx, t, tx, asset)
		seedReconSettlement(ctx, t, tx, acct, asset, "finalized", 700)
		seedReconSettlement(ctx, t, tx, acct, asset, "settled", 300)
		seedNonHouseLiability(ctx, t, tx, acct, asset, 400) // owed = 400

		fake := &fakeReconcileBalanceReader{balances: map[string]*big.Int{
			reconTokenAddr + "|" + reconTreasuryAddr: big.NewInt(1000), // == 700 + 300
		}}
		report, err := reconcileReport(ctx, db.New(tx), fake, reconRegistry(asset), reconcilePageSize, fixedNow())
		if err != nil {
			t.Fatalf("reconcileReport: %v", err)
		}

		row := assetRow(t, report, asset)
		if row.Discrepancy.Sign() != 0 {
			t.Fatalf("Discrepancy = %s, want 0", row.Discrepancy)
		}
		if row.Verdict != "OK" {
			t.Fatalf("Verdict = %q, want OK", row.Verdict)
		}
		if row.LiabilitiesMinor != 400 { // proves the SumNonHouseLiabilities negation
			t.Fatalf("LiabilitiesMinor = %d, want 400 (negation of the −400 non-house sum)", row.LiabilitiesMinor)
		}
		if got := reconcileExitCode(assetClean(row)); got != reconcileExitClean {
			t.Fatalf("exit for clean asset = %d, want %d", got, reconcileExitClean)
		}
	})

	// Case 2 — injected drift. The treasury holds 250 MORE than the ledger expects,
	// so the asset shows a non-zero discrepancy and does not reconcile (exit 1).
	t.Run("injected drift is flagged", func(t *testing.T) {
		tx, asset := beginSeedTx(ctx, t, sqlDB)
		acct := seedReconAccount(ctx, t, tx, asset)
		seedReconSettlement(ctx, t, tx, acct, asset, "finalized", 700)
		seedReconSettlement(ctx, t, tx, acct, asset, "settled", 300)

		fake := &fakeReconcileBalanceReader{balances: map[string]*big.Int{
			reconTokenAddr + "|" + reconTreasuryAddr: big.NewInt(1250), // 1000 expected + 250 drift
		}}
		report, err := reconcileReport(ctx, db.New(tx), fake, reconRegistry(asset), reconcilePageSize, fixedNow())
		if err != nil {
			t.Fatalf("reconcileReport: %v", err)
		}

		row := assetRow(t, report, asset)
		if row.Discrepancy.Cmp(big.NewInt(250)) != 0 {
			t.Fatalf("Discrepancy = %s, want 250", row.Discrepancy)
		}
		if assetClean(row) {
			t.Fatalf("asset unexpectedly clean with a 250 drift")
		}
		if got := reconcileExitCode(assetClean(row)); got != reconcileExitDiscrepancy {
			t.Fatalf("exit for drifted asset = %d, want %d", got, reconcileExitDiscrepancy)
		}
	})

	// Case 3 — undercollateralized proof-of-reserves. On-chain matches the ledger
	// (discrepancy 0) but liabilities (5000) exceed reserves (1000), so the verdict
	// is UNDERCOLLATERALIZED even though reconciliation is clean (exit 1). This also
	// asserts the negation end-to-end: a 5000 non-house DEBIT surfaces as +5000 owed.
	t.Run("undercollateralized fails proof of reserves", func(t *testing.T) {
		tx, asset := beginSeedTx(ctx, t, sqlDB)
		acct := seedReconAccount(ctx, t, tx, asset)
		seedReconSettlement(ctx, t, tx, acct, asset, "finalized", 700)
		seedReconSettlement(ctx, t, tx, acct, asset, "settled", 300)
		seedNonHouseLiability(ctx, t, tx, acct, asset, 5000) // owed = 5000 > 1000 reserves

		fake := &fakeReconcileBalanceReader{balances: map[string]*big.Int{
			reconTokenAddr + "|" + reconTreasuryAddr: big.NewInt(1000), // == expected, so discrepancy 0
		}}
		report, err := reconcileReport(ctx, db.New(tx), fake, reconRegistry(asset), reconcilePageSize, fixedNow())
		if err != nil {
			t.Fatalf("reconcileReport: %v", err)
		}

		row := assetRow(t, report, asset)
		if row.Discrepancy.Sign() != 0 {
			t.Fatalf("Discrepancy = %s, want 0 (reconciles, but is undercollateralized)", row.Discrepancy)
		}
		if row.Verdict != "UNDERCOLLATERALIZED" {
			t.Fatalf("Verdict = %q, want UNDERCOLLATERALIZED", row.Verdict)
		}
		if row.LiabilitiesMinor != 5000 {
			t.Fatalf("LiabilitiesMinor = %d, want 5000 (negation of the −5000 non-house sum)", row.LiabilitiesMinor)
		}
		if got := reconcileExitCode(assetClean(row)); got != reconcileExitDiscrepancy {
			t.Fatalf("exit for undercollateralized asset = %d, want %d", got, reconcileExitDiscrepancy)
		}
	})
}

// reconRegistry builds the one-entry treasury registry the fake reads balances for.
func reconRegistry(asset string) reconcile.Registry {
	return reconcile.Registry{Entries: []reconcile.TreasuryEntry{
		{Address: reconTreasuryAddr, Asset: asset, Token: reconTokenAddr},
	}}
}

// fixedNow pins the report timestamp so the test is deterministic; the value is
// irrelevant to the assertions.
func fixedNow() (t time.Time) { return }

// assetClean reports whether one asset both reconciles and is collateralized —
// the per-asset equivalent of Report.Clean, used so the assertions stay robust to
// unrelated assets that other rows/tests may have left in the shared settlements
// table.
func assetClean(a reconcile.AssetReconciliation) bool {
	return a.Discrepancy.Sign() == 0 && a.Verdict == "OK"
}

// assetRow returns the report row for the given asset, failing if it is absent.
func assetRow(t *testing.T, report reconcile.Report, asset string) reconcile.AssetReconciliation {
	t.Helper()
	for _, a := range report.Assets {
		if a.Asset == asset {
			return a
		}
	}
	t.Fatalf("asset %q not found in report (%d rows)", asset, len(report.Assets))
	return reconcile.AssetReconciliation{}
}

// beginSeedTx opens a transaction that is rolled back on cleanup and returns it
// alongside a unique-per-run asset symbol. All ledger writes go through the tx and
// key on this asset, so nothing seeded here survives the test or collides with
// another case (or a concurrent run).
func beginSeedTx(ctx context.Context, t *testing.T, sqlDB *sql.DB) (*sql.Tx, string) {
	t.Helper()
	tx, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx, "RECON-" + uuid.NewString()
}

// seedReconAccount creates one non-house user account for the asset (unique name
// dodges the UNIQUE(name, asset) constraint) and returns its id. It doubles as the
// payments' source/dest and the liability-bearing account.
func seedReconAccount(ctx context.Context, t *testing.T, tx *sql.Tx, asset string) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO accounts (name, kind, asset) VALUES ($1, 'user', $2) RETURNING id`,
		"recon-"+uuid.NewString(), asset,
	).Scan(&id); err != nil {
		t.Fatalf("seed account: %v", err)
	}
	return id
}

// seedReconSettlement seeds one settlement at the given status counting toward the
// ledger's on-chain expectation: a journal entry and a payment (which carries the
// asset/amount the reconcile query joins to) plus the settlement row itself.
func seedReconSettlement(ctx context.Context, t *testing.T, tx *sql.Tx, acct uuid.UUID, asset, status string, amount int64) {
	t.Helper()
	var entryID uuid.UUID
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO journal_entries (kind, external_ref, asset) VALUES ('settlement', $1, $2) RETURNING id`,
		uuid.NewString(), asset,
	).Scan(&entryID); err != nil {
		t.Fatalf("seed journal entry: %v", err)
	}
	var paymentID uuid.UUID
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO payments (status, asset, amount, source_account_id, dest_account_id, journal_entry_id)
		 VALUES ('completed', $1, $2, $3, $3, $4) RETURNING id`,
		asset, amount, acct, entryID,
	).Scan(&paymentID); err != nil {
		t.Fatalf("seed payment: %v", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO settlements (payment_id, tx_hash, status) VALUES ($1, $2, $3)`,
		paymentID, "0x"+uuid.NewString(), status,
	); err != nil {
		t.Fatalf("seed settlement: %v", err)
	}
}

// seedNonHouseLiability books a lone DEBIT of `owed` on the non-house account for
// the asset. SumNonHouseLiabilities computes Σ(credit−debit) = −owed for it, which
// reconcileReport negates back to +owed — so this drives the proof-of-reserves
// liability figure and lets the test assert the negation is correct end to end.
func seedNonHouseLiability(ctx context.Context, t *testing.T, tx *sql.Tx, acct uuid.UUID, asset string, owed int64) {
	t.Helper()
	var entryID uuid.UUID
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO journal_entries (kind, external_ref, asset) VALUES ('opening', $1, $2) RETURNING id`,
		uuid.NewString(), asset,
	).Scan(&entryID); err != nil {
		t.Fatalf("seed liability journal entry: %v", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO entry_lines (entry_id, account_id, direction, amount) VALUES ($1, $2, 'debit', $3)`,
		entryID, acct, owed,
	); err != nil {
		t.Fatalf("seed liability entry line: %v", err)
	}
}
