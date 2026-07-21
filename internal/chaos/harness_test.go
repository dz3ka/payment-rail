//go:build chaos

// Package chaos holds Payment Rail's fault-injection ("chaos") test suite: it
// drives the real money-movement code paths through injected mid-transaction
// failures and asserts the system always lands in a converged end state — no
// partial writes, a zero-sum ledger, and a clean reconciliation. Everything here
// is TEST-ONLY (guarded by the `chaos` build tag) and runs against the dev-stack
// Postgres named by PAYMENT_RAIL_TEST_DSN; the suite is skipped when that is unset.
//
// This file is the shared harness: a DB gate, asset-isolated seeders that commit
// real rows, and the convergence assertions the scenarios gate on. Every test
// keys its rows on a unique-per-run asset ("CHAOS-"+uuid) so runs never collide
// and best-effort cleanups leave the shared DB effectively untouched.
package chaos

import (
	"context"
	"database/sql"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/ledger"
	"github.com/dz3ka/payment-rail/internal/reconcile"
)

// reconcilePageSize is the keyset page size assertReconcileClean walks the whole
// settlements table with. It only needs to terminate; any value works.
const reconcilePageSize = 500

// houseAccountName mirrors settlement.houseAccountName (unexported there): the
// well-known name the settlement Sink resolves the per-asset clearing account by.
const houseAccountName = "onchain_settlement"

// requireChaosDB opens the dev-stack pool named by PAYMENT_RAIL_TEST_DSN, or skips
// the test when it is unset — the same gate the integration tests use. The pool is
// closed on cleanup. Mirrors cmd/api/api_integration_test.go:openTestDB.
func requireChaosDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("PAYMENT_RAIL_TEST_DSN")
	if dsn == "" {
		t.Skip("set PAYMENT_RAIL_TEST_DSN to run chaos tests")
	}
	dbh, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { dbh.Close() })
	if err := dbh.PingContext(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return dbh
}

// chaosAsset returns a unique per-call asset symbol so each test's ledger rows are
// isolated from every other test (and every other run) on the shared dev DB.
func chaosAsset() string {
	return "CHAOS-" + uuid.NewString()
}

// registerAssetCleanup schedules a best-effort delete of every row this suite
// could have written for asset, in FK-safe order (lines → settlements → payments →
// journal entries → accounts). Errors are ignored: cleanup is a courtesy, and the
// unique-per-run asset means any leftover is inert.
func registerAssetCleanup(t *testing.T, dbh *sql.DB, asset string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		stmts := []string{
			`DELETE FROM entry_lines el USING journal_entries je WHERE el.entry_id = je.id AND je.asset = $1`,
			`DELETE FROM settlements s USING payments p WHERE s.payment_id = p.id AND p.asset = $1`,
			`DELETE FROM payments WHERE asset = $1`,
			`DELETE FROM journal_entries WHERE asset = $1`,
			`DELETE FROM accounts WHERE asset = $1`,
		}
		for _, stmt := range stmts {
			_, _ = dbh.ExecContext(ctx, stmt) // best-effort
		}
	})
}

// seedFundedAccount creates an account for asset (uuid-suffixed name to dodge the
// UNIQUE(name, asset) constraint) and, when opening > 0, gives it that starting
// balance through one 'opening' journal entry plus a lone credit line — the
// test-only "money enters the system" shortcut, mirrored from loadtest.go:286
// seedAccounts / cmd/api seedFunds (balances are always derived, never stored).
// opening <= 0 skips the credit (entry_lines CHECK (amount > 0) forbids a zero
// line), so this doubles as the unfunded-destination seeder.
func seedFundedAccount(ctx context.Context, t *testing.T, dbh *sql.DB, asset string, opening int64) uuid.UUID {
	t.Helper()
	registerAssetCleanup(t, dbh, asset)

	acct, err := db.New(dbh).CreateAccount(ctx, db.CreateAccountParams{
		Name:  "chaos-acct-" + uuid.NewString(),
		Kind:  "user",
		Asset: asset,
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	if opening <= 0 {
		return acct.ID
	}

	// Opening credit via raw SQL, exactly like seedAccounts: one 'opening' journal
	// entry keyed by a fresh external_ref, then a single credit line.
	var entryID uuid.UUID
	if err := dbh.QueryRowContext(ctx,
		`INSERT INTO journal_entries (kind, external_ref, asset) VALUES ('opening', $1, $2) RETURNING id`,
		uuid.NewString(), asset,
	).Scan(&entryID); err != nil {
		t.Fatalf("seed opening entry: %v", err)
	}
	if _, err := dbh.ExecContext(ctx,
		`INSERT INTO entry_lines (entry_id, account_id, direction, amount) VALUES ($1, $2, 'credit', $3)`,
		entryID, acct.ID, opening,
	); err != nil {
		t.Fatalf("seed opening line: %v", err)
	}
	return acct.ID
}

// seedHouseAccount creates the per-asset onchain_settlement clearing account the
// settlement Sink balances every settle/reverse against. Migration 0003 seeds this
// house account only for USDC; because the suite uses a unique asset for isolation
// we must create it ourselves, with the SAME (name, kind) the Sink resolves it by
// via GetAccountByNameAndAsset (and that SumNonHouseLiabilities excludes by kind).
func seedHouseAccount(ctx context.Context, t *testing.T, dbh *sql.DB, asset string) uuid.UUID {
	t.Helper()
	registerAssetCleanup(t, dbh, asset)

	acct, err := db.New(dbh).CreateAccount(ctx, db.CreateAccountParams{
		Name:  houseAccountName,
		Kind:  houseAccountName,
		Asset: asset,
	})
	if err != nil {
		t.Fatalf("create house account: %v", err)
	}
	return acct.ID
}

// seedPaymentAndLink commits a completed payment moving amt from src to dst and
// links txHash to it, reproducing payments.Create's committed footprint minus the
// outbox/audit writes (not needed to seed a settleable payment). It posts the
// balanced payment entry (debit src, credit dst) so dst carries its provisional
// credit, inserts the payments row keyed on that journal entry, then inserts the
// settlement link with the same ON CONFLICT(tx_hash) DO NOTHING shape as
// settlement.Recorder.Link. Returns the payment id.
func seedPaymentAndLink(ctx context.Context, t *testing.T, dbh *sql.DB, asset string, src, dst uuid.UUID, amt int64, txHash string) uuid.UUID {
	t.Helper()
	registerAssetCleanup(t, dbh, asset)

	pid := uuid.New()
	err := ledger.NewSQLStore(dbh).ExecTx(ctx, func(q db.Querier) error {
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
	})
	if err != nil {
		t.Fatalf("seed payment: %v", err)
	}

	if _, err := db.New(dbh).InsertSettlement(ctx, db.InsertSettlementParams{
		PaymentID: pid,
		TxHash:    txHash,
	}); err != nil {
		t.Fatalf("seed settlement link: %v", err)
	}
	return pid
}

// assertLedgerClosed asserts the closed-system invariant for asset: every real
// (non-'opening') journal entry balances, so Σ(credit − debit) over asset's entry
// lines nets to zero. The 'opening' kind is the deliberate counterparty-less
// value-injection shortcut the seeders use (a lone credit line), so it is excluded
// — otherwise a funded source would always show a nonzero sum equal to the amount
// injected, and no scenario could ever converge.
func assertLedgerClosed(ctx context.Context, t *testing.T, dbh *sql.DB, asset string) {
	t.Helper()
	var net int64
	if err := dbh.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(CASE WHEN el.direction = 'credit' THEN el.amount ELSE -el.amount END), 0)::bigint
		FROM entry_lines el
		JOIN journal_entries je ON je.id = el.entry_id
		WHERE je.asset = $1 AND je.kind <> 'opening'`,
		asset,
	).Scan(&net); err != nil {
		t.Fatalf("assertLedgerClosed query: %v", err)
	}
	if net != 0 {
		t.Fatalf("ledger not closed for asset %s: Σ(credit−debit) over non-opening entries = %d, want 0", asset, net)
	}
}

// assertBalance asserts acct's derived balance equals want (mirrors
// db/query/ledger.sql GetAccountBalance: COALESCE(SUM(credit − debit), 0)).
func assertBalance(ctx context.Context, t *testing.T, dbh *sql.DB, acct uuid.UUID, want int64) {
	t.Helper()
	got, err := db.New(dbh).GetAccountBalance(ctx, acct)
	if err != nil {
		t.Fatalf("GetAccountBalance(%s): %v", acct, err)
	}
	if got != want {
		t.Fatalf("balance of %s = %d, want %d", acct, got, want)
	}
}

// assertSettleEntryCount asserts exactly want settlement.settle journal entries
// exist for paymentID. The Sink keys each settle's external_ref as
// "settle:<paymentID>:<blockHash>", so a LIKE 'settle:<paymentID>:%' scopes the
// count to this payment regardless of which block confirmed it — the "settle
// exactly once" idempotency probe.
func assertSettleEntryCount(ctx context.Context, t *testing.T, dbh *sql.DB, paymentID uuid.UUID, want int) {
	t.Helper()
	var got int
	if err := dbh.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM journal_entries
		WHERE kind = 'settlement.settle' AND external_ref LIKE $1`,
		"settle:"+paymentID.String()+":%",
	).Scan(&got); err != nil {
		t.Fatalf("assertSettleEntryCount query: %v", err)
	}
	if got != want {
		t.Fatalf("settlement.settle entries for payment %s = %d, want %d", paymentID, got, want)
	}
}

// assertReconcileClean drives the real reconcile core for asset — the same
// AggregateSettlements + SumNonHouseLiabilities → BuildReport path runReconcile
// uses — with actualOnChain (in minor units) standing in for the treasury balance,
// and asserts the asset both reconciles (Discrepancy == 0) and the report is Clean.
// The report inputs are scoped to asset alone so an unrelated asset's rows on the
// shared DB cannot dirty the verdict.
func assertReconcileClean(ctx context.Context, t *testing.T, dbh *sql.DB, asset string, actualOnChain int64) {
	t.Helper()
	q := db.New(dbh)

	allSums, err := reconcile.AggregateSettlements(ctx, q, reconcilePageSize)
	if err != nil {
		t.Fatalf("AggregateSettlements: %v", err)
	}
	sums := map[string]reconcile.AssetSums{asset: allSums[asset]}

	// SumNonHouseLiabilities returns the signed non-house balance sum; the
	// proof-of-reserves check wants liabilities as a positive owed value, so negate
	// (exactly as reconcile.go:169).
	liabSum, err := q.SumNonHouseLiabilities(ctx, asset)
	if err != nil {
		t.Fatalf("SumNonHouseLiabilities: %v", err)
	}
	liabilities := map[string]int64{asset: -liabSum}

	actuals := map[string][]reconcile.AddressBalance{
		asset: {{Address: "chaos-treasury", Actual: big.NewInt(actualOnChain)}},
	}

	rep := reconcile.BuildReport(time.Now(), sums, liabilities, actuals)
	for _, a := range rep.Assets {
		if a.Asset == asset && a.Discrepancy.Sign() != 0 {
			t.Fatalf("reconcile discrepancy for asset %s = %s, want 0", asset, a.Discrepancy.String())
		}
	}
	if !rep.Clean {
		t.Fatalf("reconcile report for asset %s not Clean", asset)
	}
}

// assertConverged is the scenario end-state gate: the ledger is closed AND the
// asset reconciles cleanly against actualOnChain (the settled minor amount).
func assertConverged(ctx context.Context, t *testing.T, dbh *sql.DB, asset string, actualOnChain int64) {
	t.Helper()
	assertLedgerClosed(ctx, t, dbh, asset)
	assertReconcileClean(ctx, t, dbh, asset, actualOnChain)
}

// TestHarness_SelfCheck proves the harness itself before the fault scenarios lean
// on it: a funded source reads back its opening balance, and the ledger is closed
// (the opening injection aside, there are no unbalanced entries).
func TestHarness_SelfCheck(t *testing.T) {
	dbh := requireChaosDB(t)
	ctx := context.Background()

	asset := chaosAsset()
	src := seedFundedAccount(ctx, t, dbh, asset, 1000)

	assertBalance(ctx, t, dbh, src, 1000)
	assertLedgerClosed(ctx, t, dbh, asset)
}
