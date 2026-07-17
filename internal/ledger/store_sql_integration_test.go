package ledger_test

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/ledger"
)

// TestSQLStoreIntegration exercises the real *sql.DB-backed SQLStore against a
// live Postgres (skipped unless CONDUIT_TEST_DSN is set). It proves the two
// things the transaction seam exists for: a successful PostEntry COMMITs and
// moves balances, and a rejected one (insufficient funds) ROLLs BACK, leaving
// balances untouched.
//
// Seed: balances are always derived, never stored, so to give the source
// account starting money we insert one journal entry plus a single credit
// entry_line for it via raw SQL (the same shape the fake's seedAccount uses).
// From there we post a normal balanced transfer through the Service.
func TestSQLStoreIntegration(t *testing.T) {
	dsn := os.Getenv("CONDUIT_TEST_DSN")
	if dsn == "" {
		t.Skip("set CONDUIT_TEST_DSN to run the SQLStore integration test")
	}

	ctx := context.Background()
	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	q := db.New(sqlDB)
	store := ledger.NewSQLStore(sqlDB)
	svc := ledger.NewService(store, discardLogger())

	// Two fresh accounts in the same asset. Unique (uuid) names dodge the
	// UNIQUE(name, asset) constraint across repeated runs.
	const asset = "USD"
	src, err := q.CreateAccount(ctx, db.CreateAccountParams{Name: "src-" + uuid.NewString(), Kind: "user", Asset: asset})
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	dst, err := q.CreateAccount(ctx, db.CreateAccountParams{Name: "dst-" + uuid.NewString(), Kind: "user", Asset: asset})
	if err != nil {
		t.Fatalf("create dest: %v", err)
	}

	// A brand-new account has a derived balance of 0.
	if bal := mustBalance(t, ctx, q, dst.ID); bal != 0 {
		t.Fatalf("fresh dest balance = %d, want 0", bal)
	}

	// Seed the source with 1000 via a raw opening credit (test-only).
	const opening = 1000
	seedOpeningBalance(t, ctx, sqlDB, asset, src.ID, opening)
	if bal := mustBalance(t, ctx, q, src.ID); bal != opening {
		t.Fatalf("seeded source balance = %d, want %d", bal, opening)
	}

	// Happy path: transfer 300 src -> dst. Debits lower, credits raise.
	const transfer = 300
	if _, err := svc.PostEntry(ctx, ledger.Entry{
		Kind:        "transfer",
		ExternalRef: uuid.NewString(),
		Asset:       asset,
		Lines: []ledger.Line{
			{AccountID: src.ID, Direction: ledger.Debit, Amount: transfer},
			{AccountID: dst.ID, Direction: ledger.Credit, Amount: transfer},
		},
	}); err != nil {
		t.Fatalf("PostEntry(transfer): %v", err)
	}
	if bal := mustBalance(t, ctx, q, src.ID); bal != opening-transfer {
		t.Errorf("source balance after transfer = %d, want %d", bal, opening-transfer)
	}
	if bal := mustBalance(t, ctx, q, dst.ID); bal != transfer {
		t.Errorf("dest balance after transfer = %d, want %d", bal, transfer)
	}

	// Rollback path: over-draw the source. PostEntry must reject with
	// ErrInsufficientFunds AND leave balances exactly as they were — proving the
	// SQLStore rolled the transaction back rather than committing a partial write.
	srcBefore := mustBalance(t, ctx, q, src.ID)
	dstBefore := mustBalance(t, ctx, q, dst.ID)
	_, err = svc.PostEntry(ctx, ledger.Entry{
		Kind:        "transfer",
		ExternalRef: uuid.NewString(),
		Asset:       asset,
		Lines: []ledger.Line{
			{AccountID: src.ID, Direction: ledger.Debit, Amount: opening * 10},
			{AccountID: dst.ID, Direction: ledger.Credit, Amount: opening * 10},
		},
	})
	if !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("over-draw error = %v, want ErrInsufficientFunds", err)
	}
	if bal := mustBalance(t, ctx, q, src.ID); bal != srcBefore {
		t.Errorf("source balance after rejected posting = %d, want unchanged %d", bal, srcBefore)
	}
	if bal := mustBalance(t, ctx, q, dst.ID); bal != dstBefore {
		t.Errorf("dest balance after rejected posting = %d, want unchanged %d", bal, dstBefore)
	}
}

// discardLogger silences the Service's structured output during the test.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func mustBalance(t *testing.T, ctx context.Context, q *db.Queries, id uuid.UUID) int64 {
	t.Helper()
	bal, err := q.GetAccountBalance(ctx, id)
	if err != nil {
		t.Fatalf("GetAccountBalance(%s): %v", id, err)
	}
	return bal
}

// seedOpeningBalance gives account id a starting balance by writing one journal
// entry and a single credit line for it, straight through the pool. This is a
// test-only shortcut for "money enters the system"; production seeds go through
// the payments/minting flow, not raw inserts.
func seedOpeningBalance(t *testing.T, ctx context.Context, sqlDB *sql.DB, asset string, id uuid.UUID, amount int64) {
	t.Helper()
	var entryID uuid.UUID
	if err := sqlDB.QueryRowContext(ctx,
		`INSERT INTO journal_entries (kind, external_ref, asset) VALUES ('opening', $1, $2) RETURNING id`,
		uuid.NewString(), asset,
	).Scan(&entryID); err != nil {
		t.Fatalf("seed journal entry: %v", err)
	}
	if _, err := sqlDB.ExecContext(ctx,
		`INSERT INTO entry_lines (entry_id, account_id, direction, amount) VALUES ($1, $2, 'credit', $3)`,
		entryID, id, amount,
	); err != nil {
		t.Fatalf("seed entry line: %v", err)
	}
}
