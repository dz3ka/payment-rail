package payments_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/ledger"
	"github.com/dz3ka/payment-rail/internal/payments"
)

// These tests exercise the payments Service and IdempotencyStore against a live
// Postgres, skipped unless CONDUIT_TEST_DSN is set (so `go test ./...` stays
// green without a database). They run on the shared dev DB, so every fixture
// uses fresh uuids and asserts on its own rows rather than global table state.

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("CONDUIT_TEST_DSN")
	if dsn == "" {
		t.Skip("set CONDUIT_TEST_DSN to run the payments integration tests")
	}
	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	if err := sqlDB.PingContext(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return sqlDB
}

// newAccount creates a fresh account; the uuid-suffixed name dodges the
// UNIQUE(name, asset) constraint across repeated runs.
func newAccount(ctx context.Context, t *testing.T, q *db.Queries, asset string) db.Account {
	t.Helper()
	a, err := q.CreateAccount(ctx, db.CreateAccountParams{
		Name:  "pay-" + uuid.NewString(),
		Kind:  "user",
		Asset: asset,
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	return a
}

// seedFunds gives id a starting balance via one opening journal entry and a lone
// credit line — the same test-only "money enters the system" shortcut the ledger
// integration test uses. Balances are always derived, never stored.
func seedFunds(ctx context.Context, t *testing.T, sqlDB *sql.DB, asset string, id uuid.UUID, amount int64) {
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

func mustBalance(ctx context.Context, t *testing.T, q *db.Queries, id uuid.UUID) int64 {
	t.Helper()
	bal, err := q.GetAccountBalance(ctx, id)
	if err != nil {
		t.Fatalf("GetAccountBalance(%s): %v", id, err)
	}
	return bal
}

func TestCreateThenGet(t *testing.T) {
	sqlDB := openTestDB(t)
	ctx := context.Background()
	q := db.New(sqlDB)
	svc := payments.NewService(sqlDB, nil)

	const asset = "USD"
	src := newAccount(ctx, t, q, asset)
	dst := newAccount(ctx, t, q, asset)
	seedFunds(ctx, t, sqlDB, asset, src.ID, 1000)

	created, err := svc.Create(ctx, payments.CreateInput{
		SourceAccountID: src.ID, DestAccountID: dst.ID, Asset: asset, Amount: 300,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.Status != "completed" || created.Amount != 300 {
		t.Fatalf("created payment = %+v, want completed/300", created)
	}

	got, err := svc.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != created.ID || got.JournalEntryID != created.JournalEntryID {
		t.Fatalf("Get returned %+v, want %+v", got, created)
	}
	if bal := mustBalance(ctx, t, q, src.ID); bal != 700 {
		t.Errorf("source balance = %d, want 700", bal)
	}
	if bal := mustBalance(ctx, t, q, dst.ID); bal != 300 {
		t.Errorf("dest balance = %d, want 300", bal)
	}
}

func TestGetUnknownPaymentIsNotFound(t *testing.T) {
	sqlDB := openTestDB(t)
	svc := payments.NewService(sqlDB, nil)

	_, err := svc.Get(context.Background(), uuid.New())
	if !errors.Is(err, payments.ErrPaymentNotFound) {
		t.Fatalf("Get(unknown) error = %v, want ErrPaymentNotFound", err)
	}
}

func TestCreateInsufficientFunds(t *testing.T) {
	sqlDB := openTestDB(t)
	ctx := context.Background()
	q := db.New(sqlDB)
	svc := payments.NewService(sqlDB, nil)

	const asset = "USD"
	src := newAccount(ctx, t, q, asset)
	dst := newAccount(ctx, t, q, asset)
	seedFunds(ctx, t, sqlDB, asset, src.ID, 100)

	_, err := svc.Create(ctx, payments.CreateInput{
		SourceAccountID: src.ID, DestAccountID: dst.ID, Asset: asset, Amount: 500,
	})
	if !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("Create(overdraw) error = %v, want ErrInsufficientFunds", err)
	}
	// The whole transaction rolled back: source keeps its money, nothing moved.
	if bal := mustBalance(ctx, t, q, src.ID); bal != 100 {
		t.Errorf("source balance after reject = %d, want unchanged 100", bal)
	}
	if bal := mustBalance(ctx, t, q, dst.ID); bal != 0 {
		t.Errorf("dest balance after reject = %d, want 0", bal)
	}
}

func TestCancelReversesBalancesAndStatus(t *testing.T) {
	sqlDB := openTestDB(t)
	ctx := context.Background()
	q := db.New(sqlDB)
	svc := payments.NewService(sqlDB, nil)

	const asset = "USD"
	src := newAccount(ctx, t, q, asset)
	dst := newAccount(ctx, t, q, asset)
	seedFunds(ctx, t, sqlDB, asset, src.ID, 1000)

	created, err := svc.Create(ctx, payments.CreateInput{
		SourceAccountID: src.ID, DestAccountID: dst.ID, Asset: asset, Amount: 400,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	canceled, err := svc.Cancel(ctx, created.ID)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if canceled.Status != "canceled" {
		t.Errorf("status after cancel = %q, want canceled", canceled.Status)
	}
	if !canceled.ReversalEntryID.Valid {
		t.Errorf("reversal_entry_id not set after cancel")
	}
	if canceled.ReversalEntryID.UUID == created.JournalEntryID {
		t.Errorf("reversal entry id equals original entry id; want a distinct reversing entry")
	}
	// The reversal (debit dest, credit source) returns the money exactly.
	if bal := mustBalance(ctx, t, q, src.ID); bal != 1000 {
		t.Errorf("source balance after cancel = %d, want restored 1000", bal)
	}
	if bal := mustBalance(ctx, t, q, dst.ID); bal != 0 {
		t.Errorf("dest balance after cancel = %d, want 0", bal)
	}
}

func TestCancelAlreadyCanceledIsNotCancelable(t *testing.T) {
	sqlDB := openTestDB(t)
	ctx := context.Background()
	q := db.New(sqlDB)
	svc := payments.NewService(sqlDB, nil)

	const asset = "USD"
	src := newAccount(ctx, t, q, asset)
	dst := newAccount(ctx, t, q, asset)
	seedFunds(ctx, t, sqlDB, asset, src.ID, 1000)

	created, err := svc.Create(ctx, payments.CreateInput{
		SourceAccountID: src.ID, DestAccountID: dst.ID, Asset: asset, Amount: 250,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := svc.Cancel(ctx, created.ID); err != nil {
		t.Fatalf("first Cancel: %v", err)
	}

	_, err = svc.Cancel(ctx, created.ID)
	if !errors.Is(err, payments.ErrPaymentNotCancelable) {
		t.Fatalf("second Cancel error = %v, want ErrPaymentNotCancelable", err)
	}
}

func TestListKeysetPaginationRoundTrip(t *testing.T) {
	sqlDB := openTestDB(t)
	ctx := context.Background()
	q := db.New(sqlDB)
	svc := payments.NewService(sqlDB, nil)

	const asset = "USD"
	src := newAccount(ctx, t, q, asset)
	dst := newAccount(ctx, t, q, asset)
	seedFunds(ctx, t, sqlDB, asset, src.ID, 10000)

	// Insert N payments; track their ids. They are the newest rows in the table,
	// so paging newest-first surfaces them first.
	const n = 5
	mine := make(map[uuid.UUID]bool, n)
	var order []uuid.UUID // insertion order (oldest -> newest)
	for range n {
		p, err := svc.Create(ctx, payments.CreateInput{
			SourceAccountID: src.ID, DestAccountID: dst.ID, Asset: asset, Amount: 10,
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		mine[p.ID] = true
		order = append(order, p.ID)
		// A tiny gap keeps created_at strictly increasing so newest-first order
		// is unambiguous even at coarse clock resolution.
		time.Sleep(2 * time.Millisecond)
	}

	// Page through with a limit smaller than N, collecting our ids in the order
	// the API returns them, following the cursor until we've seen all of ours.
	var collected []uuid.UUID
	seen := make(map[uuid.UUID]bool)
	var cursor *payments.Cursor
	for len(seen) < n {
		page, next, err := svc.List(ctx, 2, cursor)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(page) == 0 {
			t.Fatalf("List exhausted before finding all %d payments (found %d)", n, len(seen))
		}
		for _, p := range page {
			if p.ID == uuid.Nil {
				continue
			}
			if !mine[p.ID] {
				continue // skip rows other tests left behind
			}
			if seen[p.ID] {
				t.Fatalf("duplicate payment %s across pages", p.ID)
			}
			seen[p.ID] = true
			collected = append(collected, p.ID)
		}
		if next == nil {
			break
		}
		cursor = next
	}

	if len(collected) != n {
		t.Fatalf("collected %d of my payments, want %d", len(collected), n)
	}
	// Newest-first: the API order must be the reverse of insertion order.
	for i, id := range collected {
		want := order[n-1-i]
		if id != want {
			t.Fatalf("page order[%d] = %s, want %s (newest-first)", i, id, want)
		}
	}
}

func TestIdempotencyBeginCompleteAndSweep(t *testing.T) {
	sqlDB := openTestDB(t)
	ctx := context.Background()
	store := payments.NewIdempotencyStore(sqlDB)

	key := "idem-" + uuid.NewString()
	hashA := []byte("request-hash-A")
	hashB := []byte("request-hash-B")

	// First claim is fresh.
	first, err := store.Begin(ctx, key, hashA)
	if err != nil {
		t.Fatalf("Begin(fresh): %v", err)
	}
	if !first.Fresh {
		t.Fatalf("first Begin Fresh = false, want true")
	}

	// A second Begin on the same key conflicts and returns the stored row, which
	// keeps the original hash and its in_flight status.
	second, err := store.Begin(ctx, key, hashB)
	if err != nil {
		t.Fatalf("Begin(conflict): %v", err)
	}
	if second.Fresh {
		t.Fatalf("second Begin Fresh = true, want false")
	}
	if string(second.Existing.RequestHash) != string(hashA) {
		t.Errorf("existing hash = %q, want original %q", second.Existing.RequestHash, hashA)
	}
	if second.Existing.Status != "in_flight" {
		t.Errorf("existing status = %q, want in_flight", second.Existing.Status)
	}

	// Complete caches the response; Get then shows it verbatim.
	body := []byte(`{"id":"abc","status":"completed"}`)
	pid := uuid.New()
	if err := store.Complete(ctx, key, 201, body, pid); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	stored, err := db.New(sqlDB).GetIdempotencyKey(ctx, key)
	if err != nil {
		t.Fatalf("GetIdempotencyKey: %v", err)
	}
	if stored.Status != "completed" {
		t.Errorf("status after Complete = %q, want completed", stored.Status)
	}
	if string(stored.ResponseBody) != string(body) {
		t.Errorf("response body = %q, want %q", stored.ResponseBody, body)
	}
	if !stored.ResponseStatus.Valid || stored.ResponseStatus.Int32 != 201 {
		t.Errorf("response status = %+v, want 201", stored.ResponseStatus)
	}
	if !stored.PaymentID.Valid || stored.PaymentID.UUID != pid {
		t.Errorf("payment id = %+v, want %s", stored.PaymentID, pid)
	}

	// SweepExpired drops keys older than the cutoff. Backdate one key well past a
	// 24h TTL and keep a fresh one; the old one must go, the fresh one must stay.
	oldKey := "idem-old-" + uuid.NewString()
	if _, err := sqlDB.ExecContext(ctx,
		`INSERT INTO idempotency_keys (key, request_hash, status, created_at)
		 VALUES ($1, $2, 'in_flight', now() - interval '48 hours')`,
		oldKey, []byte("old"),
	); err != nil {
		t.Fatalf("seed old key: %v", err)
	}
	freshKey := "idem-fresh-" + uuid.NewString()
	if _, err := store.Begin(ctx, freshKey, []byte("fresh")); err != nil {
		t.Fatalf("Begin(freshKey): %v", err)
	}

	n, err := store.SweepExpired(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("SweepExpired: %v", err)
	}
	if n < 1 {
		t.Errorf("SweepExpired deleted %d rows, want at least 1", n)
	}
	if _, err := db.New(sqlDB).GetIdempotencyKey(ctx, oldKey); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("old key lookup after sweep = %v, want ErrNoRows", err)
	}
	if _, err := db.New(sqlDB).GetIdempotencyKey(ctx, freshKey); err != nil {
		t.Errorf("fresh key lookup after sweep = %v, want it to survive", err)
	}
}
