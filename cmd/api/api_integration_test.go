package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/payments"
)

// These e2e tests drive the real mux and payments service over HTTP against a
// live Postgres, skipped unless CONDUIT_TEST_DSN is set (so `go test ./...`
// stays green without a database). They run on the shared dev DB, so every
// fixture uses fresh uuids and asserts on its own rows, never global state.

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("CONDUIT_TEST_DSN")
	if dsn == "" {
		t.Skip("set CONDUIT_TEST_DSN to run the api e2e tests")
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

// newTestServer wires the real routes over sqlDB and serves them on a local
// httptest listener the tests drive with a plain http.Client.
func newTestServer(t *testing.T, sqlDB *sql.DB) *httptest.Server {
	t.Helper()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := payments.NewService(sqlDB, quiet)
	idem := payments.NewIdempotencyStore(sqlDB)
	ts := httptest.NewServer(newServer(svc, idem, quiet).routes())
	t.Cleanup(ts.Close)
	return ts
}

func newAccount(ctx context.Context, t *testing.T, q *db.Queries, asset string) db.Account {
	t.Helper()
	a, err := q.CreateAccount(ctx, db.CreateAccountParams{
		Name:  "api-" + uuid.NewString(),
		Kind:  "user",
		Asset: asset,
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	return a
}

// seedFunds gives id a starting balance via one opening journal entry and a lone
// credit line — the same test-only "money enters the system" shortcut the
// payments and ledger integration tests use. Balances are always derived.
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

// postCreate posts a create request with the given idempotency key and returns
// the raw response so callers can assert on status and body verbatim.
func postCreate(t *testing.T, ts *httptest.Server, key string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/payments", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func createBody(src, dst uuid.UUID, asset string, amount int64) []byte {
	return []byte(fmt.Sprintf(
		`{"source_account_id":%q,"dest_account_id":%q,"asset":%q,"amount":%d}`,
		src, dst, asset, amount))
}

func decodeBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}

func TestCreateReplaysIdenticalResponseForSameKey(t *testing.T) {
	sqlDB := openTestDB(t)
	ctx := context.Background()
	q := db.New(sqlDB)
	ts := newTestServer(t, sqlDB)

	const asset = "USD"
	src := newAccount(ctx, t, q, asset)
	dst := newAccount(ctx, t, q, asset)
	seedFunds(ctx, t, sqlDB, asset, src.ID, 1000)

	key := "idem-" + uuid.NewString()
	body := createBody(src.ID, dst.ID, asset, 300)

	first := postCreate(t, ts, key, body)
	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first create status = %d, want 201", first.StatusCode)
	}
	firstBody := decodeBody(t, first)

	second := postCreate(t, ts, key, body)
	if second.StatusCode != http.StatusCreated {
		t.Fatalf("replay status = %d, want 201", second.StatusCode)
	}
	secondBody := decodeBody(t, second)

	if !bytes.Equal(firstBody, secondBody) {
		t.Fatalf("replay body differs:\n first = %s\nsecond = %s", firstBody, secondBody)
	}
	// The replay must not have moved money a second time: source debited once.
	if bal := mustBalance(ctx, t, q, src.ID); bal != 700 {
		t.Errorf("source balance = %d, want 700 (one debit, not two)", bal)
	}
}

func TestCreateWithInflightKeyIsConflict(t *testing.T) {
	sqlDB := openTestDB(t)
	ctx := context.Background()
	q := db.New(sqlDB)
	ts := newTestServer(t, sqlDB)

	const asset = "USD"
	src := newAccount(ctx, t, q, asset)
	dst := newAccount(ctx, t, q, asset)
	seedFunds(ctx, t, sqlDB, asset, src.ID, 1000)

	// Seed an in_flight claim directly (deterministic stand-in for a concurrent
	// create still running under this key).
	key := "idem-inflight-" + uuid.NewString()
	if _, err := sqlDB.ExecContext(ctx,
		`INSERT INTO idempotency_keys (key, request_hash, status) VALUES ($1, $2, 'in_flight')`,
		key, []byte("seeded"),
	); err != nil {
		t.Fatalf("seed in_flight key: %v", err)
	}

	resp := postCreate(t, ts, key, createBody(src.ID, dst.ID, asset, 100))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409 for in_flight key", resp.StatusCode)
	}
}

func TestCreateSameKeyDifferentBodyIsUnprocessable(t *testing.T) {
	sqlDB := openTestDB(t)
	ctx := context.Background()
	q := db.New(sqlDB)
	ts := newTestServer(t, sqlDB)

	const asset = "USD"
	src := newAccount(ctx, t, q, asset)
	dst := newAccount(ctx, t, q, asset)
	seedFunds(ctx, t, sqlDB, asset, src.ID, 1000)

	key := "idem-" + uuid.NewString()
	first := postCreate(t, ts, key, createBody(src.ID, dst.ID, asset, 100))
	first.Body.Close()
	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first create status = %d, want 201", first.StatusCode)
	}

	// Same key, different amount → key reused with a different request.
	resp := postCreate(t, ts, key, createBody(src.ID, dst.ID, asset, 200))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 for key reuse with different body", resp.StatusCode)
	}
}

func TestListPaginatesInDescendingOrderOverHTTP(t *testing.T) {
	sqlDB := openTestDB(t)
	ctx := context.Background()
	q := db.New(sqlDB)
	ts := newTestServer(t, sqlDB)

	const asset = "USD"
	src := newAccount(ctx, t, q, asset)
	dst := newAccount(ctx, t, q, asset)
	seedFunds(ctx, t, sqlDB, asset, src.ID, 10000)

	const n = 5
	mine := make(map[uuid.UUID]bool, n)
	var order []uuid.UUID // insertion order, oldest -> newest
	for range n {
		resp := postCreate(t, ts, "idem-"+uuid.NewString(), createBody(src.ID, dst.ID, asset, 10))
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("create status = %d, want 201", resp.StatusCode)
		}
		var p paymentResponse
		if err := json.Unmarshal(decodeBody(t, resp), &p); err != nil {
			t.Fatalf("decode created payment: %v", err)
		}
		mine[p.ID] = true
		order = append(order, p.ID)
		// Keep created_at strictly increasing so newest-first is unambiguous.
		time.Sleep(2 * time.Millisecond)
	}

	var collected []uuid.UUID
	seen := make(map[uuid.UUID]bool)
	cursor := ""
	for len(seen) < n {
		url := ts.URL + "/v1/payments?limit=2"
		if cursor != "" {
			url += "&cursor=" + cursor
		}
		resp, err := ts.Client().Get(url)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("list status = %d, want 200", resp.StatusCode)
		}
		var page listResponse
		if err := json.Unmarshal(decodeBody(t, resp), &page); err != nil {
			t.Fatalf("decode list page: %v", err)
		}
		if len(page.Data) == 0 {
			t.Fatalf("list exhausted before finding all %d payments (found %d)", n, len(seen))
		}
		for _, p := range page.Data {
			if !mine[p.ID] {
				continue // rows other tests left behind
			}
			if seen[p.ID] {
				t.Fatalf("duplicate payment %s across pages", p.ID)
			}
			seen[p.ID] = true
			collected = append(collected, p.ID)
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}

	if len(collected) != n {
		t.Fatalf("collected %d of my payments, want %d", len(collected), n)
	}
	for i, id := range collected {
		if want := order[n-1-i]; id != want {
			t.Fatalf("page order[%d] = %s, want %s (newest-first)", i, id, want)
		}
	}
}

func TestGetUnknownPaymentIsNotFound(t *testing.T) {
	sqlDB := openTestDB(t)
	ts := newTestServer(t, sqlDB)

	resp, err := ts.Client().Get(ts.URL + "/v1/payments/" + uuid.NewString())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for unknown payment", resp.StatusCode)
	}
}

func TestCreateGetCancelThenReCancelConflict(t *testing.T) {
	sqlDB := openTestDB(t)
	ctx := context.Background()
	q := db.New(sqlDB)
	ts := newTestServer(t, sqlDB)

	const asset = "USD"
	src := newAccount(ctx, t, q, asset)
	dst := newAccount(ctx, t, q, asset)
	seedFunds(ctx, t, sqlDB, asset, src.ID, 1000)

	resp := postCreate(t, ts, "idem-"+uuid.NewString(), createBody(src.ID, dst.ID, asset, 250))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", resp.StatusCode)
	}
	var created paymentResponse
	if err := json.Unmarshal(decodeBody(t, resp), &created); err != nil {
		t.Fatalf("decode created: %v", err)
	}

	get, err := ts.Client().Get(ts.URL + "/v1/payments/" + created.ID.String())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if get.StatusCode != http.StatusOK {
		t.Fatalf("get status = %d, want 200", get.StatusCode)
	}
	get.Body.Close()

	cancelURL := ts.URL + "/v1/payments/" + created.ID.String() + "/cancel"
	first, err := ts.Client().Post(cancelURL, "application/json", nil)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if first.StatusCode != http.StatusOK {
		t.Fatalf("first cancel status = %d, want 200", first.StatusCode)
	}
	first.Body.Close()

	second, err := ts.Client().Post(cancelURL, "application/json", nil)
	if err != nil {
		t.Fatalf("re-cancel: %v", err)
	}
	if second.StatusCode != http.StatusConflict {
		t.Fatalf("re-cancel status = %d, want 409", second.StatusCode)
	}
	second.Body.Close()
}

// Non-gated: these fail before touching the database, so they run without a DSN
// and guard the validation edge.

func TestCreateWithoutIdempotencyKeyIsBadRequest(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	ts := httptest.NewServer(newServer(payments.NewService(nil, quiet), payments.NewIdempotencyStore(nil), quiet).routes())
	t.Cleanup(ts.Close)

	resp := postCreate(t, ts, "", createBody(uuid.New(), uuid.New(), "USD", 100))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for missing Idempotency-Key", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestGetMalformedIDIsBadRequest(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	ts := httptest.NewServer(newServer(payments.NewService(nil, quiet), payments.NewIdempotencyStore(nil), quiet).routes())
	t.Cleanup(ts.Close)

	resp, err := ts.Client().Get(ts.URL + "/v1/payments/not-a-uuid")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for malformed id", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestListMalformedCursorIsBadRequest(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	ts := httptest.NewServer(newServer(payments.NewService(nil, quiet), payments.NewIdempotencyStore(nil), quiet).routes())
	t.Cleanup(ts.Close)

	// "bm9waXBl" is valid base64url ("nopipe") but has no "|" separator, so it
	// decodes yet fails cursor parsing — the malformed-token path.
	resp, err := ts.Client().Get(ts.URL + "/v1/payments?cursor=bm9waXBl")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for malformed cursor", resp.StatusCode)
	}
	resp.Body.Close()
}
