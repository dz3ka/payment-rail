package main

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/db"
)

// TestConcurrentCreatesNeverOverdrawSource is the milestone's DoD gate: it drives
// M concurrent creates that all debit ONE source whose balance funds only a
// fraction of them, and proves the ledger's ordered SELECT ... FOR UPDATE
// serializes the balance holds. Every request is a clean 201 or a clean 422
// (never a 500), the successful debits never exceed the balance, and the derived
// balance lands non-negative. It must pass under -race.
func TestConcurrentCreatesNeverOverdrawSource(t *testing.T) {
	sqlDB := openTestDB(t)
	ctx := context.Background()
	q := db.New(sqlDB)
	ts := newTestServer(t, sqlDB)

	const (
		asset   = "USD"
		amount  = int64(100)
		balance = int64(1000) // funds exactly 10 successful debits
		m       = 30          // total requested 3000 >> 1000, so over-draw is guaranteed
	)

	src := newAccount(ctx, t, q, asset)
	dst := newAccount(ctx, t, q, asset) // one shared dest: credits simply accumulate
	seedFunds(ctx, t, sqlDB, asset, src.ID, balance)

	statuses := make([]int, m)
	var wg sync.WaitGroup
	wg.Add(m)
	for i := range m {
		go func(i int) {
			defer wg.Done()
			// Distinct idempotency keys so every request is a genuine create, not
			// a replay — the contention we want is on the source balance, not the
			// key.
			resp := postCreate(t, ts, "idem-conc-"+uuid.NewString(), createBody(src.ID, dst.ID, asset, amount))
			statuses[i] = resp.StatusCode
			resp.Body.Close()
		}(i)
	}
	wg.Wait()

	var successes, insufficient int
	for _, code := range statuses {
		switch code {
		case http.StatusCreated:
			successes++
		case http.StatusUnprocessableEntity:
			insufficient++
		default:
			t.Fatalf("unexpected status %d under contention; want only 201 or 422", code)
		}
	}

	// Successful debits must fit inside the balance.
	if int64(successes)*amount > balance {
		t.Fatalf("successes=%d * amount=%d = %d exceeds balance %d", successes, amount, int64(successes)*amount, balance)
	}
	// At least one rejection proves the requests actually contended; if none, the
	// test under-loaded and proves nothing.
	if insufficient == 0 {
		t.Fatalf("no 422 rejections: test under-loaded, contention not exercised")
	}

	finalBalance := mustBalance(ctx, t, q, src.ID)
	if want := balance - int64(successes)*amount; finalBalance != want {
		t.Fatalf("final source balance = %d, want %d (balance - successes*amount)", finalBalance, want)
	}
	if finalBalance < 0 {
		t.Fatalf("source balance went negative: %d", finalBalance)
	}
	t.Logf("concurrency gate: successes=%d insufficient=%d final_balance=%d (balance=%d amount=%d)",
		successes, insufficient, finalBalance, balance, amount)
}
