package main

import (
	"context"
	"database/sql"
	"errors"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/policy"
)

// TestVelocityStoreIntegration exercises the real *sql.DB-backed pgVelocityStore
// against a live Postgres (skipped unless PAYMENT_RAIL_TEST_DSN is set, the same
// gate the ledger and settlement integration tests use). WP1–WP4 are proven
// hermetically; this file proves the one thing a fake can't: the real SQL path
// — windowed SUM/COUNT, decide-returns-error ⇒ ROLLBACK ⇒ no row, sliding-window
// expiry, and that the per-key pg_advisory_xact_lock actually serializes the
// SUM-then-INSERT so N racing charges can never over-count past the cap.
//
// Isolation mirrors the other integration tests: each subtest owns a unique
// key_id (a fresh uuid), so subtests never see each other's rows and repeated
// runs on a shared dev DB stay independent — no truncation needed.
func TestVelocityStoreIntegration(t *testing.T) {
	dsn := os.Getenv("PAYMENT_RAIL_TEST_DSN")
	if dsn == "" {
		t.Skip("set PAYMENT_RAIL_TEST_DSN to run the velocity store integration test")
	}

	ctx := context.Background()
	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	store := newVelocityStore(sqlDB)

	// Case 1: windowed count correctness. A generous window with MaxCount=N must
	// admit exactly N charges for a key and deny the (N+1)th with the sentinel —
	// proving SumVelocityWindow's COUNT is read and enforced against the cap.
	t.Run("count cap admits exactly N then denies", func(t *testing.T) {
		keyID := "vel-count-" + uuid.NewString()
		const n = 3
		lim := policy.NewVelocityLimiter(store, policy.VelocityCaps{
			Window:   time.Hour, // generous: no expiry in play here
			MaxCount: n,
		})
		for i := 0; i < n; i++ {
			if err := lim.Charge(ctx, keyID, big.NewInt(100)); err != nil {
				t.Fatalf("charge %d/%d = %v, want nil (under cap)", i+1, n, err)
			}
		}
		err := lim.Charge(ctx, keyID, big.NewInt(100))
		if !errors.Is(err, policy.ErrVelocityExceeded) {
			t.Fatalf("charge %d = %v, want errors.Is ErrVelocityExceeded", n+1, err)
		}
		if got := countEvents(ctx, t, sqlDB, keyID); got != n {
			t.Errorf("rows for key = %d, want %d (denied charge must not insert)", got, n)
		}
	})

	// Case 2: amount cap breach ⇒ ROLLBACK ⇒ no row. The key correctness property:
	// when decide returns ErrVelocityExceeded the whole tx rolls back, so the
	// denied charge leaves NO event row behind (asserted by a direct COUNT).
	t.Run("amount cap breach writes no row", func(t *testing.T) {
		keyID := "vel-amount-" + uuid.NewString()
		lim := policy.NewVelocityLimiter(store, policy.VelocityCaps{
			Window:    time.Hour,
			MaxAmount: big.NewInt(1000),
		})
		// 400 + 400 = 800, both under the 1000 ceiling.
		if err := lim.Charge(ctx, keyID, big.NewInt(400)); err != nil {
			t.Fatalf("charge 400 (1st) = %v, want nil", err)
		}
		if err := lim.Charge(ctx, keyID, big.NewInt(400)); err != nil {
			t.Fatalf("charge 400 (2nd) = %v, want nil", err)
		}
		// 800 + 300 = 1100 > 1000: denied.
		err := lim.Charge(ctx, keyID, big.NewInt(300))
		if !errors.Is(err, policy.ErrVelocityExceeded) {
			t.Fatalf("over-cap charge = %v, want errors.Is ErrVelocityExceeded", err)
		}
		if got := countEvents(ctx, t, sqlDB, keyID); got != 2 {
			t.Errorf("rows for key = %d, want 2 (rollback left the denied charge unrecorded)", got)
		}
	})

	// Case 3: sliding-window expiry. A stale event seeded outside the window must
	// not count toward MaxCount=1, so a fresh charge is still admitted — proving
	// the SUM's occurred_at >= since bound excludes rows before the window start.
	t.Run("stale event outside window does not count", func(t *testing.T) {
		keyID := "vel-expiry-" + uuid.NewString()
		const window = 5 * time.Second
		// Seed one event well outside the window (2*window in the past).
		seedVelocityEvent(ctx, t, sqlDB, keyID, 100, time.Now().Add(-2*window))

		lim := policy.NewVelocityLimiter(store, policy.VelocityCaps{
			Window:   window,
			MaxCount: 1,
		})
		// The in-window count is 0 (the stale row is excluded), so this is admitted.
		if err := lim.Charge(ctx, keyID, big.NewInt(100)); err != nil {
			t.Fatalf("fresh charge = %v, want nil (stale event must not count)", err)
		}
		// Now one in-window event exists; a second must breach MaxCount=1.
		if err := lim.Charge(ctx, keyID, big.NewInt(100)); !errors.Is(err, policy.ErrVelocityExceeded) {
			t.Fatalf("second in-window charge = %v, want errors.Is ErrVelocityExceeded", err)
		}
		// Two rows total: the seeded stale one + the one admitted charge.
		if got := countEvents(ctx, t, sqlDB, keyID); got != 2 {
			t.Errorf("rows for key = %d, want 2 (stale seed + one admitted charge)", got)
		}
	})

	// Case 4: concurrency / advisory-lock proof. N goroutines race MaxCount=K on
	// ONE key. If the SUM-then-INSERT were not serialized by pg_advisory_xact_lock,
	// several would read the same stale count and all insert, over-counting past K.
	// Exactly K must succeed, the rest fail with the sentinel, and the DB must hold
	// exactly K rows — never more.
	t.Run("advisory lock prevents concurrent overcount", func(t *testing.T) {
		keyID := "vel-race-" + uuid.NewString()
		const (
			k = 5
			n = 20
		)
		// Every goroutine opens a tx and blocks on the per-key lock, so the pool must
		// admit all N at once or they deadlock-by-starvation rather than serialize.
		sqlDB.SetMaxOpenConns(n + 5)

		lim := policy.NewVelocityLimiter(store, policy.VelocityCaps{
			Window:   time.Hour,
			MaxCount: k,
		})
		var wg sync.WaitGroup
		results := make(chan error, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results <- lim.Charge(ctx, keyID, big.NewInt(10))
			}()
		}
		wg.Wait()
		close(results)

		var ok, denied int
		for err := range results {
			switch {
			case err == nil:
				ok++
			case errors.Is(err, policy.ErrVelocityExceeded):
				denied++
			default:
				t.Fatalf("unexpected concurrent charge error = %v", err)
			}
		}
		if ok != k {
			t.Errorf("admitted charges = %d, want exactly %d", ok, k)
		}
		if denied != n-k {
			t.Errorf("denied charges = %d, want %d", denied, n-k)
		}
		if got := countEvents(ctx, t, sqlDB, keyID); got != k {
			t.Errorf("rows for key = %d, want exactly %d (advisory lock must prevent overcount)", got, k)
		}
	})

	// Case 5: int64 overflow fails closed. An amount past math.MaxInt64 can't be a
	// Postgres BIGINT, so the store rejects it with an OPERATIONAL error (not the
	// policy sentinel) before any insert. Driven through the store directly with a
	// permissive decide so the ONLY thing that can fail is the overflow guard.
	t.Run("int64 overflow fails closed with no row", func(t *testing.T) {
		keyID := "vel-overflow-" + uuid.NewString()
		huge := new(big.Int).Lsh(big.NewInt(1), 64) // 2^64 > math.MaxInt64
		allow := func(policy.Usage) error { return nil }
		err := store.Charge(ctx, keyID, huge, time.Hour, time.Now(), allow)
		if err == nil {
			t.Fatalf("overflow charge = nil, want a non-nil operational error")
		}
		if errors.Is(err, policy.ErrVelocityExceeded) {
			t.Fatalf("overflow charge = %v, want an operational error, NOT ErrVelocityExceeded", err)
		}
		if got := countEvents(ctx, t, sqlDB, keyID); got != 0 {
			t.Errorf("rows for key = %d, want 0 (overflow must never insert)", got)
		}
	})
}

// countEvents returns the number of velocity_events rows for keyID, straight
// through the pool — the direct DB observation the rollback/overcount assertions
// hang on.
func countEvents(ctx context.Context, t *testing.T, sqlDB *sql.DB, keyID string) int64 {
	t.Helper()
	var n int64
	if err := sqlDB.QueryRowContext(ctx,
		`SELECT count(*) FROM velocity_events WHERE key_id = $1`, keyID,
	).Scan(&n); err != nil {
		t.Fatalf("count events for %s: %v", keyID, err)
	}
	return n
}

// seedVelocityEvent inserts one event at an explicit occurred_at, bypassing the
// store — the test-only way to place a row outside the sliding window so the
// expiry assertion has a stale event to ignore.
func seedVelocityEvent(ctx context.Context, t *testing.T, sqlDB *sql.DB, keyID string, amount int64, at time.Time) {
	t.Helper()
	if _, err := sqlDB.ExecContext(ctx,
		`INSERT INTO velocity_events (key_id, amount, occurred_at) VALUES ($1, $2, $3)`,
		keyID, amount, at,
	); err != nil {
		t.Fatalf("seed velocity event: %v", err)
	}
}
