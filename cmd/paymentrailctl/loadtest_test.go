package main

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/db"
)

// TestRunLoadRequestsModeStopsAtExactCount asserts request mode issues exactly N
// ops in total across workers — no more, no fewer — regardless of concurrency.
func TestRunLoadRequestsModeStopsAtExactCount(t *testing.T) {
	var calls int64
	op := func(_ context.Context) Outcome {
		atomic.AddInt64(&calls, 1)
		return OK
	}
	r := runLoad(context.Background(), loadConfig{Concurrency: 8, Requests: 500}, op)
	if r.Total != 500 {
		t.Fatalf("Total = %d, want 500", r.Total)
	}
	if got := atomic.LoadInt64(&calls); got != 500 {
		t.Fatalf("op calls = %d, want 500", got)
	}
	if r.ByOutcome[OK] != 500 {
		t.Fatalf("ByOutcome[OK] = %d, want 500", r.ByOutcome[OK])
	}
}

// TestRunLoadTallyByOutcome drives a single worker so the op's call index maps
// deterministically to an Outcome, then asserts the per-class tally.
func TestRunLoadTallyByOutcome(t *testing.T) {
	var i int64
	op := func(_ context.Context) Outcome {
		n := atomic.AddInt64(&i, 1)
		return Outcome((n - 1) % 4)
	}
	r := runLoad(context.Background(), loadConfig{Concurrency: 1, Requests: 8}, op)
	want := [4]int{2, 2, 2, 2}
	if r.ByOutcome != want {
		t.Fatalf("ByOutcome = %v, want %v", r.ByOutcome, want)
	}
	if r.Total != 8 {
		t.Fatalf("Total = %d, want 8", r.Total)
	}
}

// TestRunLoadDurationModeStopsOnElapse asserts duration mode keeps issuing until
// the deadline passes and reports a wall clock at least that long.
func TestRunLoadDurationModeStopsOnElapse(t *testing.T) {
	op := func(_ context.Context) Outcome {
		time.Sleep(time.Millisecond)
		return OK
	}
	const dur = 50 * time.Millisecond
	r := runLoad(context.Background(), loadConfig{Concurrency: 2, Duration: dur}, op)
	if r.Total == 0 {
		t.Fatal("Total = 0, want > 0 in duration mode")
	}
	if r.Elapsed < 40*time.Millisecond {
		t.Fatalf("Elapsed = %s, want >= ~%s", r.Elapsed, dur)
	}
}

// TestRunLoadCtxCancelEndsEarly asserts a canceled context stops the run before
// the (huge) request budget is exhausted.
func TestRunLoadCtxCancelEndsEarly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled before the loop starts
	op := func(_ context.Context) Outcome { return OK }
	r := runLoad(ctx, loadConfig{Concurrency: 4, Requests: 1_000_000}, op)
	if r.Total >= 1_000_000 {
		t.Fatalf("Total = %d, want far fewer after ctx cancel", r.Total)
	}
}

// TestRunLoadDurationModeDiscardsCancelledSample asserts the deadline-cancelled
// in-flight request each worker is running when the run ends is NOT recorded. The op
// mirrors httpOp: it returns OK normally but TransportError if the ctx is done. With
// the discard, a clean duration run reports zero TransportErrors (the cancellation
// artifacts are dropped), not one per worker.
func TestRunLoadDurationModeDiscardsCancelledSample(t *testing.T) {
	op := func(ctx context.Context) Outcome {
		select {
		case <-time.After(time.Millisecond):
			return OK
		case <-ctx.Done():
			return TransportError
		}
	}
	r := runLoad(context.Background(), loadConfig{Concurrency: 4, Duration: 30 * time.Millisecond}, op)
	if r.ByOutcome[TransportError] != 0 {
		t.Fatalf("TransportError = %d, want 0 (cancelled tail samples must be discarded)", r.ByOutcome[TransportError])
	}
	if r.Total == 0 {
		t.Fatal("Total = 0, want > 0")
	}
	if r.Min <= 0 {
		t.Fatalf("Min = %s, want > 0 (a truncated ~0 sample would mean an artifact leaked)", r.Min)
	}
}

// TestRunLoadtestRejectsUnboundedRun asserts --duration=0 with no --requests is
// rejected before any DB dial, rather than looping until SIGINT.
func TestRunLoadtestRejectsUnboundedRun(t *testing.T) {
	err := runLoadtest([]string{"--duration=0"})
	if err == nil {
		t.Fatal("runLoadtest(--duration=0) = nil, want a validation error")
	}
	if !strings.Contains(err.Error(), "duration") {
		t.Fatalf("error = %q, want it to mention duration", err)
	}
}

func TestPercentileNearestRank(t *testing.T) {
	// Ten samples 10..100ns; nearest-rank rank = ceil(p/100 * 10).
	sorted := []time.Duration{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	cases := []struct {
		p    float64
		want time.Duration
	}{
		{0, 10},   // rank clamps to 1
		{50, 50},  // ceil(5) = 5 -> index 4
		{90, 90},  // ceil(9) = 9 -> index 8
		{95, 100}, // ceil(9.5) = 10 -> index 9
		{99, 100}, // ceil(9.9) = 10 -> index 9
		{100, 100},
	}
	for _, c := range cases {
		if got := percentile(sorted, c.p); got != c.want {
			t.Errorf("percentile(p=%v) = %s, want %s", c.p, got, c.want)
		}
	}
	if got := percentile(nil, 95); got != 0 {
		t.Errorf("percentile(empty) = %s, want 0", got)
	}
}

func TestWriteReportContainsKeyFields(t *testing.T) {
	var buf bytes.Buffer
	r := loadResult{
		Total:      10,
		ByOutcome:  [4]int{7, 2, 1, 0},
		Elapsed:    time.Second,
		Throughput: 7.0,
		Min:        time.Millisecond,
		P50:        2 * time.Millisecond,
		P95:        5 * time.Millisecond,
		P99:        9 * time.Millisecond,
		Max:        12 * time.Millisecond,
	}
	writeReport(&buf, loadConfig{Concurrency: 4, Requests: 10}, r)
	out := buf.String()
	for _, want := range []string{"throughput", "P95", "OK", "ClientError", "ServerError", "TransportError", "requests=10"} {
		if !bytes.Contains(buf.Bytes(), []byte(want)) {
			t.Errorf("report missing %q\nreport:\n%s", want, out)
		}
	}
}

// TestSeedAccountsDerivedBalance is DSN-gated (mirrors openTestDB): it seeds a few
// funded accounts and asserts distinct ids and that each source's DERIVED balance
// equals the opening amount.
func TestSeedAccountsDerivedBalance(t *testing.T) {
	dsn := os.Getenv("PAYMENT_RAIL_TEST_DSN")
	if dsn == "" {
		t.Skip("set PAYMENT_RAIL_TEST_DSN to run the seedAccounts integration test")
	}
	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	ctx := context.Background()
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	const opening int64 = 1_000_000
	sources, dests, err := seedAccounts(ctx, sqlDB, "USD", 3, opening)
	if err != nil {
		t.Fatalf("seedAccounts: %v", err)
	}
	if len(sources) != 3 || len(dests) != 3 {
		t.Fatalf("got %d sources / %d dests, want 3 / 3", len(sources), len(dests))
	}
	seen := map[uuid.UUID]bool{}
	for _, id := range append(append([]uuid.UUID{}, sources...), dests...) {
		if seen[id] {
			t.Fatalf("duplicate account id %s", id)
		}
		seen[id] = true
	}

	q := db.New(sqlDB)
	for _, id := range sources {
		bal, err := q.GetAccountBalance(ctx, id)
		if err != nil {
			t.Fatalf("GetAccountBalance(%s): %v", id, err)
		}
		if bal != opening {
			t.Fatalf("source %s balance = %d, want %d", id, bal, opening)
		}
	}
}
