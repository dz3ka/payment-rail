package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/db"
)

// Outcome classifies the result of one logical request so the load loop can tally
// success against the failure modes that matter for a throughput/latency run: a
// transport error (dial/timeout) is a different signal from a server 5xx, which is
// different again from a client 4xx (e.g. insufficient funds). The zero value is
// OK on purpose so an un-set tally slot reads as success-free, not error-free.
type Outcome int

const (
	// OK is a 2xx response: the create succeeded.
	OK Outcome = iota
	// ClientError is a 4xx response: the request was rejected (validation,
	// insufficient funds, idempotency conflict). Counted, not fatal.
	ClientError
	// ServerError is a 5xx response: the server failed to process the request.
	ServerError
	// TransportError is a dial/timeout/transport failure with no HTTP status.
	TransportError
)

// Op runs exactly one logical request and classifies it. It is the single seam of
// the load harness: the real run supplies an HTTP-backed Op, unit tests supply an
// in-memory one. The load loop — not the Op — times each call, so latency is what
// is under test, independent of how the request is issued.
type Op func(ctx context.Context) Outcome

// loadConfig fixes the shape of one run. Exactly one of Duration/Requests is
// non-zero: Duration != 0 runs until that wall-clock elapses; Requests != 0 issues
// exactly that many requests in total across all workers. The flag parser enforces
// the invariant before runLoad ever sees it.
type loadConfig struct {
	Concurrency int
	Duration    time.Duration
	Requests    int
}

// loadResult is the merged outcome of a run: the total requests issued, the tally
// per Outcome class, the wall-clock elapsed, derived throughput, and the latency
// distribution. Throughput counts only OK requests per elapsed second.
type loadResult struct {
	Total                   int
	ByOutcome               [4]int
	Elapsed                 time.Duration
	Throughput              float64
	Min, P50, P95, P99, Max time.Duration
}

// runLoad drives op under cfg.Concurrency worker goroutines and returns the merged
// latency distribution and outcome tally. Each worker owns its OWN latency slice
// and outcome tally so nothing on the hot path contends a shared lock — a mutex
// there would distort the very latency being measured. Workers stop when the
// Duration deadline passes, the shared Requests counter is exhausted, or ctx is
// canceled. After they join, the per-worker slices are merged, sorted once, and the
// percentiles read off by nearest-rank. Given a deterministic op the result is
// deterministic (the per-worker split of work is not, but the merged totals are).
func runLoad(ctx context.Context, cfg loadConfig, op Op) loadResult {
	start := time.Now()

	// Duration mode expresses the deadline as a derived context so a worker's only
	// stop check is ctx.Err(); Requests mode leaves ctx untouched and gates on the
	// shared counter instead.
	runCtx := ctx
	if cfg.Duration > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
	}

	// remaining is the shared request budget in Requests mode; a worker claims a
	// slot with a single atomic decrement, so exactly cfg.Requests ops run in total.
	remaining := int64(cfg.Requests)

	type workerResult struct {
		lats  []time.Duration
		tally [4]int
	}
	results := make([]workerResult, cfg.Concurrency)

	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(wr *workerResult) {
			defer wg.Done()
			for {
				if runCtx.Err() != nil {
					return
				}
				if cfg.Requests > 0 {
					// Claim a slot; a negative result means the budget is spent.
					if atomic.AddInt64(&remaining, -1) < 0 {
						return
					}
				}
				t0 := time.Now()
				oc := op(runCtx)
				// If the run ended (deadline elapsed or ctx canceled) while this
				// request was in flight, discard the sample: its outcome is a
				// cancellation artifact — a truncated latency and, for the HTTP op, a
				// spurious TransportError from the aborted client.Do — not a real
				// measurement. Costs at most one dropped sample per worker at the tail.
				if runCtx.Err() != nil {
					return
				}
				wr.lats = append(wr.lats, time.Since(t0))
				wr.tally[oc]++
			}
		}(&results[i])
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Merge the disjoint per-worker slices and tallies, then sort once for the
	// nearest-rank percentile reads.
	var all []time.Duration
	var tally [4]int
	for i := range results {
		all = append(all, results[i].lats...)
		for j := range tally {
			tally[j] += results[i].tally[j]
		}
	}
	sort.Slice(all, func(a, b int) bool { return all[a] < all[b] })

	res := loadResult{
		Total:     len(all),
		ByOutcome: tally,
		Elapsed:   elapsed,
		P50:       percentile(all, 50),
		P95:       percentile(all, 95),
		P99:       percentile(all, 99),
	}
	if len(all) > 0 {
		res.Min = all[0]
		res.Max = all[len(all)-1]
	}
	if secs := elapsed.Seconds(); secs > 0 {
		res.Throughput = float64(tally[OK]) / secs
	}
	return res
}

// percentile returns the nearest-rank pth percentile of a pre-sorted slice (p in
// [0,100]). An empty slice yields 0. Nearest-rank is deliberate over interpolation:
// every reported latency is a value that actually occurred, which is what a load
// report wants.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// writeReport renders the run as a plain fixed-label table to w. This is the report
// artifact (it carries latency/throughput figures by design); the structured slog
// summary is the amount-free counterpart for a log aggregator.
func writeReport(w io.Writer, cfg loadConfig, r loadResult) {
	mode := fmt.Sprintf("duration=%s", cfg.Duration)
	if cfg.Requests > 0 {
		mode = fmt.Sprintf("requests=%d", cfg.Requests)
	}
	fmt.Fprintf(w, "load test report\n")
	fmt.Fprintf(w, "  concurrency:    %d\n", cfg.Concurrency)
	fmt.Fprintf(w, "  mode:           %s\n", mode)
	fmt.Fprintf(w, "  elapsed:        %s\n", r.Elapsed)
	fmt.Fprintf(w, "  total:          %d\n", r.Total)
	fmt.Fprintf(w, "  throughput:     %.2f req/s\n", r.Throughput)
	fmt.Fprintf(w, "  latency:\n")
	fmt.Fprintf(w, "    Min: %s\n", r.Min)
	fmt.Fprintf(w, "    P50: %s\n", r.P50)
	fmt.Fprintf(w, "    P95: %s\n", r.P95)
	fmt.Fprintf(w, "    P99: %s\n", r.P99)
	fmt.Fprintf(w, "    Max: %s\n", r.Max)
	fmt.Fprintf(w, "  outcomes:\n")
	fmt.Fprintf(w, "    OK:             %d\n", r.ByOutcome[OK])
	fmt.Fprintf(w, "    ClientError:    %d\n", r.ByOutcome[ClientError])
	fmt.Fprintf(w, "    ServerError:    %d\n", r.ByOutcome[ServerError])
	fmt.Fprintf(w, "    TransportError: %d\n", r.ByOutcome[TransportError])
}

// createPaymentRequest is the wire shape of a POST /v1/payments body. It mirrors
// cmd/api's createRequest field-for-field; it is redeclared here because that type
// is unexported in a different package main, and duplicating four fields is cheaper
// than exporting the API's DTO for a load tool.
type createPaymentRequest struct {
	SourceAccountID uuid.UUID `json:"source_account_id"`
	DestAccountID   uuid.UUID `json:"dest_account_id"`
	Asset           string    `json:"asset"`
	Amount          int64     `json:"amount"`
}

// classifyStatus maps an HTTP status code onto an Outcome. 2xx is OK, 4xx is a
// client rejection, 5xx (and any unexpected non-2xx) is a server failure.
func classifyStatus(status int) Outcome {
	switch {
	case status >= 200 && status < 300:
		return OK
	case status >= 400 && status < 500:
		return ClientError
	default:
		return ServerError
	}
}

// httpOp builds the HTTP-backed Op the real run drives: each call picks a random
// source/dest pair (spreading traffic across many sources to avoid serializing on a
// single account's row lock), marshals the create body, sets a UNIQUE
// Idempotency-Key so every call exercises the create path rather than replaying a
// cached response, and POSTs to baseURL+"/v1/payments". A transport error classifies
// as TransportError; otherwise the status code decides.
func httpOp(client *http.Client, baseURL, asset string, sources, dests []uuid.UUID) Op {
	endpoint := baseURL + "/v1/payments"
	return func(ctx context.Context) Outcome {
		src := sources[rand.IntN(len(sources))]
		dst := dests[rand.IntN(len(dests))]
		body, err := json.Marshal(createPaymentRequest{
			SourceAccountID: src,
			DestAccountID:   dst,
			Asset:           asset,
			Amount:          1,
		})
		if err != nil {
			return TransportError
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return TransportError
		}
		req.Header.Set("Content-Type", "application/json")
		// A fresh key per request: a reused key would measure idempotent replay, not
		// a real create.
		req.Header.Set("Idempotency-Key", uuid.NewString())
		resp, err := client.Do(req)
		if err != nil {
			return TransportError
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(io.Discard, resp.Body)
		return classifyStatus(resp.StatusCode)
	}
}

// seedAccounts creates n source and n dest accounts (uuid-suffixed names to dodge
// the UNIQUE(name,asset) constraint) via the same db.Queries.CreateAccount the tests
// use, and gives every SOURCE a starting balance through one opening journal entry
// plus a lone credit line — the exact test-only "money enters the system" shortcut
// seedFunds uses (balances are always derived, never stored). It returns the created
// source and dest ids so the load Op can spread traffic across them.
func seedAccounts(ctx context.Context, sqlDB *sql.DB, asset string, n int, openingBalance int64) (sources, dests []uuid.UUID, err error) {
	q := db.New(sqlDB)
	for i := 0; i < n; i++ {
		acct, err := q.CreateAccount(ctx, db.CreateAccountParams{
			Name:  "loadtest-src-" + uuid.NewString(),
			Kind:  "user",
			Asset: asset,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("create source account: %w", err)
		}
		// Opening credit via raw SQL, exactly like seedFunds: one 'opening' journal
		// entry keyed by a fresh external_ref, then a single credit line.
		var entryID uuid.UUID
		if err := sqlDB.QueryRowContext(ctx,
			`INSERT INTO journal_entries (kind, external_ref, asset) VALUES ('opening', $1, $2) RETURNING id`,
			uuid.NewString(), asset,
		).Scan(&entryID); err != nil {
			return nil, nil, fmt.Errorf("seed opening entry: %w", err)
		}
		if _, err := sqlDB.ExecContext(ctx,
			`INSERT INTO entry_lines (entry_id, account_id, direction, amount) VALUES ($1, $2, 'credit', $3)`,
			entryID, acct.ID, openingBalance,
		); err != nil {
			return nil, nil, fmt.Errorf("seed opening line: %w", err)
		}
		sources = append(sources, acct.ID)
	}
	for i := 0; i < n; i++ {
		acct, err := q.CreateAccount(ctx, db.CreateAccountParams{
			Name:  "loadtest-dst-" + uuid.NewString(),
			Kind:  "user",
			Asset: asset,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("create dest account: %w", err)
		}
		dests = append(dests, acct.ID)
	}
	return sources, dests, nil
}

// applyMigrations is a hermetic fresh-DB bootstrap: it reads every *.up.sql in dir,
// sorts them lexically, and Execs each in order. It is deliberately minimal — no
// version table, no down migrations, fail-fast on the first error — because
// ADR-0015 forbids shelling out to host psql or adding a migration dependency, and
// the load tool only ever runs this once against an empty database. Running it
// against an already-migrated DB fails on the first CREATE TABLE; that is expected.
func applyMigrations(ctx context.Context, sqlDB *sql.DB, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	for _, f := range files {
		stmt, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			return fmt.Errorf("read migration %s: %w", f, err)
		}
		if _, err := sqlDB.ExecContext(ctx, string(stmt)); err != nil {
			return fmt.Errorf("apply migration %s: %w", f, err)
		}
	}
	return nil
}

// runLoadtest is the operator-facing load command: it seeds funded ledger accounts,
// drives POST /v1/payments under sustained concurrency, and prints a throughput +
// latency report. It mirrors the reconcile/submit subcommand shape — own FlagSet,
// config load, short-lived Postgres pool under a signal-cancel ctx, %w-wrapped
// fail-fast errors — and never routes through a long-running service. The report
// (with latencies) goes to stdout; the structured summary on stderr stays
// amount-free per the repo's logging convention.
func runLoadtest(args []string) error {
	fs := flag.NewFlagSet("loadtest", flag.ContinueOnError)

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("loadtest: load config: %w", err)
	}

	var (
		urlFlag         = fs.String("url", "http://localhost:8080", "base URL of the payments API")
		dsnFlag         = fs.String("dsn", cfg.DatabaseURL, "Postgres DSN for seeding accounts")
		concurrencyFlag = fs.Int("concurrency", 32, "number of concurrent workers")
		durationFlag    = fs.Duration("duration", 30*time.Second, "run duration (mutually exclusive with --requests)")
		requestsFlag    = fs.Int("requests", 0, "total requests to issue (mutually exclusive with --duration)")
		assetFlag       = fs.String("asset", "USD", "asset symbol for seeded accounts and payments")
		accountsFlag    = fs.Int("accounts", 100, "number of source (and dest) accounts to seed")
		openingFlag     = fs.Int64("opening-balance", 1<<40, "opening balance credited to each source account")
		migrateFlag     = fs.Bool("migrate", false, "apply db/migrations before seeding (fresh-DB bootstrap)")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Duration XOR Requests: reject both explicitly set; --requests>0 selects request
	// mode; otherwise the (default or explicit) --duration is used.
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	if set["duration"] && set["requests"] {
		return errors.New("loadtest: set only one of --duration or --requests")
	}
	if *concurrencyFlag < 1 {
		return errors.New("loadtest: --concurrency must be >= 1")
	}
	if *accountsFlag < 1 {
		return errors.New("loadtest: --accounts must be >= 1")
	}
	lc := loadConfig{Concurrency: *concurrencyFlag}
	if *requestsFlag > 0 {
		lc.Requests = *requestsFlag
	} else {
		lc.Duration = *durationFlag
	}
	// Guard the lower bound the XOR check misses: --duration=0 alone leaves neither a
	// deadline nor a request budget, which would run until SIGINT.
	if lc.Duration <= 0 && lc.Requests <= 0 {
		return errors.New("loadtest: --duration must be > 0 (or set --requests)")
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	// Cancel on the first termination signal so a slow run unwinds cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sqlDB, err := sql.Open("postgres", *dsnFlag)
	if err != nil {
		return fmt.Errorf("loadtest: open database: %w", err)
	}
	defer func() { _ = sqlDB.Close() }()
	if err := sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("loadtest: ping database: %w", err)
	}

	if *migrateFlag {
		if err := applyMigrations(ctx, sqlDB, "db/migrations"); err != nil {
			return fmt.Errorf("loadtest: apply migrations: %w", err)
		}
	}

	sources, dests, err := seedAccounts(ctx, sqlDB, *assetFlag, *accountsFlag, *openingFlag)
	if err != nil {
		return fmt.Errorf("loadtest: seed accounts: %w", err)
	}

	// One shared client with bounded timeouts; the transport pools connections so
	// the concurrency knob is what limits in-flight requests, not socket churn.
	client := &http.Client{Timeout: 30 * time.Second}
	op := httpOp(client, *urlFlag, *assetFlag, sources, dests)

	result := runLoad(ctx, lc, op)
	writeReport(os.Stdout, lc, result)

	// Amount-free structured summary (repo convention): shape of the outcome only —
	// no balances, account ids, or the opening amount.
	logger.InfoContext(ctx, "loadtest complete",
		"concurrency", lc.Concurrency,
		"total", result.Total,
		"throughput_rps", result.Throughput,
		"p50_ms", result.P50.Milliseconds(),
		"p95_ms", result.P95.Milliseconds(),
		"p99_ms", result.P99.Milliseconds(),
		"max_ms", result.Max.Milliseconds(),
		"ok", result.ByOutcome[OK],
		"client_error", result.ByOutcome[ClientError],
		"server_error", result.ByOutcome[ServerError],
		"transport_error", result.ByOutcome[TransportError],
	)
	return nil
}
