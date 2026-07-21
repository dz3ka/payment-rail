package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/chain"
	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/reconcile"
)

// Tri-state exit codes owned by runReconcile. The exit code — not stdout — is the
// signal a monitor or cron gates on: a discrepancy (1) is a distinct outcome from
// an operational failure (2), so an alert can tell "reserves are short" from "the
// job could not run".
const (
	reconcileExitClean       = 0 // ran clean: every asset reconciled and collateralized
	reconcileExitDiscrepancy = 1 // ran fine but found a discrepancy or UNDERCOLLATERALIZED asset
	reconcileExitError       = 2 // usage / config / DB / RPC / operational failure
)

// reconcilePageSize is the keyset page size AggregateSettlements walks the
// settlements table with. Large enough to keep the round-trip count low on a big
// table, small enough that one page is a bounded index range scan.
const reconcilePageSize int32 = 1000

// reconcileDBQuerier is the narrow ledger seam the reconcile core depends on: the
// two keyset queries AggregateSettlements pages over, plus the per-asset liability
// sum. *db.Queries (db.New(sqlDB)) satisfies it structurally. The integration test
// passes the real querier and fakes only the chain, so no DB fake is needed.
type reconcileDBQuerier interface {
	ListSettlementsForReconcileFirstPage(ctx context.Context, limit int32) ([]db.ListSettlementsForReconcileFirstPageRow, error)
	ListSettlementsForReconcileAfter(ctx context.Context, arg db.ListSettlementsForReconcileAfterParams) ([]db.ListSettlementsForReconcileAfterRow, error)
	SumNonHouseLiabilities(ctx context.Context, asset string) (int64, error)
}

// runReconcile is the operator-facing proof-of-reserves command (PRD F-recon): it
// folds the ledger's settlement expectation against on-chain treasury balances and
// user liabilities, prints the report to stdout, and returns the tri-state exit
// code. It mirrors runAuditVerify's shape — own FlagSet, config load, validate
// BEFORE dialing, short-lived pool under a signal-cancel ctx — but owns its int
// exit directly (main calls os.Exit on it) because a discrepancy is a success with
// a distinct code, not an error.
//
// Order is fail-closed: parse flags, load config, build the treasury registry (a
// bad manifest or missing fallback address dies here without a connection), THEN
// dial the DB and chain. Any operational failure prints to stderr and returns 2.
func runReconcile(args []string) int {
	fs := flag.NewFlagSet("reconcile", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, "usage: paymentrailctl reconcile\n\n"+
			"Reconcile on-chain treasury balances against the ledger and check proof-of-reserves.\n"+
			"Exit codes: 0 clean, 1 discrepancy or undercollateralized, 2 usage/operational error.\n")
	}
	if err := fs.Parse(args); err != nil {
		return reconcileExitError
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "reconcile: load config: %v\n", err)
		return reconcileExitError
	}

	// Build the registry before any dial so a bad manifest / missing fallback
	// address fails closed without a pointless connection (mirrors audit's
	// decode-the-anchor-first discipline).
	reg, err := buildReconcileRegistry(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reconcile: build treasury registry: %v\n", err)
		return reconcileExitError
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	// Cancel on the first termination signal so a slow scan / RPC unwinds cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Short-lived ledger pool owned here (mirrors the audit/submit convention).
	sqlDB, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reconcile: open database: %v\n", err)
		return reconcileExitError
	}
	defer func() { _ = sqlDB.Close() }()
	if err := sqlDB.PingContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "reconcile: ping database: %v\n", err)
		return reconcileExitError
	}

	// Dial the chain node and wrap it in the balance reader (a bad/missing RPC URL
	// fails closed → exit 2).
	br, closeChain, err := newBalanceReader(ctx, cfg, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reconcile: %v\n", err)
		return reconcileExitError
	}
	defer closeChain()

	start := time.Now()
	report, err := reconcileReport(ctx, db.New(sqlDB), br, reg, reconcilePageSize, time.Now())
	if err != nil {
		fmt.Fprintf(os.Stderr, "reconcile: %v\n", err)
		return reconcileExitError
	}

	// The report artifact goes to stdout (it carries amounts by design); the
	// structured summary stays amount-free on stderr.
	report.WriteText(os.Stdout)
	logReconcileResult(ctx, logger, report, reg, time.Since(start))

	return reconcileExitCode(report.Clean)
}

// reconcileReport is the injectable core: it composes the three report inputs —
// ledger settlement sums, on-chain treasury balances, and user liabilities — into
// one Report, over the narrow DB seam and the neutral chain.BalanceReader port so
// the integration test drives it with a real querier and a fake chain. It fails
// closed: any DB or RPC error propagates so the caller maps it to exit 2.
//
// It guarantees consistent asset keys across the three inputs: every registry
// asset gets an entry in actuals (an empty slice if the manifest lists no address
// for it) AND a liabilities entry (−SumNonHouseLiabilities — the query returns the
// signed non-house balance sum, which is negated here to get a positive owed
// value), so BuildReport never sees a nil surprise for a treasury asset.
func reconcileReport(ctx context.Context, q reconcileDBQuerier, br chain.BalanceReader, reg reconcile.Registry, pageSize int32, now time.Time) (reconcile.Report, error) {
	sums, err := reconcile.AggregateSettlements(ctx, q, pageSize)
	if err != nil {
		return reconcile.Report{}, fmt.Errorf("aggregate settlements: %w", err)
	}

	// Read each treasury's on-chain balance, accumulating per asset. Seed every
	// registry asset with an (empty) slice first so an asset with no address still
	// gets a report row rather than being dropped.
	actuals := make(map[string][]reconcile.AddressBalance)
	for _, e := range reg.Entries {
		if _, ok := actuals[e.Asset]; !ok {
			actuals[e.Asset] = []reconcile.AddressBalance{}
		}
		bal, err := br.BalanceOf(ctx, e.Token, e.Address)
		if err != nil {
			// The BalanceReader already redacts RPC transport errors; the asset
			// symbol is safe to name, the amount/address are not echoed here.
			return reconcile.Report{}, fmt.Errorf("read on-chain balance for asset %s: %w", e.Asset, err)
		}
		actuals[e.Asset] = append(actuals[e.Asset], reconcile.AddressBalance{Address: e.Address, Actual: bal})
	}

	// Liabilities for each distinct registry asset. SumNonHouseLiabilities returns
	// Σ(credit−debit) over non-house accounts (the signed user-facing balance); the
	// proof-of-reserves check wants liabilities as a positive owed value, so negate.
	liabilities := make(map[string]int64, len(actuals))
	for asset := range actuals {
		sum, err := q.SumNonHouseLiabilities(ctx, asset)
		if err != nil {
			return reconcile.Report{}, fmt.Errorf("sum liabilities for asset %s: %w", asset, err)
		}
		liabilities[asset] = -sum
	}

	return reconcile.BuildReport(now, sums, liabilities, actuals), nil
}

// reconcileExitCode maps the report's clean flag onto the tri-state exit code's
// clean/discrepancy split. The operational-error code (2) is returned directly by
// runReconcile's fail-closed branches, never here.
func reconcileExitCode(clean bool) int {
	if clean {
		return reconcileExitClean
	}
	return reconcileExitDiscrepancy
}

// logReconcileResult emits one amount-free structured summary per run, extending
// the repo's logResult discipline (cmd/signer, evm.Adapter): it carries the shape
// of the outcome — how many assets and treasuries, whether a discrepancy is
// present, the per-asset verdict labels, and the duration — and NEVER an amount,
// balance, discrepancy value, or address. The report artifact on stdout is where
// amounts live; this line is safe to ship to a log aggregator.
func logReconcileResult(ctx context.Context, logger *slog.Logger, report reconcile.Report, reg reconcile.Registry, dur time.Duration) {
	verdicts := make([]string, 0, len(report.Assets))
	discrepancyPresent := false
	for _, a := range report.Assets {
		verdicts = append(verdicts, a.Asset+"="+a.Verdict)
		if a.Discrepancy != nil && a.Discrepancy.Sign() != 0 {
			discrepancyPresent = true
		}
	}
	logger.InfoContext(ctx, "reconciliation complete",
		"asset_count", len(report.Assets),
		"treasury_count", len(reg.Entries),
		"discrepancy_present", discrepancyPresent,
		"verdicts", verdicts,
		"clean", report.Clean,
		"duration_ms", dur.Milliseconds(),
	)
}
