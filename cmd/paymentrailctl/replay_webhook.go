package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/db"
)

// runReplayWebhook re-drives every dead-lettered webhook delivery for one
// subscription — the operator action after a broken endpoint is fixed (PRD F11).
// Like submit it is a one-shot command: it owns its FlagSet, validates flags
// before any I/O, opens the ledger DB directly, runs a single UPDATE, and
// returns — it deliberately does NOT route through internal/service.Run.
//
// The requeued count is printed to stdout so the result is scriptable; the
// structured (redacted) log line goes to stderr, matching submit.go.
func runReplayWebhook(args []string) error {
	fs := flag.NewFlagSet("replay-webhook", flag.ContinueOnError)
	subIDFlag := fs.String("subscription-id", "", "UUID of the webhook subscription whose dead-lettered deliveries to re-drive (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Required-flag and uuid validation up front, before any config load or DB
	// dial: a missing or malformed id must fail fast and never touch Postgres.
	if *subIDFlag == "" {
		return errors.New("replay-webhook: --subscription-id is required")
	}
	subID, err := uuid.Parse(*subIDFlag)
	if err != nil {
		return fmt.Errorf("replay-webhook: --subscription-id %q is not a valid uuid: %w", *subIDFlag, err)
	}

	// Structured logs to stderr keep stdout clean for the requeued count; the id
	// is not a secret, the payloads (which carry amounts) are never logged.
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("replay-webhook: load config: %w", err)
	}

	// Cancel on the first termination signal so a slow query unwinds cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sqlDB, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("replay-webhook: open database: %w", err)
	}
	defer func() { _ = sqlDB.Close() }()

	if err := sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("replay-webhook: ping database: %w", err)
	}

	n, err := db.New(sqlDB).ReplayDeadLettered(ctx, subID)
	if err != nil {
		return fmt.Errorf("replay-webhook: replay: %w", err)
	}

	logger.Info("replayed dead-lettered webhook deliveries",
		"subscription_id", subID.String(),
		"requeued", n,
	)
	fmt.Printf("requeued %d dead-lettered deliver%s for subscription %s\n", n, plural(n), subID)
	return nil
}

// plural picks the delivery/deliveries suffix for the stdout summary.
func plural(n int64) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}
