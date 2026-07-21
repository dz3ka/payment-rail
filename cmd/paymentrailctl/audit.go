package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/audit"
	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/db"
)

// runAudit is the second-level router for the `audit` command family. It mirrors
// main.go's flat one-argument dispatch, one level down: today the only subcommand
// is `verify`, so anything else (including an empty args) is a usage error. Fail
// closed — an unrecognized subcommand never silently no-ops.
func runAudit(args []string) error {
	if len(args) == 0 || args[0] != "verify" {
		return errors.New("usage: paymentrailctl audit verify [--expect-head-hash <hex>]")
	}
	return runAuditVerify(args[1:])
}

// runAuditVerify is the operator-facing chain-integrity check (PRD F9): it reads
// the whole audit chain seq-ascending and runs the pure audit.Verify walk over it,
// optionally anchored to a known head hash. It mirrors runApprove's shape — own
// FlagSet, config load, signal-cancel ctx, error return that main turns into a
// non-zero exit.
//
// The EXIT CODE is the headline signal: 0 iff the chain is intact, non-zero for any
// tamper, bad anchor, bad flag, or DB error — so a monitor or cron can gate on it
// without parsing stdout. On success stdout carries the head hash so an operator can
// record it as the next run's --expect-head-hash anchor against tail-truncation.
func runAuditVerify(args []string) error {
	fs := flag.NewFlagSet("audit verify", flag.ContinueOnError)
	expectHead := fs.String("expect-head-hash", "", "optional hex-encoded expected head entry_hash; anchors against tail-truncation / chain-rebuild")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("audit verify: load config: %w", err)
	}

	// Decode the anchor before touching the DB so a malformed flag fails closed
	// without a pointless connection.
	var opts []audit.VerifyOpt
	if *expectHead != "" {
		h, err := hex.DecodeString(*expectHead)
		if err != nil {
			return fmt.Errorf("audit verify: --expect-head-hash is not valid hex: %w", err)
		}
		opts = append(opts, audit.WithExpectedHead(h))
	}

	// Short-lived pool: the verify path opens and closes its own handle (mirrors the
	// submit/approve convention).
	sqlDB, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("audit verify: open database: %w", err)
	}
	defer func() { _ = sqlDB.Close() }()

	// Cancel on the first termination signal so a slow scan unwinds cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("audit verify: ping database: %w", err)
	}

	rows, err := db.New(sqlDB).ScanAuditChain(ctx)
	if err != nil {
		return fmt.Errorf("audit verify: scan audit chain: %w", err)
	}

	res, err := audit.Verify(rows, opts...)
	if err != nil {
		// Fail closed with an operator-legible message to STDERR, then return the
		// error so main exits non-zero. A *TamperError carries the offending seq and
		// kind, which point the operator straight at the broken row.
		var te *audit.TamperError
		if errors.As(err, &te) {
			fmt.Fprintf(os.Stderr, "AUDIT CHAIN INVALID: tamper at seq %d: %s\n", te.Seq, te.Kind)
		} else {
			fmt.Fprintf(os.Stderr, "AUDIT CHAIN INVALID: %v\n", err)
		}
		return err
	}

	fmt.Printf("AUDIT CHAIN VALID: %d entries; head seq=%d hash=%s\n", res.Count, res.HeadSeq, hex.EncodeToString(res.HeadHash))
	return nil
}
