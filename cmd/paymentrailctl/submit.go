package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"syscall"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/policy"
)

// runSubmit executes one payment intent end-to-end: it resolves config, dials
// the isolated signer and the chain node, builds the EVM adapter, fails fast if
// the node is on the wrong network, and submits a single ERC-20 transfer. It is
// a one-shot command — NOT a long-running service — so it deliberately does not
// route through internal/service.Run; it wires the adapter directly and returns.
//
// On success it prints only the resulting transaction hash to stdout so the
// output is machine-consumable; all structured/redacted logging goes to stderr.
func runSubmit(args []string) error {
	fs := flag.NewFlagSet("submit", flag.ContinueOnError)
	var (
		toFlag     = fs.String("to", "", "recipient address (0x-hex, required)")
		amountFlag = fs.String("amount", "", "amount in the asset's smallest unit, decimal integer (required)")
		assetFlag  = fs.String("asset", "USDC", "asset symbol")
		keyIDFlag  = fs.String("key-id", "", "signer key id (default: PAYMENT_RAIL_CHAIN_KEY_ID)")
		// Optional: when set, the payment↔tx-hash link is persisted after a
		// successful broadcast so the chainwatcher can settle it. Empty keeps the
		// legacy behavior — print the hash and never touch Postgres.
		paymentIDFlag = fs.String("payment-id", "", "ledger payment id (uuid) to link this settlement to (optional)")
		// Operator id proposing this payment. Required only when the amount lands
		// at/above the four-eyes approval threshold (PRD F8c); ignored below it.
		proposerFlag = fs.String("proposer", "", "operator id proposing this payment (required at/above the four-eyes threshold)")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Required-flag and amount validation up front: --to and --amount have no
	// sensible default, and a non-positive amount is never a real payment.
	if *toFlag == "" {
		return errors.New("submit: --to is required")
	}
	// Reject a malformed destination up front, before config load or any dial: a
	// non-address --to can never be a real payment, and screening/signing must
	// only ever see a well-formed 20-byte address.
	if !common.IsHexAddress(*toFlag) {
		return fmt.Errorf("submit: --to %q is not a valid address", *toFlag)
	}
	if *amountFlag == "" {
		return errors.New("submit: --amount is required")
	}
	amount, ok := new(big.Int).SetString(*amountFlag, 10)
	if !ok {
		return fmt.Errorf("submit: --amount %q is not a valid decimal integer", *amountFlag)
	}
	if amount.Sign() <= 0 {
		return errors.New("submit: --amount must be positive")
	}

	// Validate --payment-id up front, before any config load or network dial: a
	// malformed id must fail fast and never broadcast an unlinkable transaction.
	// The parsed value is not kept here — broadcastIntent re-parses it at link time
	// (the intent carries it as a string); this guard only fails the command early.
	if *paymentIDFlag != "" {
		if _, err := uuid.Parse(*paymentIDFlag); err != nil {
			return fmt.Errorf("submit: --payment-id %q is not a valid uuid: %w", *paymentIDFlag, err)
		}
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("submit: load config: %w", err)
	}

	// Structured logs to stderr keep stdout clean for the tx hash; the adapter
	// emits one redacted line per outcome (never the amount or recipient). It is
	// constructed here, before screening, so a denial can be audit-logged.
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	// Screen the destination BEFORE any signing or dialing: a denied address must
	// never cause a signer/chain dial, a VerifyChainID, or a broadcast. policy.Load
	// is fail-CLOSED — a missing/malformed manifest aborts the payment; an empty
	// PolicyDenylist path disables screening. The screen is an in-memory lookup, so
	// context.Background() is used (the signal-cancel ctx is not established yet and
	// is only needed for the downstream network calls).
	screener, err := policy.Load(cfg.PolicyDenylist)
	if err != nil {
		return fmt.Errorf("submit: load policy denylist: %w", err) // fail closed
	}
	if err := screener.Screen(context.Background(), *toFlag); err != nil {
		if errors.Is(err, policy.ErrDenied) {
			// Compliance audit trail: a DENIED payment is exactly the event that must
			// be recorded, so the destination + reason are logged here — a deliberate,
			// deny-only exception to the "never log recipient" convention. Allowed
			// payments still log nothing about the recipient; the amount is never logged.
			logger.Warn("payment rejected by policy", "to", *toFlag, "error", err)
		} else {
			// The screening backend itself failed (unreachable for the in-memory
			// denylist, but the Screener port admits an I/O-backed provider). We
			// still fail closed below; log it at ERROR so an operator sees the
			// control is degraded rather than silently aborting.
			logger.Error("policy screening failed; failing closed", "error", err)
		}
		return fmt.Errorf("submit: screen destination: %w", err)
	}

	// --key-id defaults to the configured chain key. Resolve it after loading
	// config since flag defaults are fixed before config is read.
	keyID := *keyIDFlag
	if keyID == "" {
		keyID = cfg.ChainKeyID
	}

	// The frozen intent this command will either broadcast now or, when four-eyes
	// applies, park for a distinct second approver. Built from the validated flags;
	// the payment id travels as a string (broadcastIntent re-parses it at link time).
	intent := policy.Intent{
		To:        *toFlag,
		Asset:     *assetFlag,
		KeyID:     keyID,
		PaymentID: *paymentIDFlag,
		Amount:    amount,
	}

	// Four-eyes gate (PRD F8c) is evaluated AFTER denylist screening — screening
	// precedes four-eyes — and is fail-CLOSED: a malformed threshold, or a threshold
	// set with no approvers, aborts rather than broadcasting un-approvable value.
	gate, err := buildApprovalGate(cfg)
	if err != nil {
		return fmt.Errorf("submit: %w", err)
	}

	// Cancel on the first termination signal so a slow Postgres, RPC, or signer call
	// unwinds cleanly. Shared by both the park (propose) and broadcast paths below.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// At/above the threshold: DO NOT broadcast. Park the full intent as a pending
	// approval and require a distinct second operator to run `approve`. The proposer
	// must be named AND must itself be an allowlisted approver, so a parked payment
	// can never be one that no valid pair of eyes could ever clear.
	if gate.Required(amount) {
		if *proposerFlag == "" {
			return errors.New("submit: --proposer is required for a payment at or above the four-eyes approval threshold")
		}
		if !gate.KnownApprover(*proposerFlag) {
			return fmt.Errorf("submit: proposer %q is not in the approver allowlist", *proposerFlag)
		}

		// The park path opens its own short-lived pool (mirrors the velocity/link
		// convention: the path that needs Postgres opens and closes its own handle).
		sqlDB, err := sql.Open("postgres", cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("submit: four-eyes: open database: %w", err) // fail closed
		}
		defer func() { _ = sqlDB.Close() }()

		id, err := newApprovalStore(sqlDB).Propose(ctx, *proposerFlag, intent)
		if err != nil {
			return fmt.Errorf("submit: four-eyes: record pending approval: %w", err) // fail closed
		}

		// Audit trail for a parked high-value payment. This gate path is the one
		// place the amount may be logged (mirroring the deny-only log exception); the
		// below-threshold broadcast path still logs nothing about the amount.
		logger.Info("four-eyes required: payment parked for approval",
			"key_id", keyID, "amount", amount.String(), "approval_id", id, "proposer", *proposerFlag)
		fmt.Printf("four-eyes required: recorded pending approval %s; a distinct approver must run: paymentrailctl approve %s --approver=<id>\n", id, id)
		return nil // NOT broadcast: awaits a second approver
	}

	// Below the threshold (or four-eyes disabled): broadcast now, exactly as the
	// pre-four-eyes path did. broadcastIntent prints the tx hash on success.
	if _, err := broadcastIntent(ctx, cfg, logger, intent); err != nil {
		return err
	}
	return nil
}
