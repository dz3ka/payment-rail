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
	"strings"
	"syscall"

	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/policy"
)

// runApprove is the second half of four-eyes (PRD F8c): a DISTINCT operator claims
// a payment parked by `submit` and, only if the claim authorizes, broadcasts it.
// It mirrors runSubmit's shape (own FlagSet, config load, signal-cancel ctx) and is
// fail-CLOSED end to end — every rejection (unknown approval, non-pending row,
// unknown or self approver) returns without broadcasting.
//
// The claim (guarded status transition under a row lock) and the broadcast are two
// steps by necessity: the DB commit that marks the row approved must land before
// the irreversible on-chain send, so a broadcast that then fails leaves a claimed
// (approved) row the operator must reconcile — surfaced honestly rather than
// silently rolled back over a network blip. On success stdout carries the tx hash
// (printed by broadcastIntent); the tx-hash bookkeeping write is best-effort.
func runApprove(args []string) error {
	fs := flag.NewFlagSet("approve", flag.ContinueOnError)
	approverFlag := fs.String("approver", "", "operator id approving the payment (required; must differ from the proposer)")

	// The approval id is positional. Go's flag package stops at the first non-flag
	// arg, so pull a leading positional off before Parse to accept it in either
	// order: `approve <id> --approver=x` and `approve --approver=x <id>`.
	var approvalID string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		approvalID, args = args[0], args[1:]
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if approvalID == "" {
		approvalID = fs.Arg(0)
	}
	if approvalID == "" {
		return errors.New("approve: <approval-id> is required")
	}
	if *approverFlag == "" {
		return errors.New("approve: --approver is required")
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("approve: load config: %w", err)
	}

	// Structured logs to stderr keep stdout clean for the tx hash.
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	// Build the gate up front (fail closed): the same threshold/allowlist config the
	// proposer was validated against now authorizes the approver.
	gate, err := buildApprovalGate(cfg)
	if err != nil {
		return fmt.Errorf("approve: %w", err) // fail closed
	}

	sqlDB, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("approve: open database: %w", err)
	}
	defer func() { _ = sqlDB.Close() }()
	store := newApprovalStore(sqlDB)

	// Cancel on the first termination signal so a slow claim or broadcast unwinds
	// cleanly instead of hanging.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Claim atomically: the decide closure runs the four-eyes authorization inside
	// the locked transaction, so an unauthorized approver never flips the row.
	intent, err := store.Claim(ctx, approvalID, *approverFlag, func(pa policy.PendingApproval) error {
		return gate.Authorize(pa.Proposer, *approverFlag)
	})
	if err != nil {
		switch {
		case errors.Is(err, ErrApprovalNotFound):
			return fmt.Errorf("approve: no pending approval with id %s", approvalID)
		case errors.Is(err, ErrAlreadyApproved):
			return fmt.Errorf("approve: approval %s is not pending (already approved, broadcast, or being claimed)", approvalID)
		case errors.Is(err, policy.ErrUnknownApprover):
			// Audit trail for a rejected approval. approver + approval id are safe to
			// log (operator handle, not a recipient or amount).
			logger.Warn("approval rejected: approver not in allowlist", "approval_id", approvalID, "approver", *approverFlag)
			return fmt.Errorf("approve: approver %q is not in the approver allowlist", *approverFlag)
		case errors.Is(err, policy.ErrSelfApproval):
			logger.Warn("approval rejected: self-approval", "approval_id", approvalID, "approver", *approverFlag)
			return fmt.Errorf("approve: approver %q must differ from the proposer (four-eyes)", *approverFlag)
		default:
			return fmt.Errorf("approve: claim approval %s: %w", approvalID, err) // fail closed
		}
	}

	// The approval is now committed as 'approved'. Broadcast the frozen intent.
	hash, err := broadcastIntent(ctx, cfg, logger, intent)
	if err != nil {
		// Branch on the returned hash to tell a pre-send failure apart from a
		// post-send bookkeeping failure (broadcastIntent's contract):
		//   - EMPTY hash ⇒ the failure happened BEFORE adapter.Submit (chain-cfg
		//     check, feeCap parse, velocity Charge, signer/chain dial, VerifyChainID).
		//     NOTHING was broadcast, so it is safe to reopen the row to pending and let
		//     a retry re-claim it after the cause is fixed — no double-send is possible.
		//   - NON-EMPTY hash ⇒ adapter.Submit already broadcast the tx; only the
		//     optional payment-id link failed. Reopening here would let a second approve
		//     re-send the SAME payment — a double-send hazard — so we NEVER reopen; the
		//     operator reconciles the already-sent tx by hand.
		if hash == "" {
			if reopenErr := store.Reopen(ctx, approvalID); reopenErr != nil {
				// Reopen itself failed: fall back to the reconcile-manually message so the
				// operator still knows the row needs attention, and surface why reopen failed.
				fmt.Fprintf(os.Stderr, "approve: approval %s is claimed (approved) but broadcast did not complete and reopen failed (%v) — reconcile manually before re-approving\n", approvalID, reopenErr)
				return err
			}
			fmt.Fprintf(os.Stderr, "approve: broadcast failed before send (%v); approval %s reopened to pending — fix the cause and re-approve\n", err, approvalID)
			return err
		}
		// Non-empty hash: the tx WAS broadcast. Keep the existing reconcile-manually
		// honesty (do NOT reopen — that risks a double-send). Mirror submit's
		// link-failure behavior: the operator reconciles before retrying, since the
		// guarded MarkBroadcast means a genuine re-broadcast can't just re-run blindly.
		fmt.Fprintf(os.Stderr, "approve: approval %s is claimed (approved) but broadcast did not complete — reconcile manually before re-approving\n", approvalID)
		return err
	}

	// Best-effort bookkeeping: the tx DID broadcast, so a failure to record its hash
	// must NOT fail the command — that would misreport an irreversible send as failed.
	// Warn loudly so an operator can reconcile the approval row.
	if err := store.MarkBroadcast(ctx, approvalID, string(hash)); err != nil {
		logger.Error("broadcast succeeded but recording tx hash on approval failed; reconcile the approval row",
			"approval_id", approvalID, "tx_hash", string(hash), "error", err)
		fmt.Fprintf(os.Stderr, "warning: tx %s broadcast but approval %s tx_hash was not recorded — reconcile manually\n", hash, approvalID)
	}
	return nil
}
