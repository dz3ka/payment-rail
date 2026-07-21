package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dz3ka/payment-rail/internal/chain"
	"github.com/dz3ka/payment-rail/internal/chain/evm"
	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/policy"
	"github.com/dz3ka/payment-rail/internal/settlement"
	"github.com/dz3ka/payment-rail/internal/signerpb"
)

// broadcastIntent executes a single frozen payment intent end-to-end: it
// validates the chain config, enforces per-key velocity limits (record-on-attempt,
// BEFORE any dial), dials the isolated signer and the chain node, fails fast on the
// wrong network, submits the ERC-20 transfer, prints the resulting tx hash to
// stdout, and — when the intent carries a payment id — persists the payment↔tx-hash
// link so the chainwatcher can settle it.
//
// It is the shared execution core of BOTH the submit (below-threshold / four-eyes
// disabled) path and the approve (post-second-eyes) path: the sequence it runs was
// lifted verbatim from runSubmit, parameterized by the intent, so the two callers
// broadcast through byte-identical machinery. The tx hash is also returned so the
// approve path can record it against its approval row.
//
// On a link failure it returns the tx hash ALONGSIDE the error: the broadcast did
// succeed, only the bookkeeping write failed, and the caller's reconcile message
// (and stdout hash) must reflect that honestly.
func broadcastIntent(ctx context.Context, cfg config.Config, logger *slog.Logger, in policy.Intent) (chain.TxHash, error) {
	// The chain config is operator-supplied and has no safe default for these
	// fields, so a missing one must fail with the exact env var to set — not a
	// zero address the signer or node would silently reject deeper in.
	switch {
	case cfg.ChainRPCURL == "":
		return "", errors.New("broadcast: PAYMENT_RAIL_CHAIN_RPC_URL is required")
	case cfg.ChainFromAddress == "":
		return "", errors.New("broadcast: PAYMENT_RAIL_CHAIN_FROM_ADDRESS is required")
	case cfg.ChainUSDCAddress == "":
		return "", errors.New("broadcast: PAYMENT_RAIL_CHAIN_USDC_ADDRESS is required")
	case in.KeyID == "":
		return "", errors.New("broadcast: PAYMENT_RAIL_CHAIN_KEY_ID (or --key-id) is required")
	}

	// The fee cap is a decimal-wei string in config (config stays big.Int-free);
	// the composition root parses it so the adapter never re-parses config text.
	feeCap, ok := new(big.Int).SetString(cfg.ChainMaxFeePerGasCapWei, 10)
	if !ok {
		return "", fmt.Errorf("broadcast: PAYMENT_RAIL_CHAIN_MAX_FEE_PER_GAS_CAP_WEI %q is not a valid decimal integer", cfg.ChainMaxFeePerGasCapWei)
	}

	evmCfg := evm.Config{
		KeyID:              in.KeyID,
		ChainID:            cfg.ChainID,
		From:               common.HexToAddress(cfg.ChainFromAddress),
		Token:              common.HexToAddress(cfg.ChainUSDCAddress),
		GasLimitCap:        cfg.ChainGasLimitCap,
		MaxFeePerGasCapWei: feeCap,
	}

	// Enforce per-key velocity limits BEFORE any signer/chain dial: like the
	// denylist screen, a rejected payment must never cause a dial or a broadcast.
	// The caps come from config (config stays big.Int-free, so the composition root
	// parses MaxAmount here); a malformed cap fails closed.
	caps := policy.VelocityCaps{
		Window:   cfg.PolicyVelocityWindow,
		MaxCount: cfg.PolicyVelocityMaxCount,
	}
	if cfg.PolicyVelocityMaxAmount != "" {
		maxAmount, ok := new(big.Int).SetString(cfg.PolicyVelocityMaxAmount, 10)
		if !ok {
			return "", fmt.Errorf("broadcast: PAYMENT_RAIL_POLICY_VELOCITY_MAX_AMOUNT %q is not a valid decimal integer", cfg.PolicyVelocityMaxAmount) // fail closed
		}
		caps.MaxAmount = maxAmount
	}
	// Only open Postgres when velocity enforcement is actually configured: with the
	// caps disabled the legacy contract holds — no DB dial happens at all.
	if caps.Enabled() {
		sqlDB, err := sql.Open("postgres", cfg.DatabaseURL)
		if err != nil {
			return "", fmt.Errorf("broadcast: velocity check: open database: %w", err) // fail closed
		}
		defer func() { _ = sqlDB.Close() }()

		// Charge records the spend event as part of the same locked transaction that
		// admits it (record-on-attempt), so a later broadcast failure still consumes
		// window budget. That over-counts only in the safe direction. See ADR-0019.
		limiter := policy.NewVelocityLimiter(newVelocityStore(sqlDB), caps)
		if err := limiter.Charge(ctx, in.KeyID, in.Amount); err != nil {
			if errors.Is(err, policy.ErrVelocityExceeded) {
				// Audit trail for a rejected payment. key_id is safe to log (it is a
				// signing-key handle, not a recipient or amount); the amount and --to
				// are never logged.
				logger.Warn("payment rejected by velocity policy", "key_id", in.KeyID, "error", err)
			} else {
				// The velocity backend itself failed (DB unreachable, lock error, ...).
				// Fail closed and log at ERROR so an operator sees the control is degraded.
				logger.Error("velocity check failed; failing closed", "key_id", in.KeyID, "error", err)
			}
			return "", fmt.Errorf("broadcast: velocity check: %w", err)
		}
	}

	// Dial the isolated signer over loopback (no mTLS in slice 1); grpc.NewClient
	// is lazy, so a bad address surfaces on the first RPC, not here.
	conn, err := grpc.NewClient(cfg.SignerGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("broadcast: dial signer at %s: %w", cfg.SignerGRPCAddr, err)
	}
	defer func() { _ = conn.Close() }()
	sc := newSignerClient(signerpb.NewSignerServiceClient(conn))

	// Dial the chain node.
	ethClient, err := ethclient.DialContext(ctx, cfg.ChainRPCURL)
	if err != nil {
		return "", fmt.Errorf("broadcast: dial chain rpc at %s: %w", cfg.ChainRPCURL, err)
	}
	defer ethClient.Close()

	adapter, err := evm.NewAdapter(ethClient, sc, evmCfg, logger)
	if err != nil {
		return "", fmt.Errorf("broadcast: build adapter: %w", err)
	}

	// Fail fast on the wrong network before anything is signed: a signer key is
	// bound to one chain id, and a mismatched RPC must not sign a live-value tx.
	if err := adapter.VerifyChainID(ctx); err != nil {
		return "", fmt.Errorf("broadcast: verify chain id: %w", err)
	}

	txHash, err := adapter.Submit(ctx, chain.PaymentIntent{
		KeyID:  in.KeyID,
		Asset:  in.Asset,
		To:     in.To,
		Amount: in.Amount,
	})
	if err != nil {
		return "", fmt.Errorf("broadcast: %w", err)
	}

	fmt.Println(txHash)

	// With no payment id this is where execution ends: hash printed, and (unless
	// velocity was enabled) Postgres untouched — the legacy contract. With one,
	// persist the payment↔tx-hash link so the chainwatcher can settle it.
	if in.PaymentID != "" {
		// The submit path validates this up front and the approve path replays an id
		// that was validated at propose-time, so a parse error here is not expected;
		// handle it honestly anyway since the tx has already broadcast.
		paymentID, err := uuid.Parse(in.PaymentID)
		if err != nil {
			return txHash, fmt.Errorf("broadcast: tx %s broadcast succeeded but payment id %q is not a valid uuid — reconcile manually: %w", txHash, in.PaymentID, err)
		}
		sqlDB, err := sql.Open("postgres", cfg.DatabaseURL)
		if err != nil {
			return txHash, fmt.Errorf("broadcast: tx %s broadcast succeeded but linking to payment %s failed: open database: %w", txHash, paymentID, err)
		}
		defer func() { _ = sqlDB.Close() }()

		recorder := settlement.NewRecorder(db.New(sqlDB))
		if err := recorder.Link(ctx, paymentID, string(txHash)); err != nil {
			return txHash, fmt.Errorf("broadcast: tx %s broadcast succeeded but linking to payment %s failed — reconcile manually: %w", txHash, paymentID, err)
		}
		fmt.Printf("linked settlement: payment %s -> %s\n", paymentID, txHash)
	}
	return txHash, nil
}

// buildApprovalGate constructs the four-eyes ApprovalGate (PRD F8c) from config.
// It is fail-CLOSED: config stays big.Int-free, so the composition root parses the
// decimal threshold here, and a malformed value aborts rather than defaulting to a
// gate that would silently wave every payment straight to broadcast.
//
//   - ""/"0" disables four-eyes: a disabled gate (nil threshold) whose Required
//     always reports false. The approver allowlist is still carried so `approve`
//     can be used even with the threshold off (an operator may pre-park by other means).
//   - A parsed threshold must be a valid decimal > 0.
//   - Coherence: an ENABLED threshold with an EMPTY approver allowlist is a
//     misconfiguration — every gated payment would be un-approvable — so it fails fast.
func buildApprovalGate(cfg config.Config) (*policy.ApprovalGate, error) {
	if cfg.PolicyApprovalThreshold == "" || cfg.PolicyApprovalThreshold == "0" {
		return policy.NewApprovalGate(nil, cfg.PolicyApprovers), nil
	}
	threshold, ok := new(big.Int).SetString(cfg.PolicyApprovalThreshold, 10)
	if !ok || threshold.Sign() <= 0 {
		return nil, fmt.Errorf("config: invalid PAYMENT_RAIL_POLICY_APPROVAL_THRESHOLD %q", cfg.PolicyApprovalThreshold)
	}
	if len(cfg.PolicyApprovers) == 0 {
		return nil, errors.New("config: PAYMENT_RAIL_POLICY_APPROVAL_THRESHOLD is set but PAYMENT_RAIL_POLICY_APPROVERS is empty — gated payments would be un-approvable")
	}
	return policy.NewApprovalGate(threshold, cfg.PolicyApprovers), nil
}
