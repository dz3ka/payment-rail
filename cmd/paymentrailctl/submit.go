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
	var paymentID uuid.UUID
	if *paymentIDFlag != "" {
		parsed, err := uuid.Parse(*paymentIDFlag)
		if err != nil {
			return fmt.Errorf("submit: --payment-id %q is not a valid uuid: %w", *paymentIDFlag, err)
		}
		paymentID = parsed
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

	// The chain config is operator-supplied and has no safe default for these
	// fields, so a missing one must fail with the exact env var to set — not a
	// zero address the signer or node would silently reject deeper in.
	switch {
	case cfg.ChainRPCURL == "":
		return errors.New("submit: PAYMENT_RAIL_CHAIN_RPC_URL is required")
	case cfg.ChainFromAddress == "":
		return errors.New("submit: PAYMENT_RAIL_CHAIN_FROM_ADDRESS is required")
	case cfg.ChainUSDCAddress == "":
		return errors.New("submit: PAYMENT_RAIL_CHAIN_USDC_ADDRESS is required")
	case keyID == "":
		return errors.New("submit: PAYMENT_RAIL_CHAIN_KEY_ID (or --key-id) is required")
	}

	// The fee cap is a decimal-wei string in config (config stays big.Int-free);
	// the composition root parses it so the adapter never re-parses config text.
	feeCap, ok := new(big.Int).SetString(cfg.ChainMaxFeePerGasCapWei, 10)
	if !ok {
		return fmt.Errorf("submit: PAYMENT_RAIL_CHAIN_MAX_FEE_PER_GAS_CAP_WEI %q is not a valid decimal integer", cfg.ChainMaxFeePerGasCapWei)
	}

	evmCfg := evm.Config{
		KeyID:              keyID,
		ChainID:            cfg.ChainID,
		From:               common.HexToAddress(cfg.ChainFromAddress),
		Token:              common.HexToAddress(cfg.ChainUSDCAddress),
		GasLimitCap:        cfg.ChainGasLimitCap,
		MaxFeePerGasCapWei: feeCap,
	}

	// Cancel on the first termination signal so a slow RPC or signer call unwinds
	// cleanly instead of hanging.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Enforce per-key velocity limits BEFORE any signer/chain dial: like the
	// denylist screen above, a rejected payment must never cause a dial or a
	// broadcast. The caps come from config (config stays big.Int-free, so the
	// composition root parses MaxAmount here); a malformed cap fails closed.
	caps := policy.VelocityCaps{
		Window:   cfg.PolicyVelocityWindow,
		MaxCount: cfg.PolicyVelocityMaxCount,
	}
	if cfg.PolicyVelocityMaxAmount != "" {
		maxAmount, ok := new(big.Int).SetString(cfg.PolicyVelocityMaxAmount, 10)
		if !ok {
			return fmt.Errorf("submit: PAYMENT_RAIL_POLICY_VELOCITY_MAX_AMOUNT %q is not a valid decimal integer", cfg.PolicyVelocityMaxAmount) // fail closed
		}
		caps.MaxAmount = maxAmount
	}
	// Only open Postgres when velocity enforcement is actually configured: with the
	// caps disabled the legacy contract holds — no DB dial happens at all.
	if caps.Enabled() {
		sqlDB, err := sql.Open("postgres", cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("submit: velocity check: open database: %w", err) // fail closed
		}
		defer func() { _ = sqlDB.Close() }()

		// Charge records the spend event as part of the same locked transaction that
		// admits it (record-on-attempt), so a later broadcast failure still consumes
		// window budget. That over-counts only in the safe direction — recording after
		// a successful broadcast would reopen the concurrent-double-spend window the
		// per-key advisory lock exists to close. See ADR-0019.
		limiter := policy.NewVelocityLimiter(newVelocityStore(sqlDB), caps)
		if err := limiter.Charge(ctx, keyID, amount); err != nil {
			if errors.Is(err, policy.ErrVelocityExceeded) {
				// Audit trail for a rejected payment. key_id is safe to log (it is a
				// signing-key handle, not a recipient or amount) — this extends the
				// slice-1 deny-only log exception; the amount and --to are never logged.
				logger.Warn("payment rejected by velocity policy", "key_id", keyID, "error", err)
			} else {
				// The velocity backend itself failed (DB unreachable, lock error, ...).
				// Fail closed and log at ERROR so an operator sees the control is degraded.
				logger.Error("velocity check failed; failing closed", "key_id", keyID, "error", err)
			}
			return fmt.Errorf("submit: velocity check: %w", err)
		}
	}

	// Dial the isolated signer over loopback (no mTLS in slice 1); grpc.NewClient
	// is lazy, so a bad address surfaces on the first RPC, not here.
	conn, err := grpc.NewClient(cfg.SignerGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("submit: dial signer at %s: %w", cfg.SignerGRPCAddr, err)
	}
	defer func() { _ = conn.Close() }()
	sc := newSignerClient(signerpb.NewSignerServiceClient(conn))

	// Dial the chain node.
	ethClient, err := ethclient.DialContext(ctx, cfg.ChainRPCURL)
	if err != nil {
		return fmt.Errorf("submit: dial chain rpc at %s: %w", cfg.ChainRPCURL, err)
	}
	defer ethClient.Close()

	adapter, err := evm.NewAdapter(ethClient, sc, evmCfg, logger)
	if err != nil {
		return fmt.Errorf("submit: build adapter: %w", err)
	}

	// Fail fast on the wrong network before anything is signed: a signer key is
	// bound to one chain id, and a mismatched RPC must not sign a live-value tx.
	if err := adapter.VerifyChainID(ctx); err != nil {
		return fmt.Errorf("submit: verify chain id: %w", err)
	}

	txHash, err := adapter.Submit(ctx, chain.PaymentIntent{
		KeyID:  keyID,
		Asset:  *assetFlag,
		To:     *toFlag,
		Amount: amount,
	})
	if err != nil {
		return fmt.Errorf("submit: %w", err)
	}

	fmt.Println(txHash)

	// With no --payment-id this is where the command ends: hash printed, Postgres
	// untouched — the legacy contract. With one, persist the payment↔tx-hash link
	// so the chainwatcher can settle it once the tx confirms.
	if *paymentIDFlag != "" {
		sqlDB, err := sql.Open("postgres", cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("submit: tx %s broadcast succeeded but linking to payment %s failed: open database: %w", txHash, paymentID, err)
		}
		defer func() { _ = sqlDB.Close() }()

		recorder := settlement.NewRecorder(db.New(sqlDB))
		if err := recorder.Link(ctx, paymentID, string(txHash)); err != nil {
			return fmt.Errorf("submit: tx %s broadcast succeeded but linking to payment %s failed — reconcile manually: %w", txHash, paymentID, err)
		}
		fmt.Printf("linked settlement: payment %s -> %s\n", paymentID, txHash)
	}
	return nil
}
