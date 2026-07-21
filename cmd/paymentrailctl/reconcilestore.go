package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/dz3ka/payment-rail/internal/chain"
	"github.com/dz3ka/payment-rail/internal/chain/evm"
	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/reconcile"
)

// buildReconcileRegistry resolves the treasury set the reconcile command reads
// on-chain balances for: an explicit JSON manifest when PAYMENT_RAIL_RECONCILE_TREASURIES
// is set, else a single-entry fallback derived from the Chain* config. It fails
// CLOSED — a bad manifest, or a missing/invalid Chain* fallback address — returns
// an error rather than an empty registry that would silently reconcile against
// nothing. It touches no network, so the caller runs it BEFORE any dial.
func buildReconcileRegistry(cfg config.Config) (reconcile.Registry, error) {
	if cfg.ReconcileTreasuries == "" {
		return reconcile.SingleEntry(cfg.ChainFromAddress, "USDC", cfg.ChainUSDCAddress)
	}
	return reconcile.LoadRegistry(cfg.ReconcileTreasuries)
}

// newBalanceReader dials the chain node and wraps the client in the EVM
// BalanceReader (the chain.BalanceReader port). It mirrors broadcast.go's dialing
// — an ethclient over cfg.ChainRPCURL, which satisfies the evm ethRPC seam — and
// returns a close func the caller defers so the connection is always released.
// The RPC URL is validated BEFORE the dial so a missing endpoint fails with the
// exact env var to set rather than a murky transport error deeper in.
func newBalanceReader(ctx context.Context, cfg config.Config, log *slog.Logger) (chain.BalanceReader, func(), error) {
	if cfg.ChainRPCURL == "" {
		return nil, func() {}, errors.New("PAYMENT_RAIL_CHAIN_RPC_URL is required")
	}
	ethClient, err := ethclient.DialContext(ctx, cfg.ChainRPCURL)
	if err != nil {
		// Redact to scheme://host: a managed-node URL can embed an API key in its
		// path/query, and this error is printed to stderr. Mirrors the endpoint
		// redaction in evm.BalanceReader so a key never reaches a log or report.
		return nil, func() {}, fmt.Errorf("dial chain rpc at %s: %w", redactEndpoint(cfg.ChainRPCURL), err)
	}
	return evm.NewBalanceReader(ethClient, log), ethClient.Close, nil
}

// redactEndpoint returns the scheme://host of a node URL, dropping any path or
// query that could carry a secret. On a parse failure it returns a fixed label
// rather than risk echoing the raw (possibly key-bearing) string.
func redactEndpoint(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "the configured endpoint"
	}
	return u.Scheme + "://" + u.Host
}
