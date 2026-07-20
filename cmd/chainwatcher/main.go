// Command chainwatcher tracks on-chain confirmations per EVM chain, applies
// ledger effects at a configurable confirmation depth, and detects reorgs to
// roll back and reapply those effects correctly.
//
// M3 slice 1: dials the configured EVM node and runs the confirmation/reorg
// watcher against it. The feed of broadcast transactions to Track (from the
// payments path) arrives in a later slice; until then the watcher runs against
// an empty tracking set so the wiring, config, and shutdown path are real.
package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/dz3ka/payment-rail/internal/chain/evm"
	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/service"
)

func main() {
	service.Run("chainwatcher", func(ctx context.Context, cfg config.Config, log *slog.Logger) error {
		if cfg.ChainRPCURL == "" {
			// No chain configured: idle cleanly rather than dial an empty URL.
			log.Info("chainwatcher idle: no chain RPC configured (set PAYMENT_RAIL_CHAIN_RPC_URL)")
			<-ctx.Done()
			return nil
		}

		// Dial the chain node. For http(s) endpoints DialContext is lazy, so a
		// dead endpoint first surfaces on the poll path, not here; a malformed URL
		// can still error synchronously. Either way the error is redacted before
		// it escapes — the raw *url.Error embeds the API-key-bearing endpoint.
		client, err := ethclient.DialContext(ctx, cfg.ChainRPCURL)
		if err != nil {
			return fmt.Errorf("chainwatcher: dial chain rpc: %s", evm.RedactRPCError(err))
		}
		defer client.Close()

		w, err := evm.NewWatcher(client, cfg.WatcherConfirmations, cfg.WatcherPollInterval, log)
		if err != nil {
			return fmt.Errorf("chainwatcher: build watcher: %w", err)
		}

		// The RPC URL is intentionally not logged: managed-node endpoints embed
		// API keys, so only the non-secret knobs go to the log.
		log.Info("chainwatcher watching",
			"confirmations", cfg.WatcherConfirmations,
			"poll_interval", cfg.WatcherPollInterval.String(),
		)
		return w.Run(ctx)
	})
}
