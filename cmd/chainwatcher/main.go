// Command chainwatcher tracks on-chain confirmations per EVM chain, applies
// ledger effects at a configurable confirmation depth, and detects reorgs to
// roll back and reapply those effects correctly.
//
// M3 slice 2: dials the configured EVM node and runs the confirmation/reorg
// watcher against it, feeding every emitted Status into the settlement Sink so
// confirmations and reorgs become ledger truth. The tracking set is seeded at
// startup from the settlements still pending in Postgres (see below).
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/ethclient"
	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/chain"
	"github.com/dz3ka/payment-rail/internal/chain/evm"
	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/ledger"
	"github.com/dz3ka/payment-rail/internal/service"
	"github.com/dz3ka/payment-rail/internal/settlement"
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

		// The ledger DB backs both effects the watcher now drives: the Sink posts
		// settlement/reversal entries through it, and the Track feed below is
		// seeded from the settlements it still holds as pending.
		sqlDB, err := sql.Open("postgres", cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("chainwatcher: open database: %w", err)
		}
		defer sqlDB.Close()

		sink := settlement.NewSink(ledger.NewSQLStore(sqlDB), log)

		// Seed the tracking set ONCE, at startup, from the settlements still
		// pending. KNOWN LIMITATION: payments submitted while chainwatcher is
		// already running are not picked up until the next restart. A periodic
		// re-scan is a documented follow-up — it must wait until the watcher's
		// `tracked` map is made concurrency-safe, since a parallel Track would
		// race the poll loop that reads that map.
		rows, err := db.New(sqlDB).ListPendingSettlements(ctx)
		if err != nil {
			return fmt.Errorf("chainwatcher: list pending settlements: %w", err)
		}
		tracked := 0
		for _, row := range rows {
			// Log-and-continue on a per-row failure: one malformed hash must not
			// keep the watcher from tracking every other pending settlement.
			if err := w.Track(chain.TxHash(row.TxHash)); err != nil {
				log.Error("chainwatcher: track pending settlement", "tx_hash", row.TxHash, "error", err)
				continue
			}
			tracked++
		}
		log.Info("chainwatcher tracking", "settlements", tracked)

		return w.Run(ctx, sink)
	})
}
