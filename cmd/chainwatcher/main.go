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
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"

	"github.com/dz3ka/payment-rail/internal/chain"
	"github.com/dz3ka/payment-rail/internal/chain/evm"
	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/ledger"
	"github.com/dz3ka/payment-rail/internal/service"
	"github.com/dz3ka/payment-rail/internal/settlement"
)

// defaultFinalityDepth is Ethereum's ~2-epoch (64-block) finality distance: past
// this depth a canonical block is economically final. It is deliberately
// hardcoded this slice; the knob is deferred until a second chain with a
// different finality distance forces it into config.
const defaultFinalityDepth uint64 = 64

// txTracker is the slice of the watcher that seed drives — the two idempotent,
// concurrency-safe entry points that register a settlement for observation. Both
// lock the watcher's internal mutex, so seed is safe to call from the re-scan
// goroutine while the poll loop runs. *evm.Watcher satisfies it; the interface
// exists so seed's branch logic is unit-testable with a spy.
type txTracker interface {
	Track(tx chain.TxHash) error
	Resume(tx chain.TxHash, blockHash string, blockNumber uint64) error
}

// seed registers one pending-settlement row with the watcher, choosing the
// recovery path from the row's persisted state. A row already recorded settled
// at a known anchor resumes as a Confirmed anchor, so an in-flight reorg is still
// caught without re-emitting the settle that already landed; everything else —
// including a legacy settled row whose anchor predates the columns and is still
// NULL — tracks as pending, the money-safe default. Both calls are idempotent, so
// seed is a no-op for a tx already in the watcher's set and safe to re-run.
func seed(w txTracker, row db.Settlement) error {
	tx := chain.TxHash(row.TxHash)
	if row.Status == "settled" && row.SettledBlockHash.Valid {
		return w.Resume(tx, row.SettledBlockHash.String, uint64(row.SettledBlockNumber.Int64))
	}
	return w.Track(tx)
}

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

		w, err := evm.NewWatcher(client, cfg.WatcherConfirmations, defaultFinalityDepth, cfg.WatcherPollInterval, log)
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

		querier := db.New(sqlDB)

		// Seed the tracking set at startup from the settlements still pending, then
		// keep it fresh with the periodic re-scan below. Startup and re-scan share
		// one path (seed): a settled row with a persisted anchor resumes as a
		// Confirmed anchor, everything else tracks as pending.
		rows, err := querier.ListPendingSettlements(ctx)
		if err != nil {
			return fmt.Errorf("chainwatcher: list pending settlements: %w", err)
		}
		tracked := 0
		for _, row := range rows {
			// Log-and-continue on a per-row failure: one malformed hash must not
			// keep the watcher from tracking every other pending settlement.
			if err := seed(w, row); err != nil {
				log.Error("chainwatcher: seed pending settlement", "tx_hash", row.TxHash, "error", err)
				continue
			}
			tracked++
		}
		log.Info("chainwatcher tracking", "settlements", tracked)

		// Run the watcher and the re-scan under one context: a termination signal
		// cancels both. The re-scan closes the startup-only gap — payments
		// submitted while chainwatcher is already running are now picked up on the
		// next tick rather than waiting for a restart. Idempotent Track/Resume make
		// re-seeding an already-tracked tx a no-op, and both lock the watcher's
		// mutex, so re-seeding in parallel with the poll loop is safe.
		g, gctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			return rescan(gctx, querier, w, cfg.WatcherPollInterval, log)
		})
		g.Go(func() error {
			return w.Run(gctx, sink)
		})
		// A cancelled context is a clean shutdown, not a failure: the watcher's Run
		// already returns nil on cancel, and rescan surfaces ctx.Err(), so filter
		// the expected context.Canceled and report only a genuine error.
		if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})
}

// rescan re-seeds the watcher from the pending settlements on every tick until
// the context is cancelled, closing the startup-only gap so newly-submitted
// payments are tracked without a restart. A transient query error is logged and
// skipped rather than killing the watcher — one failed poll must not stop
// confirmation tracking; the row stays pending and the next tick retries. It
// returns ctx.Err() on cancel, matching the errgroup goroutine convention.
func rescan(ctx context.Context, querier *db.Queries, w txTracker, interval time.Duration, log *slog.Logger) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			rows, err := querier.ListPendingSettlements(ctx)
			if err != nil {
				// A cancel racing the query surfaces here as context.Canceled;
				// let the next loop's ctx.Done() report the clean shutdown
				// instead of logging it as a query failure.
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Error("chainwatcher: rescan list pending settlements", "error", err)
				continue
			}
			reseeded := 0
			for _, row := range rows {
				if err := seed(w, row); err != nil {
					log.Error("chainwatcher: rescan seed pending settlement", "tx_hash", row.TxHash, "error", err)
					continue
				}
				reseeded++
			}
			// Debug level: one line per tick is routine chatter, but on-call can
			// enable debug to confirm the re-scan is picking up new payments.
			log.Debug("chainwatcher rescan", "settlements", reseeded)
		}
	}
}
