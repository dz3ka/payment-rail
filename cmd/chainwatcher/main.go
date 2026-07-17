// Command chainwatcher tracks on-chain confirmations per EVM chain, applies
// ledger effects at a configurable confirmation depth, and detects reorgs to
// roll back and reapply those effects correctly.
//
// M0: entrypoint skeleton only. The per-chain watchers, worker pools, and
// reorg-safe finality tracking arrive in milestone M3.
package main

import (
	"context"
	"log/slog"

	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/service"
)

func main() {
	service.Run("chainwatcher", func(ctx context.Context, cfg config.Config, log *slog.Logger) error {
		log.Info("chainwatcher ready (M0 skeleton: watching no chains yet)")
		<-ctx.Done()
		return nil
	})
}
