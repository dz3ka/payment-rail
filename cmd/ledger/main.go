// Command ledger owns Payment Rail's double-entry ledger: accounts, journal
// entries, and derived balances in Postgres — the internal source of truth.
//
// M0: entrypoint skeleton only. The gRPC ledger service and its serializable
// journal transactions arrive in milestone M1.
package main

import (
	"context"
	"log/slog"

	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/service"
)

func main() {
	service.Run("ledger", func(ctx context.Context, _ config.Config, log *slog.Logger) error {
		log.Info("ledger ready (M0 skeleton: no journal yet)")
		<-ctx.Done()
		return nil
	})
}
