// Command webhookd delivers domain events to subscriber endpoints with
// HMAC-signed payloads, exponential backoff, and a dead-letter path after N
// attempts.
//
// M0: entrypoint skeleton only. The Kafka consumer and delivery dispatcher
// arrive in milestone M4.
package main

import (
	"context"
	"log/slog"

	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/service"
)

func main() {
	service.Run("webhookd", func(ctx context.Context, cfg config.Config, log *slog.Logger) error {
		log.Info("webhookd ready (M0 skeleton: no delivery yet)")
		<-ctx.Done()
		return nil
	})
}
