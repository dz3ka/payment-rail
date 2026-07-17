// Command signer is the network-isolated signing service. It holds keys, signs
// only well-formed payloads, and enforces per-key spend limits independently of
// the rest of the system.
//
// M0: entrypoint skeleton only. gRPC signing and key handling arrive in
// milestone M2. No keys are loaded or configured yet.
package main

import (
	"context"
	"log/slog"

	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/service"
)

func main() {
	service.Run("signer", func(ctx context.Context, cfg config.Config, log *slog.Logger) error {
		log.Info("signer ready (M0 skeleton: no keys, no signing yet)")
		<-ctx.Done()
		return nil
	})
}
