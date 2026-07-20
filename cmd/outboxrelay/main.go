// Command outboxrelay claims unsent transactional-outbox rows and forwards each
// envelope verbatim to Kafka, preserving per-aggregate order.
//
// It runs the poll loop from internal/outbox: every tick it drains one batch of
// unsent rows inside a single ledger transaction (claim -> publish -> mark). A
// publish failure rolls the batch back so the rows stay unsent and retry next tick
// — publishing is at-least-once. The Kafka client lives only in kafka.go behind the
// outbox.Producer port, so this wiring stays client-free.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/ledger"
	"github.com/dz3ka/payment-rail/internal/outbox"
	"github.com/dz3ka/payment-rail/internal/service"
)

func main() {
	service.Run("outboxrelay", func(ctx context.Context, cfg config.Config, log *slog.Logger) error {
		// The ledger DB backs the drain tx: each batch is claimed, published, and
		// marked sent inside one ExecTx so a publish error rolls the whole batch back.
		sqlDB, err := sql.Open("postgres", cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("outboxrelay: open database: %w", err)
		}
		defer sqlDB.Close()

		if err := sqlDB.PingContext(ctx); err != nil {
			return fmt.Errorf("outboxrelay: ping database: %w", err)
		}

		store := ledger.NewSQLStore(sqlDB)

		prod := newKafkaProducer(cfg.KafkaBrokers, outbox.DefaultTopic)
		defer func() { _ = prod.Close() }()

		// Log the non-secret wiring only — never the payloads, which carry amounts.
		log.Info("outboxrelay draining",
			"brokers", cfg.KafkaBrokers,
			"topic", outbox.DefaultTopic,
			"poll_interval", cfg.OutboxPollInterval.String(),
		)

		relay := outbox.NewRelay(store, prod, outbox.DefaultBatchSize, cfg.OutboxPollInterval, log)
		return relay.Run(ctx)
	})
}
