// Command webhookd delivers domain events to subscriber endpoints with
// HMAC-signed payloads, exponential backoff, and a dead-letter path after N
// attempts.
//
// It runs two loops under one errgroup: (a) a Kafka consume loop that reads outbox
// event envelopes and fans each out to pending delivery rows (kafka.go), and (b)
// the internal/webhook delivery worker that claims due rows and POSTs them to
// subscribers via the HTTP Sender adapter (httpsender.go). The Kafka client and
// net/http live only in this package, behind the webhook package's ports, so the
// domain stays transport-free — mirroring cmd/outboxrelay's producer isolation.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"

	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/service"
	"github.com/dz3ka/payment-rail/internal/webhook"
)

func main() {
	service.Run("webhookd", func(ctx context.Context, cfg config.Config, log *slog.Logger) error {
		// One DB handle backs both loops: the consumer's fan-out inserts and the
		// worker's claim/mark queries are independent statements (no shared tx).
		sqlDB, err := sql.Open("postgres", cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("webhookd: open database: %w", err)
		}
		defer sqlDB.Close()

		if err := sqlDB.PingContext(ctx); err != nil {
			return fmt.Errorf("webhookd: ping database: %w", err)
		}

		q := db.New(sqlDB)

		consumer := newKafkaConsumer(cfg.KafkaBrokers, webhook.DefaultTopic, webhook.DefaultConsumerGroup)
		defer func() { _ = consumer.Close() }()

		sender := newHTTPSender()
		worker := webhook.NewWorker(q, sender, cfg.WebhookPollInterval, log)

		// Log the non-secret wiring only — never payloads (amounts) or signatures.
		log.Info("webhookd starting",
			"brokers", cfg.KafkaBrokers,
			"topic", webhook.DefaultTopic,
			"group", webhook.DefaultConsumerGroup,
			"poll_interval", cfg.WebhookPollInterval.String(),
		)

		// Run the consume loop and the delivery worker under one context: if either
		// returns a genuine error it cancels the sibling and service.Run exits
		// non-zero so the supervisor restarts the process (the consumer resumes from
		// its last committed offset). A cancelled context on shutdown is clean.
		g, gctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			return consumer.run(gctx, q, log)
		})
		g.Go(func() error {
			return worker.Run(gctx)
		})
		if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})
}
