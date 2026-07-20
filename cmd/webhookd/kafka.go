package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"

	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/webhook"
)

// kafkaConsumer is the only place in webhookd that imports the Kafka client; it
// wraps a *kafka.Reader and drives the pure webhook.Handle fan-out, keeping the
// rest of the service client-free (mirrors cmd/outboxrelay's kafkaProducer).
type kafkaConsumer struct {
	reader *kafka.Reader
}

// webhookFanOut is the narrow port the consume loop hands to webhook.Handle: a
// single FanOutDelivery call. *db.Queries satisfies it, so the wiring passes the
// concrete queries typed as this interface (mirrors the package's spy convention).
// It is structurally identical to webhook's own unexported fanOutQuerier, so a
// value typed as webhookFanOut is accepted directly by webhook.Handle.
type webhookFanOut interface {
	FanOutDelivery(ctx context.Context, arg db.FanOutDeliveryParams) (int64, error)
}

// newKafkaConsumer constructs a group-consuming reader. It does not dial:
// kafka-go connects lazily on the first FetchMessage, so this is pure
// construction. MinBytes 1 keeps latency low; MaxBytes caps a single fetch.
// GroupID enables managed offset commits via CommitMessages.
func newKafkaConsumer(brokers []string, topic, groupID string) *kafkaConsumer {
	return &kafkaConsumer{reader: kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 1 << 20,
	})}
}

// run consumes messages until ctx is cancelled. For each message it fans the
// event out to delivery rows via webhook.Handle, committing the offset only after
// a successful fan-out (or when the message is poison and safely skippable), so a
// crash before commit redelivers and the (event_id, subscription_id) unique
// constraint dedupes.
//
// Transient-failure offset handling: on a non-poison (DB) error we do NOT commit
// and we RETURN the wrapped error so the errgroup cancels the delivery worker and
// service.Run exits non-zero; the process supervisor restarts the reader, which
// resumes from the last committed offset and re-processes the uncommitted message.
// This is chosen over sleep-and-retry-same-message because it is the simplest
// strictly-correct at-least-once behaviour — re-processing is safe (the fan-out
// insert is idempotent on redelivery) and we never advance past an uncommitted
// message the way a bare `continue` would.
func (c *kafkaConsumer) run(ctx context.Context, q webhookFanOut, log *slog.Logger) error {
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			// On shutdown FetchMessage returns ctx.Canceled; the caller treats
			// that as a clean stop.
			return err
		}

		herr := webhook.Handle(ctx, q, m.Value, log)
		if herr != nil && !errors.Is(herr, webhook.ErrPoisonMessage) {
			// Transient (DB) failure: leave the offset uncommitted and bubble up
			// so the reader restarts from the last commit and redelivers.
			return fmt.Errorf("webhookd: fan-out failed at offset %d, redelivering: %w", m.Offset, herr)
		}

		// Success or safely-skippable poison: commit so the message is not
		// redelivered. A commit failure is fatal to the loop (bubble up to restart).
		if err := c.reader.CommitMessages(ctx, m); err != nil {
			log.Error("webhookd: commit offset failed", "err", err, "kafka_offset", m.Offset)
			return fmt.Errorf("webhookd: commit offset %d: %w", m.Offset, err)
		}
	}
}

// Close releases the reader's connections and commits any pending offsets.
func (c *kafkaConsumer) Close() error {
	return c.reader.Close()
}
