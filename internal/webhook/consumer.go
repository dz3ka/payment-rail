package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/outbox"
)

// ErrPoisonMessage marks a Kafka value the consumer can never process — a
// malformed envelope or an unparseable event id. Handle returns it (via
// errors.Is) so the consume loop can skip the message and commit its offset,
// rather than redelivering a message that will always fail. It is distinct from
// a transient DB error, which Handle returns raw so the loop does NOT commit and
// the message is redelivered.
var ErrPoisonMessage = errors.New("webhook: poison message")

// fanOutQuerier is the narrow slice of db.Querier the consumer drives: insert one
// pending delivery per matching subscription. The interface exists so Handle's
// parse -> fan-out flow is unit-testable with a spy (mirrors outbox's
// outboxQuerier). *db.Queries satisfies it.
type fanOutQuerier interface {
	FanOutDelivery(ctx context.Context, arg db.FanOutDeliveryParams) (int64, error)
}

// Handle parses one Kafka message value (an outbox event envelope) and fans it
// out to pending webhook_deliveries rows — one per active subscription matching
// the event type. The raw value bytes are stored verbatim as the delivery
// payload and later POSTed verbatim, so the signature the worker computes covers
// exactly what the subscriber receives.
//
// A parse failure or an unparseable event id is a poison message: it wraps
// ErrPoisonMessage so the loop skips and commits. A FanOutDelivery DB error is
// returned raw so the loop leaves the offset uncommitted and the message
// redelivers (at-least-once; the fan-out insert is idempotent on redelivery).
// The payload body is never logged.
func Handle(ctx context.Context, q fanOutQuerier, value []byte, log *slog.Logger) error {
	env, err := outbox.ParseEnvelope(value)
	if err != nil {
		log.Error("webhook: skipping unparseable message", "err", err)
		return fmt.Errorf("%w: %w", ErrPoisonMessage, err)
	}
	eventID, err := uuid.Parse(env.ID)
	if err != nil {
		log.Error("webhook: skipping message with bad event id",
			"event_id", env.ID, "type", env.Type, "err", err)
		return fmt.Errorf("%w: bad event id %q: %w", ErrPoisonMessage, env.ID, err)
	}

	n, err := q.FanOutDelivery(ctx, db.FanOutDeliveryParams{
		EventID:   eventID,
		EventType: env.Type,
		Payload:   json.RawMessage(value),
	})
	if err != nil {
		// Transient: not wrapped in ErrPoisonMessage, so the loop redelivers.
		return fmt.Errorf("webhook: fan out %s: %w", eventID, err)
	}
	if n > 0 {
		log.Info("webhook: event fanned out",
			"event_id", eventID, "type", env.Type, "deliveries", n)
	}
	return nil
}
