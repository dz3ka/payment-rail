package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/db"
)

// schemaVersion is the current envelope schema. It is stamped into every payload
// so a consumer can dispatch on shape; bump it only alongside a breaking change
// to the envelope's fields.
const schemaVersion = 1

// DefaultTopic and DefaultBatchSize are the relay's wiring defaults, exported here
// so the outbox package owns the event-transport constants in one place.
const (
	// DefaultTopic is the Kafka topic the relay publishes the envelope to.
	DefaultTopic = "payment-rail.events"
	// DefaultBatchSize is how many unsent rows the relay claims per poll.
	DefaultBatchSize = 100
)

// envelope is the JSON shape written to the outbox payload column and later
// published verbatim as the Kafka value. The field order below is the on-the-wire
// order; keep it stable — consumers and the envelope_test assert on it.
type envelope struct {
	ID            string    `json:"id"`
	Type          string    `json:"type"`
	AggregateType string    `json:"aggregate_type"`
	AggregateID   string    `json:"aggregate_id"`
	OccurredAt    time.Time `json:"occurred_at"`
	SchemaVersion int       `json:"schema_version"`
	Data          any       `json:"data"`
}

// Emit wraps e in a versioned envelope and appends it to the outbox table through
// q, so it commits in whatever transaction q belongs to. The envelope's id and
// the row's id are the same generated uuid, so the relay and the payload agree on
// the event's identity. aggregate_type is derived from Type's prefix (the segment
// before the first "."), never passed in. A marshal or insert failure wraps with
// the event type for context and propagates, rolling back the surrounding tx.
func Emit(ctx context.Context, q db.Querier, e Event) error {
	id := uuid.New()
	env := envelope{
		ID:            id.String(),
		Type:          e.Type,
		AggregateType: strings.SplitN(e.Type, ".", 2)[0],
		AggregateID:   e.AggregateID,
		OccurredAt:    time.Now().UTC(),
		SchemaVersion: schemaVersion,
		Data:          e.Data,
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("outbox: emit %s: %w", e.Type, err)
	}
	if err := q.InsertOutboxEvent(ctx, db.InsertOutboxEventParams{
		ID:          id,
		EventType:   e.Type,
		AggregateID: e.AggregateID,
		Payload:     payload,
	}); err != nil {
		return fmt.Errorf("outbox: emit %s: %w", e.Type, err)
	}
	return nil
}
