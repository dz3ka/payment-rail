// Package outbox turns a domain state change into a durable, ordered intent to
// publish. A producer inside a write transaction hands Emit an Event; Emit wraps
// it in a versioned envelope and appends one row to the outbox table via the same
// db.Querier, so the aggregate write and its intent-to-publish commit atomically
// (the transactional-outbox pattern). A separate relay (later work) claims those
// rows and forwards the envelope verbatim to Kafka; this package never talks to
// Kafka and imports only stdlib, uuid, and internal/db.
package outbox

// Event is what a producer hands Emit. Data is the event-specific body, marshaled
// into the envelope's "data" field. Type is a namespaced "<aggregate>.<verb>"
// (e.g. "payment.created"); its prefix becomes the envelope's aggregate_type.
type Event struct {
	Type        string // namespaced "<aggregate>.<verb>", e.g. "payment.created"
	AggregateID string // stable per-aggregate key (Kafka message key)
	Data        any
}
