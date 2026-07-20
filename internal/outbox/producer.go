package outbox

import "context"

// Message is one event to publish. Key preserves per-aggregate ordering (the
// Kafka partition key); Value is the serialized envelope, published verbatim.
type Message struct {
	Key   []byte
	Value []byte
}

// Producer publishes a batch of messages. Publishing must be all-or-error from
// the relay's perspective: on a returned error the relay leaves the rows unsent
// and retries them next tick (at-least-once).
type Producer interface {
	Publish(ctx context.Context, msgs []Message) error
}
