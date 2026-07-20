package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"

	"github.com/dz3ka/payment-rail/internal/outbox"
)

// kafkaProducer is the only place in the repo that imports the Kafka client; it
// adapts a *kafka.Writer to the outbox.Producer port so the rest of the codebase
// stays client-free (ADR-0003: plain Kafka protocol, no Redpanda-only APIs).
type kafkaProducer struct {
	w *kafka.Writer
}

// Compile-time proof the adapter satisfies the port.
var _ outbox.Producer = (*kafkaProducer)(nil)

// newKafkaProducer constructs a synchronous, all-acks writer. It does not dial:
// kafka-go's Writer connects lazily on the first WriteMessages call, so this is
// pure construction and never blocks. Hash balancing routes each message by key
// so all events for one aggregate land on the same partition, preserving order.
func newKafkaProducer(brokers []string, topic string) *kafkaProducer {
	return &kafkaProducer{
		w: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		},
	}
}

// Publish maps the outbox messages to kafka messages and writes them
// synchronously; a nil return means the batch is committed on the broker, so the
// relay can safely mark those rows sent. An empty batch is a no-op with no broker
// round-trip. Any write error is wrapped so the relay leaves the rows unsent and
// retries them next tick (at-least-once).
func (p *kafkaProducer) Publish(ctx context.Context, msgs []outbox.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	kms := make([]kafka.Message, len(msgs))
	for i, m := range msgs {
		kms[i] = kafka.Message{Key: m.Key, Value: m.Value}
	}
	if err := p.w.WriteMessages(ctx, kms...); err != nil {
		return fmt.Errorf("outbox kafka: publish %d msgs: %w", len(msgs), err)
	}
	return nil
}

// Close releases the underlying writer's connections.
func (p *kafkaProducer) Close() error {
	return p.w.Close()
}
