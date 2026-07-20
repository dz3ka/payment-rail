// Package webhook is the pure domain core of the webhook delivery service: the
// consumer fan-out handler that turns an outbox event into pending delivery rows,
// and the delivery worker that signs and pushes each due row to a subscriber
// endpoint with retry/backoff and dead-lettering.
//
// It holds NO transport: there is no net/http and no Kafka client here. Both
// seams are driven through narrow interfaces defined in this package (mirroring
// internal/outbox's outboxQuerier spy convention) so the fan-out and delivery
// logic are unit-testable without a database, broker, or HTTP server. The
// consume loop (Kafka reader) and the HTTP Sender adapter live in cmd/webhookd
// and call into the pure Handle/Worker exposed here.
package webhook

import (
	"time"

	"github.com/dz3ka/payment-rail/internal/outbox"
)

const (
	// DefaultConsumerGroup is the fixed Kafka consumer group / service identity
	// for the webhook consumer. It is a service constant, not configuration.
	DefaultConsumerGroup = "webhookd"
	// DefaultTopic is the topic the consumer reads outbox event envelopes from;
	// it must match the relay's publish topic.
	DefaultTopic = outbox.DefaultTopic

	// HTTPTimeout bounds a single webhook POST; consumed by the cmd/webhookd
	// HTTP Sender adapter.
	HTTPTimeout = 10 * time.Second
	// RespBodyCap is the max response-body byte count the Sender adapter reads
	// (bodies are only drained for connection reuse, never stored).
	RespBodyCap = 4096

	// Delivery request headers set by the Sender adapter.
	SignatureHeader = "X-Payment-Rail-Signature"
	EventIDHeader   = "X-Payment-Rail-Event-Id"
	AttemptHeader   = "X-Payment-Rail-Delivery-Attempt"

	// maxAttempts is the dead-letter threshold: once a delivery has failed this
	// many times it is parked in dead_letter instead of retried.
	maxAttempts = 8
	// backoffBase and backoffCap bound the exponential retry schedule.
	backoffBase = 2 * time.Second
	backoffCap  = 1 * time.Hour
	// leaseSeconds is how far ClaimDueDeliveries pushes a claimed row's
	// next_attempt_at forward, i.e. the crash-recovery window before an
	// unfinished row is re-claimed by another worker.
	leaseSeconds = 60
	// claimBatch is the max rows the worker claims per tick.
	claimBatch = 100
)
