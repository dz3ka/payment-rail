package webhook

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dz3ka/payment-rail/internal/db"
)

// SendResult is what a Sender reports for a completed HTTP round-trip. StatusCode
// is meaningful only when Send returns a nil error (a response was received).
type SendResult struct {
	StatusCode int
}

// Sender is the transport port the worker calls to deliver one signed webhook.
// It is implemented by the HTTP adapter in cmd/webhookd; the interface keeps this
// package free of net/http and lets the delivery outcomes be tested with a fake.
//
// A nil error means a response was received (StatusCode is set, even for a 5xx);
// a non-nil error means the request never completed (transport failure, timeout)
// and StatusCode is not meaningful. Implementations must never include the
// signing secret or the payload body in a returned error.
type Sender interface {
	Send(ctx context.Context, url string, body []byte, sig, eventID string, attempt int) (SendResult, error)
}

// workerQuerier is the narrow slice of db.Querier the delivery worker drives:
// claim due rows under a lease, then stamp each with its terminal or retry state.
// *db.Queries satisfies it.
type workerQuerier interface {
	ClaimDueDeliveries(ctx context.Context, arg db.ClaimDueDeliveriesParams) ([]db.ClaimDueDeliveriesRow, error)
	MarkDeliverySucceeded(ctx context.Context, arg db.MarkDeliverySucceededParams) error
	MarkDeliveryRetry(ctx context.Context, arg db.MarkDeliveryRetryParams) error
	MarkDeliveryDeadLettered(ctx context.Context, arg db.MarkDeliveryDeadLetteredParams) error
}

// outcome is the terminal decision for one delivery attempt.
type outcome int

const (
	outcomeSucceeded outcome = iota
	outcomeRetry
	outcomeDeadLetter
)

// classify decides a delivery's fate from the send result. A 2xx with no error
// succeeds; anything else retries until newAttempts hits the dead-letter
// threshold. It is pure so the branch is testable without a worker.
func classify(newAttempts int32, res SendResult, err error) outcome {
	if err == nil && res.StatusCode >= 200 && res.StatusCode < 300 {
		return outcomeSucceeded
	}
	if newAttempts >= maxAttempts {
		return outcomeDeadLetter
	}
	return outcomeRetry
}

// Worker polls the delivery table and pushes due rows to subscriber endpoints.
type Worker struct {
	q        workerQuerier
	sender   Sender
	interval time.Duration
	now      func() time.Time
	log      *slog.Logger
}

// NewWorker wires a delivery worker. interval is the poll cadence; now defaults
// to time.Now (overridden in tests to pin backoff math).
func NewWorker(q workerQuerier, sender Sender, interval time.Duration, log *slog.Logger) *Worker {
	return &Worker{q: q, sender: sender, interval: interval, now: time.Now, log: log}
}

// Run polls until the context is cancelled, draining one batch per tick. A drain
// error that is not context cancellation is logged and the loop continues: the
// affected rows keep their lease and are re-claimed after it expires, so a
// transient DB blip never kills the worker. Context cancellation is a clean
// shutdown returning nil.
func (w *Worker) Run(ctx context.Context) error {
	t := time.NewTicker(w.interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := w.drainOnce(ctx); err != nil &&
				!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				w.log.Error("webhook: delivery drain failed", "err", err)
			}
		}
	}
}

// drainOnce is the pure, tested seam: claim the due batch under a lease, then
// deliver each row and record its outcome. A DB error (claim or mark) aborts the
// remaining batch and propagates; those rows stay leased and are retried once the
// lease expires. Send failures are not drain errors — they are recorded as retry
// or dead-letter outcomes.
func (w *Worker) drainOnce(ctx context.Context) error {
	rows, err := w.q.ClaimDueDeliveries(ctx, db.ClaimDueDeliveriesParams{
		LeaseSeconds: leaseSeconds,
		ClaimLimit:   claimBatch,
	})
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := w.deliver(ctx, row); err != nil {
			return err
		}
	}
	return nil
}

// deliver signs and sends one claimed row, then records the outcome. The returned
// error is only ever a DB mark failure; a Send failure is folded into a
// retry/dead-letter outcome. The signing secret and payload body are never
// logged and never written to last_error.
func (w *Worker) deliver(ctx context.Context, row db.ClaimDueDeliveriesRow) error {
	newAttempts := row.Attempts + 1
	sig := Sign([]byte(row.SigningSecret), w.now().Unix(), row.Payload)
	res, sendErr := w.sender.Send(ctx, row.Url, row.Payload, sig, row.EventID.String(), int(newAttempts))

	// A status code exists only when the request completed (sendErr == nil).
	statusCode := sql.NullInt32{}
	if sendErr == nil {
		statusCode = sql.NullInt32{Int32: int32(res.StatusCode), Valid: true}
	}

	switch classify(newAttempts, res, sendErr) {
	case outcomeSucceeded:
		w.log.Info("webhook: delivered",
			"delivery_id", row.ID, "event_id", row.EventID,
			"attempt", newAttempts, "http_status", res.StatusCode)
		return w.q.MarkDeliverySucceeded(ctx, db.MarkDeliverySucceededParams{
			ID:             row.ID,
			Attempts:       newAttempts,
			LastStatusCode: statusCode,
		})
	case outcomeDeadLetter:
		lastErr := deliveryError(res, sendErr)
		w.log.Warn("webhook: dead-lettered",
			"delivery_id", row.ID, "event_id", row.EventID,
			"attempt", newAttempts, "http_status", statusCodeOrZero(statusCode),
			"outcome", "dead_letter", "reason", lastErr.String)
		return w.q.MarkDeliveryDeadLettered(ctx, db.MarkDeliveryDeadLetteredParams{
			ID:             row.ID,
			Attempts:       newAttempts,
			LastStatusCode: statusCode,
			LastError:      lastErr,
		})
	default:
		lastErr := deliveryError(res, sendErr)
		next := w.now().Add(backoff(newAttempts))
		w.log.Warn("webhook: delivery failed, retrying",
			"delivery_id", row.ID, "event_id", row.EventID,
			"attempt", newAttempts, "http_status", statusCodeOrZero(statusCode),
			"outcome", "retry", "next_attempt_at", next, "reason", lastErr.String)
		return w.q.MarkDeliveryRetry(ctx, db.MarkDeliveryRetryParams{
			ID:             row.ID,
			Attempts:       newAttempts,
			NextAttemptAt:  next,
			LastStatusCode: statusCode,
			LastError:      lastErr,
		})
	}
}

// deliveryError renders a short, secret-free reason for a failed attempt: the
// transport error's message, or a "status NNN" description for a completed
// non-2xx response.
func deliveryError(res SendResult, err error) sql.NullString {
	if err != nil {
		return sql.NullString{String: err.Error(), Valid: true}
	}
	return sql.NullString{String: fmt.Sprintf("status %d", res.StatusCode), Valid: true}
}

// statusCodeOrZero unwraps a nullable status code for logging (0 = no response).
func statusCodeOrZero(c sql.NullInt32) int32 {
	if c.Valid {
		return c.Int32
	}
	return 0
}
