package outbox

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/db"
)

// outboxQuerier is the narrow slice of db.Querier the relay drives: claim the
// oldest unsent rows, then stamp a published batch sent. The interface exists so
// drainBatch's claim -> publish -> mark flow is unit-testable with a spy (mirrors
// the txTracker/poll() convention in cmd/chainwatcher). *db.Queries satisfies it.
type outboxQuerier interface {
	ClaimUnsentOutbox(ctx context.Context, limit int32) ([]db.Outbox, error)
	MarkOutboxSent(ctx context.Context, ids []uuid.UUID) (int64, error)
}

// transactor is the transaction boundary the relay runs each drain inside. It is
// satisfied by *ledger.SQLStore; the interface keeps the relay free of *sql.DB and
// lets Run be exercised with a fake tx that just invokes fn.
type transactor interface {
	ExecTx(ctx context.Context, fn func(q db.Querier) error) error
}

// drainBatch is the pure, synchronous seam: claim the oldest unsent rows, publish
// them verbatim to the producer, then mark them sent — all inside the one tx the
// caller has already opened via q. Publish is held INSIDE that tx on purpose: a
// publish error returns before MarkOutboxSent, so the caller rolls the whole batch
// back and the rows stay unsent for the next tick (at-least-once). An empty claim
// is a clean no-op: no Publish round-trip, no Mark.
func drainBatch(ctx context.Context, q outboxQuerier, p Producer, batch int32) (published int, err error) {
	rows, err := q.ClaimUnsentOutbox(ctx, batch)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}

	// Preserve the claim's oldest-first order into the published batch so a
	// hash-keyed producer keeps per-aggregate ordering. Key is the aggregate id;
	// Value is the envelope payload, forwarded verbatim (never inspected/logged).
	msgs := make([]Message, len(rows))
	ids := make([]uuid.UUID, len(rows))
	for i, row := range rows {
		msgs[i] = Message{Key: []byte(row.AggregateID), Value: []byte(row.Payload)}
		ids[i] = row.ID
	}

	if err := p.Publish(ctx, msgs); err != nil {
		return 0, err
	}

	_, markErr := q.MarkOutboxSent(ctx, ids)
	return len(rows), markErr
}

// Relay periodically drains the transactional outbox to a Producer. Each tick runs
// one drainBatch inside a single tx; a claim/publish failure rolls that tick back
// and is retried on the next one, so publishing is at-least-once.
type Relay struct {
	store    transactor
	prod     Producer
	batch    int32
	interval time.Duration
	log      *slog.Logger
}

// NewRelay wires a relay. batch is the per-tick claim limit (DefaultBatchSize);
// interval is the poll cadence.
func NewRelay(store transactor, prod Producer, batch int32, interval time.Duration, log *slog.Logger) *Relay {
	return &Relay{store: store, prod: prod, batch: batch, interval: interval, log: log}
}

// Run polls until the context is cancelled, draining one batch per tick inside a
// tx. A drain error that is NOT context cancellation is logged and the loop
// continues: the rows stay unsent and retry next tick (the at-least-once path), so
// a transient broker or DB blip never kills the relay. Context cancellation is a
// clean shutdown — it returns nil and is filtered from the error log.
func (r *Relay) Run(ctx context.Context) error {
	t := time.NewTicker(r.interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			err := r.store.ExecTx(ctx, func(q db.Querier) error {
				n, e := drainBatch(ctx, q, r.prod, r.batch)
				if e == nil && n > 0 {
					r.log.Info("outbox drained", "rows", n)
				}
				return e
			})
			// A cancel racing the drain surfaces as context.Canceled/
			// DeadlineExceeded — that is the clean shutdown the next loop's
			// ctx.Done() reports, not a drain failure worth logging.
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				r.log.Error("outbox drain failed", "err", err)
			}
		}
	}
}
