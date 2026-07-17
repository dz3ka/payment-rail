package payments

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/db"
)

// IdempotencyStore backs at-most-once request handling: a client-supplied key
// claims an in-flight slot, the handler runs once, and its response is cached so
// retries of the same key replay it verbatim instead of re-running the payment.
// It operates on the pool directly — each method is a single statement — because
// the claim/complete lifecycle is deliberately separate from the payment's own
// transaction (a payment must not roll back just because caching its response
// failed).
type IdempotencyStore struct {
	db *sql.DB
}

// NewIdempotencyStore builds a store over an already-open pool.
func NewIdempotencyStore(db *sql.DB) *IdempotencyStore {
	return &IdempotencyStore{db: db}
}

// BeginResult reports whether Begin claimed a fresh key. When Fresh is false the
// key was already taken and Existing holds the stored row, so the handler can
// compare request hashes and replay the cached response.
type BeginResult struct {
	Fresh    bool
	Existing db.IdempotencyKey
}

// Begin claims key for an in-flight request, seeding its request hash. The INSERT
// is ON CONFLICT DO NOTHING: a returned row means the claim is ours (Fresh), and
// sql.ErrNoRows means the key already exists, in which case the stored row is
// fetched and returned as Existing so the caller resolves the retry.
func (s *IdempotencyStore) Begin(ctx context.Context, key string, hash []byte) (BeginResult, error) {
	q := db.New(s.db)

	row, err := q.InsertIdempotencyKey(ctx, db.InsertIdempotencyKeyParams{
		Key:         key,
		RequestHash: hash,
	})
	if err == nil {
		return BeginResult{Fresh: true, Existing: row}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return BeginResult{}, fmt.Errorf("payments: begin idempotency key: %w", err)
	}

	existing, err := q.GetIdempotencyKey(ctx, key)
	if err != nil {
		return BeginResult{}, fmt.Errorf("payments: load idempotency key: %w", err)
	}
	return BeginResult{Fresh: false, Existing: existing}, nil
}

// Complete caches the handler's outcome under key so future retries replay it:
// it stores the HTTP status, the response body, the resulting payment id, and
// marks the key completed. paymentID may be uuid.Nil for a request that produced
// no payment (e.g. a validation error the caller still wants replayed).
func (s *IdempotencyStore) Complete(ctx context.Context, key string, status int, body []byte, paymentID uuid.UUID) error {
	err := db.New(s.db).CompleteIdempotencyKey(ctx, db.CompleteIdempotencyKeyParams{
		Key:            key,
		ResponseStatus: sql.NullInt32{Int32: int32(status), Valid: true},
		ResponseBody:   body,
		PaymentID:      uuid.NullUUID{UUID: paymentID, Valid: paymentID != uuid.Nil},
	})
	if err != nil {
		return fmt.Errorf("payments: complete idempotency key: %w", err)
	}
	return nil
}

// Delete releases a claim whose handler failed, so a transient error does not
// leave the key stuck in_flight and block every retry.
func (s *IdempotencyStore) Delete(ctx context.Context, key string) error {
	if err := db.New(s.db).DeleteIdempotencyKey(ctx, key); err != nil {
		return fmt.Errorf("payments: delete idempotency key: %w", err)
	}
	return nil
}

// SweepExpired deletes keys older than olderThan and returns how many were
// removed. The cutoff is computed once from the current wall clock; callers hold
// keys only as long as retries are plausible.
func (s *IdempotencyStore) SweepExpired(ctx context.Context, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan)
	n, err := db.New(s.db).DeleteExpiredIdempotencyKeys(ctx, cutoff)
	if err != nil {
		return 0, fmt.Errorf("payments: sweep idempotency keys: %w", err)
	}
	return n, nil
}

// RunSweeper deletes expired keys every interval until ctx is canceled, giving
// the idempotency table a bounded size without a separate cron. A failed sweep
// is logged and the loop continues — the next tick retries. It returns when ctx
// is done; the caller owns ctx's lifetime.
func (s *IdempotencyStore) RunSweeper(ctx context.Context, interval, ttl time.Duration, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := s.SweepExpired(ctx, ttl)
			if err != nil {
				log.ErrorContext(ctx, "idempotency sweep failed", "err", err)
				continue
			}
			if n > 0 {
				log.InfoContext(ctx, "idempotency keys swept", "deleted", n)
			}
		}
	}
}
