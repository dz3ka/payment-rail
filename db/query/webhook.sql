-- name: FanOutDelivery :execrows
-- Insert one pending delivery per active subscription matching the event type.
-- Idempotent on redelivery via the (event_id, subscription_id) unique constraint.
INSERT INTO webhook_deliveries (event_id, subscription_id, event_type, payload)
SELECT $1, s.id, $2, $3
FROM webhook_subscriptions s
WHERE s.active AND $2 = ANY(s.event_types)
ON CONFLICT (event_id, subscription_id) DO NOTHING;

-- name: ClaimDueDeliveries :many
-- Atomically claim due pending deliveries and push their next_attempt_at forward
-- by a lease so a crashed/slow worker's rows are not re-claimed until the lease expires.
UPDATE webhook_deliveries d
SET next_attempt_at = now() + make_interval(secs => sqlc.arg(lease_seconds)::int),
    updated_at = now()
FROM webhook_subscriptions s
WHERE d.subscription_id = s.id
  AND d.id IN (
      SELECT id FROM webhook_deliveries
      WHERE status = 'pending' AND next_attempt_at <= now()
      ORDER BY next_attempt_at
      LIMIT sqlc.arg(claim_limit)::int
      FOR UPDATE SKIP LOCKED
  )
RETURNING d.id, d.event_id, d.attempts, d.payload, s.url, s.signing_secret;

-- name: MarkDeliverySucceeded :exec
UPDATE webhook_deliveries
SET status = 'delivered', attempts = $2, last_status_code = $3,
    last_error = NULL, updated_at = now()
WHERE id = $1;

-- name: MarkDeliveryRetry :exec
UPDATE webhook_deliveries
SET attempts = $2, next_attempt_at = $3, last_status_code = $4,
    last_error = $5, updated_at = now()
WHERE id = $1;

-- name: MarkDeliveryDeadLettered :exec
UPDATE webhook_deliveries
SET status = 'dead_letter', attempts = $2, last_status_code = $3,
    last_error = $4, updated_at = now()
WHERE id = $1;

-- name: ReplayDeadLettered :execrows
-- Re-drive every dead-lettered delivery for one subscription (operator action
-- after fixing a broken endpoint): reset to pending, clear error, deliver now.
UPDATE webhook_deliveries
SET status = 'pending', attempts = 0, next_attempt_at = now(),
    last_error = NULL, updated_at = now()
WHERE status = 'dead_letter' AND subscription_id = $1;
