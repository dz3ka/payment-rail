-- name: InsertIdempotencyKey :one
-- Claims a key for an in-flight request. ON CONFLICT DO NOTHING means a key that
-- already exists returns zero rows, so the caller gets sql.ErrNoRows — that is
-- the "key already taken" signal, which the handler resolves against the stored
-- request_hash/response rather than re-running the operation.
INSERT INTO idempotency_keys (key, request_hash, status)
VALUES ($1, $2, 'in_flight')
ON CONFLICT DO NOTHING
RETURNING *;

-- name: GetIdempotencyKey :one
SELECT * FROM idempotency_keys
WHERE key = $1;

-- name: CompleteIdempotencyKey :exec
-- Caches the response so subsequent retries of the same key replay it verbatim.
UPDATE idempotency_keys
SET status = 'completed', response_status = $2, response_body = $3, payment_id = $4, completed_at = now()
WHERE key = $1;

-- name: DeleteIdempotencyKey :exec
-- Releases a claim whose handler failed, so the key isn't stuck in_flight.
DELETE FROM idempotency_keys
WHERE key = $1;

-- name: DeleteExpiredIdempotencyKeys :execrows
-- TTL sweeper: drop keys created before the caller-supplied cutoff.
DELETE FROM idempotency_keys
WHERE created_at < $1;
