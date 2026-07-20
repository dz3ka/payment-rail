-- name: InsertOutboxEvent :exec
-- Appended in the same transaction as the aggregate write it describes, so the
-- domain change and its intent-to-publish commit atomically.
INSERT INTO outbox (id, event_type, aggregate_id, payload)
VALUES ($1, $2, $3, $4);

-- name: ClaimUnsentOutbox :many
-- The relay's claim scan: oldest unsent rows first, FOR UPDATE SKIP LOCKED so
-- concurrent relay workers each grab a disjoint batch without blocking.
SELECT * FROM outbox
WHERE sent_at IS NULL
ORDER BY created_at
LIMIT $1
FOR UPDATE SKIP LOCKED;

-- name: MarkOutboxSent :execrows
-- Stamps a claimed batch as published; ANY($1) marks the whole batch in one
-- round-trip after Kafka acks.
UPDATE outbox SET sent_at = now()
WHERE id = ANY($1::uuid[]);
