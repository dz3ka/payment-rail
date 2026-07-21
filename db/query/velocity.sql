-- name: AcquireVelocityLock :exec
-- Serialize check-then-insert for one signing key: the xact-scoped advisory lock
-- releases at commit/rollback, so concurrent submissions on the same key can't
-- both read a stale window sum and race past the ceiling.
SELECT pg_advisory_xact_lock(sqlc.arg(lock_key)::bigint);

-- name: SumVelocityWindow :one
-- Count and total amount of a key's events since the window start.
SELECT COUNT(*)::bigint AS count, COALESCE(SUM(amount), 0)::bigint AS sum
FROM velocity_events
WHERE key_id = sqlc.arg(key_id) AND occurred_at >= sqlc.arg(since);

-- name: InsertVelocityEvent :exec
INSERT INTO velocity_events (key_id, amount, occurred_at)
VALUES (sqlc.arg(key_id), sqlc.arg(amount), sqlc.arg(occurred_at));
