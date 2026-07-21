-- name: AcquireAuditChainLock :exec
-- Serialize the whole chain: the xact-scoped advisory lock releases at
-- commit/rollback, so concurrent appenders can't both read the same head and
-- assign the same seq / prev_hash, which would fork the chain.
SELECT pg_advisory_xact_lock(sqlc.arg(lock_key)::bigint);

-- name: GetAuditHead :one
-- The current tail of the chain, whose entry_hash becomes the next row's
-- prev_hash and whose seq + 1 becomes the next seq. Callers treat sql.ErrNoRows
-- as the empty chain: the first row is seq 1 with prev = 32 zero bytes (genesis).
SELECT seq, entry_hash FROM audit_log ORDER BY seq DESC LIMIT 1;

-- name: InsertAuditEntry :exec
-- Appends one already-hashed row. seq, prev_hash and entry_hash are computed by
-- the app from the head read under the chain lock in the same transaction.
INSERT INTO audit_log (seq, prev_hash, entry_hash, actor, action, aggregate_type, aggregate_id, occurred_at, payload)
VALUES (sqlc.arg(seq), sqlc.arg(prev_hash), sqlc.arg(entry_hash), sqlc.arg(actor), sqlc.arg(action), sqlc.arg(aggregate_type), sqlc.arg(aggregate_id), sqlc.arg(occurred_at), sqlc.arg(payload));

-- name: ScanAuditChain :many
-- The full chain oldest-first, so a verifier can rehash each row against its
-- predecessor and confirm no historical row was altered or removed.
SELECT seq, prev_hash, entry_hash, actor, action, aggregate_type, aggregate_id, occurred_at, payload
FROM audit_log ORDER BY seq ASC;
