-- name: InsertSettlement :one
-- Links an on-chain tx to the payment it settles. ON CONFLICT (tx_hash) DO
-- NOTHING means a re-submitted tx returns zero rows, so the caller gets
-- sql.ErrNoRows — the "already linked" signal — and resolves it via
-- GetSettlementByTxHash instead of double-inserting.
INSERT INTO settlements (payment_id, tx_hash)
VALUES ($1, $2)
ON CONFLICT (tx_hash) DO NOTHING
RETURNING *;

-- name: GetSettlementByTxHash :one
SELECT * FROM settlements
WHERE tx_hash = $1;

-- name: MarkSettlementSettled :one
-- Guarded on status IN ('pending', 'reorged') so a concurrent or repeated
-- settle matches no row and the caller sees sql.ErrNoRows instead of
-- re-pointing settle_entry_id. A reorged tx that re-confirms settles again.
UPDATE settlements
SET status = 'settled', settle_entry_id = $2, updated_at = now()
WHERE tx_hash = $1 AND status IN ('pending', 'reorged')
RETURNING *;

-- name: MarkSettlementReorged :one
-- Guarded on status = 'settled' so only a previously-settled tx can be
-- reorged; a still-pending or already-reorged tx matches no row.
UPDATE settlements
SET status = 'reorged', updated_at = now()
WHERE tx_hash = $1 AND status = 'settled'
RETURNING *;

-- name: ListPendingSettlements :many
-- The payments→Track feed for the chainwatcher: rows still being watched,
-- i.e. pending (awaiting confirmation) or settled (watched for reorg). Ordered
-- by created_at so the watcher processes them oldest-first.
SELECT * FROM settlements
WHERE status IN ('pending', 'settled')
ORDER BY created_at;
