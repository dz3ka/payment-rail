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
SET status = 'settled', settle_entry_id = $2,
    settled_block_hash = $3, settled_block_number = $4, updated_at = now()
WHERE tx_hash = $1 AND status IN ('pending', 'reorged')
RETURNING *;

-- name: MarkSettlementReorged :one
-- Guarded on status = 'settled' so only a previously-settled tx can be
-- reorged; a still-pending or already-reorged tx matches no row. Clears the
-- recorded block so a reorged row carries no stale finality provenance.
UPDATE settlements
SET status = 'reorged', settled_block_hash = NULL, settled_block_number = NULL,
    updated_at = now()
WHERE tx_hash = $1 AND status = 'settled'
RETURNING *;

-- name: MarkSettlementFinalized :one
-- Guarded on status = 'settled' so only a settled tx can finalize; a reorged or
-- still-pending tx matches no row. ErrNoRows here is an idempotent no-op for the
-- caller (already finalized, or reorged out from under the promotion).
UPDATE settlements
SET status = 'finalized', updated_at = now()
WHERE tx_hash = $1 AND status = 'settled'
RETURNING *;

-- name: ListPendingSettlements :many
-- The payments→Track feed for the chainwatcher: rows still being watched, i.e.
-- pending (awaiting confirmation), settled (watched for reorg), or reorged
-- (watched to re-settle once the tx re-confirms). Including 'reorged' lets a
-- restart inside the reorg window re-seed the tx instead of losing it. 'finalized'
-- is terminal and deliberately excluded. Ordered by created_at so the watcher
-- processes them oldest-first.
SELECT * FROM settlements
WHERE status IN ('pending', 'settled', 'reorged')
ORDER BY created_at;
