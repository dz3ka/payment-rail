-- name: InsertPayment :one
-- Records a completed payment. status/created_at default appropriately; a
-- canceled payment is only ever reached via CancelPayment, never inserted.
INSERT INTO payments (id, status, asset, amount, source_account_id, dest_account_id, journal_entry_id)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING *;

-- name: GetPayment :one
SELECT * FROM payments
WHERE id = $1;

-- name: ListPaymentsFirstPage :many
-- Newest-first page; the keyset cursor for the next page is the last row's
-- (created_at, id). Matches idx_payments_keyset.
SELECT * FROM payments
ORDER BY created_at DESC, id DESC
LIMIT $1;

-- name: ListPaymentsAfter :many
-- Keyset continuation: everything strictly older than the cursor. The row-value
-- comparison (created_at, id) < ($1, $2) is a single index range scan over
-- idx_payments_keyset — stable under inserts and free of OFFSET's skew.
SELECT * FROM payments
WHERE (created_at, id) < (sqlc.arg(after_created_at)::timestamptz, sqlc.arg(after_id)::uuid)
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(page_limit);

-- name: CancelPayment :one
-- Guarded on status = 'completed' so a concurrent or repeated cancel matches no
-- row and the caller sees sql.ErrNoRows instead of double-reversing.
UPDATE payments
SET status = 'canceled', reversal_entry_id = $2, canceled_at = now()
WHERE id = $1 AND status = 'completed'
RETURNING *;
