-- name: InsertPaymentApproval :one
-- Parks a payment's full intent as a pending approval attributed to its proposer.
-- Returns the generated id so the submit path can reference the queued row.
INSERT INTO payment_approvals (to_address, amount, asset, key_id, payment_id, proposer)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id;

-- name: GetApprovalForUpdate :one
-- Locks the row for the duration of the claim transaction so a concurrent
-- approver blocks rather than racing past the status guard.
SELECT * FROM payment_approvals
WHERE id = $1
FOR UPDATE;

-- name: MarkApprovalApproved :execrows
-- Guarded on status = 'pending' so a concurrent or repeated claim matches no row;
-- the rows-affected count lets the caller detect an already-claimed approval.
UPDATE payment_approvals
SET status = 'approved', approver = $2
WHERE id = $1 AND status = 'pending';

-- name: MarkApprovalBroadcast :execrows
-- Guarded on status = 'approved' AND tx_hash IS NULL so a double-broadcast matches
-- no row; the rows-affected count lets the caller detect an already-broadcast row.
UPDATE payment_approvals
SET tx_hash = $2
WHERE id = $1 AND status = 'approved' AND tx_hash IS NULL;

-- name: ReopenApproval :execrows
-- Reverts a claimed-but-never-broadcast approval back to pending so a retry can
-- re-claim it after a pre-send broadcast failure. Guarded on status = 'approved'
-- AND tx_hash IS NULL: once a broadcast has landed (tx_hash set) reopen matches no
-- row, so a sent payment can never be resurrected. The rows-affected count lets the
-- caller detect a row that was not in the reopenable state.
UPDATE payment_approvals
SET status = 'pending', approver = NULL
WHERE id = $1 AND status = 'approved' AND tx_hash IS NULL;
