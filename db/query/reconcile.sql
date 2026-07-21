-- name: ListSettlementsForReconcileFirstPage :many
-- First keyset page of settlements that count toward the ledger's on-chain
-- expectation: only 'settled' (confirmed, not yet final) and 'finalized' rows.
-- Amount/asset live on the payment, so we join. Ordered ASC by (created_at, id)
-- — the reconcile job pages forward, oldest-first, so a mid-scan insert lands
-- past the cursor and is simply picked up on a later run. The cursor for the
-- next page is the last row's (created_at, id).
SELECT s.id, s.created_at, s.status, p.asset, p.amount
FROM settlements s
JOIN payments p ON p.id = s.payment_id
WHERE s.status IN ('settled', 'finalized')
ORDER BY s.created_at, s.id
LIMIT $1;

-- name: ListSettlementsForReconcileAfter :many
-- Keyset continuation: everything strictly after the cursor. The row-value
-- comparison (created_at, id) > (@after_created_at, @after_id) is a single
-- index range scan, stable under concurrent inserts and free of OFFSET's skew.
SELECT s.id, s.created_at, s.status, p.asset, p.amount
FROM settlements s
JOIN payments p ON p.id = s.payment_id
WHERE s.status IN ('settled', 'finalized')
  AND (s.created_at, s.id) > (sqlc.arg(after_created_at)::timestamptz, sqlc.arg(after_id)::uuid)
ORDER BY s.created_at, s.id
LIMIT sqlc.arg(page_limit);

-- name: SumNonHouseLiabilities :one
-- Per-asset Σ(credit − debit) over every account EXCEPT the onchain_settlement
-- house account. This is the (signed) sum of user-facing balances; the Go caller
-- negates it to get liabilities = −Σ(non-house balances) for proof-of-reserves.
SELECT COALESCE(
    SUM(CASE WHEN el.direction = 'credit' THEN el.amount ELSE -el.amount END),
    0
)::bigint
FROM entry_lines el
JOIN accounts a ON a.id = el.account_id
WHERE a.kind <> 'onchain_settlement' AND a.asset = $1;
