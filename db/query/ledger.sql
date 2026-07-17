-- name: CreateAccount :one
INSERT INTO accounts (name, kind, asset)
VALUES ($1, $2, $3)
RETURNING *;

-- name: GetAccount :one
SELECT * FROM accounts
WHERE id = $1;

-- name: GetAccountsForUpdate :many
-- Production locking path: lock the named account rows for the duration of the
-- surrounding transaction, ordered by id to impose a deterministic lock order
-- and avoid deadlocks between concurrent transfers. WP2 calls this on Querier.
SELECT * FROM accounts
WHERE id = ANY(@ids::uuid[])
ORDER BY id
FOR UPDATE;

-- name: GetAccountBalance :one
-- Derived balance: credits add, debits subtract; never stored.
SELECT COALESCE(
    SUM(CASE WHEN direction = 'credit' THEN amount ELSE -amount END),
    0
)::bigint AS balance
FROM entry_lines
WHERE account_id = $1;

-- name: InsertJournalEntry :one
INSERT INTO journal_entries (kind, external_ref, asset)
VALUES ($1, $2, $3)
RETURNING *;

-- name: InsertEntryLine :one
INSERT INTO entry_lines (entry_id, account_id, direction, amount)
VALUES ($1, $2, $3, $4)
RETURNING *;
