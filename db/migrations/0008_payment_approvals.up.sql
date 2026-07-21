-- Payment Rail four-eyes approval queue (M5 / slice 3, PRD F8c).
--
-- A payment at or above the four-eyes threshold is not broadcast at submit time;
-- instead its full intent (destination, amount, asset, signing key) is parked
-- here as a pending row attributed to its proposer. A distinct approver later
-- claims the row (pending → approved) and broadcasts it, so no single operator
-- can both propose and release a large payment. Two states only: tx_hash IS NOT
-- NULL marks broadcast-complete; approved with tx_hash still NULL means the
-- broadcast never landed and the row must be reconciled by hand. Audit
-- timestamps (approved_at/broadcast_at) are deliberately deferred to the F9 slice.

CREATE TABLE payment_approvals (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    to_address  TEXT NOT NULL,
    amount      BIGINT NOT NULL CHECK (amount > 0),
    asset       TEXT NOT NULL,
    key_id      TEXT NOT NULL,
    payment_id  UUID,
    proposer    TEXT NOT NULL,
    approver    TEXT,
    status      TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved')),
    tx_hash     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Lists the pending queue oldest-first; the partial predicate keeps the index to
-- just the rows still awaiting an approver.
CREATE INDEX idx_payment_approvals_pending ON payment_approvals (created_at)
    WHERE status = 'pending';
