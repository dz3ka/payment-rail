-- Conduit payments schema (M1 / IMPL-A).
--
-- A payment is the API-facing record of one money movement; the authoritative
-- accounting lives in journal_entries/entry_lines (see 0001). Every payment
-- points at the journal entry that effected it, and — once canceled — at the
-- reversing entry that undid it. Balances stay derived; nothing here stores one.

CREATE TABLE payments (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status            TEXT NOT NULL CHECK (status IN ('completed', 'canceled')),
    asset             TEXT NOT NULL,
    amount            BIGINT NOT NULL CHECK (amount > 0),
    source_account_id UUID NOT NULL REFERENCES accounts(id),
    dest_account_id   UUID NOT NULL REFERENCES accounts(id),
    journal_entry_id  UUID NOT NULL REFERENCES journal_entries(id),
    -- Set only when the payment is canceled: the entry that reverses the money.
    reversal_entry_id UUID REFERENCES journal_entries(id),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    canceled_at       TIMESTAMPTZ
);

-- Keyset pagination: list newest-first and page with (created_at, id) < cursor.
-- The composite DESC index matches ListPaymentsAfter's row-value comparison.
CREATE INDEX idx_payments_keyset ON payments (created_at DESC, id DESC);

CREATE TABLE idempotency_keys (
    key             TEXT PRIMARY KEY,
    -- Hash of the request payload: lets the handler detect a key reused with a
    -- different body (a client bug) rather than silently replaying the old one.
    request_hash    BYTEA NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('in_flight', 'completed')),
    -- Cached response, populated on completion so retries replay it verbatim.
    response_status INT,
    response_body   BYTEA,
    payment_id      UUID,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

-- Supports the TTL sweeper's range delete over old keys.
CREATE INDEX idx_idempotency_keys_created ON idempotency_keys (created_at);
