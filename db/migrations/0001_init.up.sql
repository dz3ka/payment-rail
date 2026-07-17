-- Conduit ledger schema (M1 / WP1).
--
-- Derived-balances design: an account's balance is NEVER stored. It is always
-- computed on demand as Σ(credit) − Σ(debit) over that account's entry_lines
-- (see GetAccountBalance). Journal entries are immutable once written, so the
-- ledger is an append-only log and balances are a pure function of that log.

CREATE TABLE accounts (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name       TEXT NOT NULL,
    kind       TEXT NOT NULL,
    asset      TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (name, asset)
);

CREATE TABLE journal_entries (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kind         TEXT NOT NULL,
    external_ref TEXT NOT NULL,
    asset        TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Ledger-layer idempotency: a (kind, external_ref) pair maps to at most one
    -- journal entry, so retries of the same logical operation collapse.
    UNIQUE (kind, external_ref)
);

CREATE TABLE entry_lines (
    id         BIGSERIAL PRIMARY KEY,
    entry_id   UUID NOT NULL REFERENCES journal_entries(id),
    account_id UUID NOT NULL REFERENCES accounts(id),
    direction  TEXT NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount     BIGINT NOT NULL CHECK (amount > 0)
);

CREATE INDEX idx_entry_lines_account ON entry_lines(account_id);
