-- Payment Rail settlements schema (M3 / slice 2).
--
-- A settlement links an on-chain transaction to the payment it settles and
-- tracks that transaction's fate as the chainwatcher observes confirmations
-- and reorgs. The tx_hash UNIQUE gives link idempotency at submit; status
-- moves pending → settled once the ledger entry lands, and settled → reorged
-- if the block that carried it is later orphaned. Balances stay derived
-- (see 0001); settle_entry_id just points at the journal entry that settled it.

CREATE TABLE settlements (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id      UUID NOT NULL REFERENCES payments(id),
    -- 0x-hex on-chain tx hash; UNIQUE so re-submitting the same tx is a no-op.
    tx_hash         TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'settled', 'reorged')),
    -- The ledger entry that settled this tx; null until status = 'settled'.
    settle_entry_id UUID REFERENCES journal_entries(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Feeds the chainwatcher Track query, which pages by settlement status.
CREATE INDEX idx_settlements_status ON settlements (status);

-- House clearing account: the per-asset destination for on-chain settlements.
INSERT INTO accounts (name, kind, asset)
VALUES ('onchain_settlement', 'onchain_settlement', 'USDC');
