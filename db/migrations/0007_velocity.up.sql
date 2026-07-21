-- Payment Rail per-signing-key velocity limits (M5 / slice 2).
--
-- Each accepted payment appends one velocity_events row keyed by the signing
-- key that authorized it. The policy engine sums a key's recent events under an
-- advisory lock to enforce count/amount ceilings over a rolling window, so a
-- compromised or misbehaving key cannot exceed its budget faster than it can be
-- caught. The (key_id, occurred_at) index keeps that windowed sum cheap as
-- history accumulates.

CREATE TABLE velocity_events (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key_id      TEXT NOT NULL,
    amount      BIGINT NOT NULL CHECK (amount > 0),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_velocity_events_key_time ON velocity_events (key_id, occurred_at);
