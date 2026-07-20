-- Payment Rail webhook fan-out and delivery (outbound HTTP notifications).
--
-- Subscriptions register an endpoint URL plus the event types it cares about and
-- a per-subscription signing secret. When a domain event fires, one delivery row
-- is fanned out per matching active subscription; a worker claims due deliveries,
-- POSTs the signed payload, and records success, a scheduled retry, or (after
-- exhausting retries) a dead-letter for later operator replay.

CREATE TABLE webhook_subscriptions (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url            TEXT NOT NULL,
    event_types    TEXT[] NOT NULL,
    signing_secret TEXT NOT NULL,
    active         BOOLEAN NOT NULL DEFAULT true,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE webhook_deliveries (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id         UUID NOT NULL,
    subscription_id  UUID NOT NULL REFERENCES webhook_subscriptions(id),
    event_type       TEXT NOT NULL,
    payload          JSONB NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'delivered', 'dead_letter')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_status_code INTEGER,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Fan-out idempotency: a given event reaches each subscription at most once,
    -- so redelivery of the same event collapses onto the existing delivery row.
    UNIQUE (event_id, subscription_id)
);

-- Partial index over due, still-pending deliveries only: the worker's claim scan
-- stays cheap as delivered/dead-letter history accumulates.
CREATE INDEX idx_webhook_deliveries_due ON webhook_deliveries (next_attempt_at)
    WHERE status = 'pending';
