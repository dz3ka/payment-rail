-- Payment Rail transactional outbox (reliable event publishing).
--
-- Domain writes append an event row in the same transaction that mutates the
-- aggregate, so a payment/settlement change and its intent-to-publish commit
-- atomically. A relay then claims unsent rows and publishes them to Kafka,
-- stamping sent_at on success — at-least-once delivery without a distributed
-- transaction. aggregate_id is the Kafka message key; there is deliberately no
-- aggregate_type column (it is derived from event_type at the application layer).

CREATE TABLE outbox (
    id            UUID PRIMARY KEY,
    event_type    TEXT NOT NULL,          -- e.g. 'payment.created', 'settlement.finalized'
    aggregate_id  TEXT NOT NULL,          -- Kafka message key (payment uuid / tx hash)
    payload       JSONB NOT NULL,         -- complete serialized event envelope, published verbatim
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at       TIMESTAMPTZ             -- NULL = unsent
);

-- Partial index over unsent rows only: the relay's claim scan stays cheap as
-- sent history accumulates, and rows drop out of the index once stamped.
CREATE INDEX idx_outbox_unsent ON outbox (created_at) WHERE sent_at IS NULL;
