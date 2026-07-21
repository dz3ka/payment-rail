# ADR-0011: Transactional outbox & event schema

- **Status:** Accepted
- **Date:** 2026-07-20
- **Deciders:** Bogdan Dzekic

## Context

M4 makes the event backbone real: payment and settlement lifecycle changes must
become domain events other systems (starting with `webhookd`) can consume. The
hard part is the write itself. A state change lives in Postgres; the event must
reach Kafka. Doing both as two independent writes ("dual write") is unsafe — a
crash between them either loses an event (DB committed, publish never happened) or
emits a phantom (published, DB rolled back). At a money boundary neither is
acceptable, but a real distributed transaction across Postgres and Kafka is
operationally out of scope for a self-hostable testnet project.

## Decision

**Transactional outbox.** A producer inside a write transaction hands `outbox.Emit`
an `Event`; Emit appends one row to an `outbox` table via the *same* `db.Querier`,
so the aggregate change and its intent-to-publish commit atomically. A separate
`outboxrelay` binary polls unsent rows (`sent_at IS NULL`), publishes them to Kafka,
and stamps `sent_at` on success. Publish is held **inside** the claim transaction:
a publish failure returns before `MarkOutboxSent`, the tx rolls back, and the rows
stay unsent for the next tick — **at-least-once** delivery without 2PC.

**Envelope.** A versioned JSON `Envelope` (`schema_version`, currently 1) is written
to the `payload` JSONB column and published to Kafka **verbatim** (never re-marshaled
by the relay). `Type` is a namespaced `"<aggregate>.<verb>"` (e.g. `payment.created`);
`aggregate_type` is **derived** from its prefix, so there is deliberately no
`aggregate_type` column. `aggregate_id` is the Kafka message key, so a hash-keyed
producer preserves per-aggregate ordering; the relay claims oldest-first to keep it.

## Alternatives considered

- **Dual write (app writes DB, then Kafka):** the failure mode above — lost or
  phantom events on a crash between the two writes. Rejected outright at a money boundary.
- **Distributed transaction / 2PC (XA) across Postgres + Kafka:** operational weight
  and brittle Kafka support; over-engineered for a self-hosted testnet rail.
- **Log-based CDC (Debezium/connectors):** infra to run and operate that a portfolio
  project should not carry; the outbox is legible in application code and needs only Postgres.
- **Exactly-once delivery:** not pursued — at-least-once plus consumer idempotency
  (webhookd dedupes on the event id) is simpler and sufficient.
- **A separate `aggregate_type` column:** redundant with `event_type`'s prefix; one
  source of truth avoids the two drifting apart.

## Consequences

- Easier: no dual-write inconsistency; the relay is a thin, restartable poller; a
  partial index over unsent rows (`WHERE sent_at IS NULL`) keeps the claim scan cheap
  as sent history accumulates; the versioned envelope lets consumers dispatch on shape.
- Harder / deferred: at-least-once means **every** consumer must be idempotent (a
  responsibility, not an option); ordering holds only *per aggregate*, not globally;
  the relay's poll cadence adds latency between commit and publish. A breaking envelope
  change requires bumping `schema_version` and dual-reading consumers.
