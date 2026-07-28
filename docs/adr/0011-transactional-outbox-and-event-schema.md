# ADR-0011: Transactional outbox & event schema

- **Status:** Accepted. Scopes the **producer side only** — `cmd/webhookd` and any second
  consumer are deferred to M4 slice 3 (delivered by [0017]).
- **Date:** 2026-07-20
- **Deciders:** Bogdan Dzekic

## Context

M4 makes the event backbone real: payment and settlement lifecycle changes must
become domain events other systems (starting with `webhookd`) can consume. The five
events are `payment.created`, `payment.canceled`, `settlement.confirmed`,
`settlement.reorged`, and `settlement.finalized`, published onto the Kafka-wire bus of
[0003] (Redpanda in dev) so downstream consumers — webhooks first, later audit and
reconciliation — can react without coupling to the ledger.

The hard part is the write itself. A state change lives in Postgres; the event must
reach Kafka. Doing both as two independent writes ("dual write") is unsafe — a
crash between them either loses an event (DB committed, publish never happened) or
emits a phantom (published, DB rolled back). At a money boundary neither is
acceptable, but a real distributed transaction across Postgres and Kafka is
operationally out of scope for a self-hostable testnet project.

Three existing constraints shape the answer. The repo already funnels every write
through one transaction primitive — `ledger.SQLStore.ExecTx(ctx, func(q db.Querier) error)`
over `database/sql` at Read Committed — reused by both `internal/payments` and
`internal/settlement` ([0004], [0007], [0014]); whatever we build must ride it.
Everything must stay hermetically testable, with no broker and no Postgres in CI, per the
repo's fake-querier convention. And the M1 payment path and the [0014]/[0016] settlement
model must be left byte-for-byte unchanged.

## Decision

**Transactional outbox.** A producer inside a write transaction hands `outbox.Emit`
an `Event`; Emit appends one row to an `outbox` table via the *same* `db.Querier`,
so the aggregate change and its intent-to-publish commit atomically. A separate
`outboxrelay` binary polls unsent rows (`sent_at IS NULL`), publishes them to Kafka,
and stamps `sent_at` on success. Publish is held **inside** the claim transaction:
a publish failure returns before `MarkOutboxSent`, the tx rolls back, and the rows
stay unsent for the next tick — **at-least-once** delivery without 2PC.

**Zero plumbing change.** sqlc's generated `Querier`/`DBTX` is satisfied by both `*sql.DB`
and `*sql.Tx`, so `q.InsertOutboxEvent(...)` inside the existing `ExecTx` closures needed no
new signatures — only a call added after each guarded mutation.

**Emit only on a real transition.** All five emit sites sit *after* the guarded `Mark*`/status
write and *inside* the branch where the row actually flipped, past the idempotent-no-op /
`sql.ErrNoRows` / status short-circuit returns. A redelivery or replay therefore emits zero
events. (`payments.Create` is the one unconditional emit, because a freshly generated payment
id makes every insert a real transition.)

**Schema (migration 0005).** `outbox(id uuid pk, event_type text, aggregate_id text,
payload jsonb, created_at timestamptz default now(), sent_at timestamptz null)` plus a partial
index `idx_outbox_unsent (created_at) WHERE sent_at IS NULL`. `sent_at IS NULL` is the unsent
sentinel — no status enum. `aggregate_id` is a real column rather than living only in the
payload, so the relay can set the Kafka key without deserializing.

**Envelope.** A versioned JSON `Envelope` — `{id, type, aggregate_type, aggregate_id,
occurred_at, schema_version, data}`, `schema_version` currently 1 — is written to the `payload`
JSONB column and published to Kafka **verbatim** (never re-marshaled by the relay). `Type` is a
namespaced `"<aggregate>.<verb>"` (e.g. `payment.created`); `aggregate_type` is **derived** as
`strings.SplitN(type, ".", 2)[0]`, so there is deliberately no `aggregate_type` column.
`aggregate_id` is the Kafka message key, so a hash-keyed producer preserves per-aggregate
ordering; the relay claims oldest-first to keep it. `schema_version` ships at 1 from day one:
a published wire contract is cheap to version now and a messy migration once a consumer exists.

**Topic and transport.** One topic, `payment-rail.events`, keyed by `aggregate_id` with a
`Hash` balancer — one partition key per payment/tx gives per-aggregate ordering, and a single
topic keeps the (as yet unwritten) consumer simple. Topic and batch size are package constants
(`DefaultTopic`, `DefaultBatchSize = 100`), not config knobs.

**Relay.** A new `cmd/outboxrelay` peer binary (`service.Run` plus a `time.Ticker` poll loop,
mirroring `cmd/chainwatcher`). Not folded into `chainwatcher` — which idles out when no chain
RPC is configured, while payment events fire regardless — nor into `webhookd`, which is the
consumer. `drainBatch` is a pure synchronous seam: `ClaimUnsentOutbox`
(`… WHERE sent_at IS NULL ORDER BY created_at LIMIT n FOR UPDATE SKIP LOCKED`) →
`Producer.Publish` → `MarkOutboxSent(ids)`, all inside one `ExecTx` per batch.

**Producer port, concrete client isolated.** `outbox.Producer.Publish(ctx, []Message)` lives in
`internal/outbox` and is Kafka-client-free; the `segmentio/kafka-go` adapter (`RequiredAcks:
RequireAll`, `Async: false`, `Balancer: Hash`) lives only in `cmd/outboxrelay/kafka.go`. That
keeps `drainBatch`/`Relay` unit-testable against a fake producer with no broker — the repo's
narrow-seam convention.

**Config.** `PAYMENT_RAIL_KAFKA_BROKERS` (default `localhost:19092`, comma-split) and
`PAYMENT_RAIL_OUTBOX_POLL_INTERVAL_SECONDS` (default 5), the latter mirroring the existing
`WatcherPollInterval` poll-loop knob.

## Alternatives considered

- **Dual write (app writes DB, then Kafka):** the failure mode above — lost or
  phantom events on a crash between the two writes. Rejected outright at a money boundary.
- **Distributed transaction / 2PC (XA) across Postgres + Kafka:** operational weight
  and brittle Kafka support; over-engineered for a self-hosted testnet rail.
- **Log-based CDC (Debezium/connectors):** captures the same events with no application-level
  outbox table, and is a legitimate production pattern — but it means running Debezium/Kafka
  Connect and managing replication slots (a stuck slot can fill the disk), and it leaves the
  public event contract implicit in the physical table layout. Infra a portfolio project should
  not carry; the outbox is legible in application code and needs only Postgres. If throughput
  ever demands it, CDC *on the outbox table* is the natural upgrade — same rows, WAL-tailed
  instead of polled.
- **Exactly-once delivery (Kafka transactions / the idempotent producer):** not pursued.
  Rejected as *redundant* rather than as too hard: the only duplicate window here is
  publish-succeeds-then-mark/commit-fails, and consumers already dedupe on the envelope `id`.
  At-least-once plus consumer idempotency is simpler and sufficient.
- **`twmb/franz-go` over `segmentio/kafka-go`:** richer (it offers an idempotent producer), but
  producer-side exactly-once is redundant for the reason above, so we took the smaller API.
  Kafka-wire only either way ([0003]).
- **A separate `aggregate_type` column:** redundant with `event_type`'s prefix; one
  source of truth avoids the two drifting apart.
- **A status enum (`pending`/`sent`) instead of a nullable `sent_at`:** a second representation
  of one timestamp; cut.
- **Per-event-type topics:** premature routing with zero consumers; cut in favour of one topic
  plus a partition key.
- **Config knobs for topic / batch size:** no second value exists, so they are kept as package
  constants, per the repo's "hardcode until a second value forces it" precedent
  (`defaultFinalityDepth`).

## Consequences

- Easier: no dual-write inconsistency; the relay is a thin, restartable poller; a
  partial index over unsent rows (`WHERE sent_at IS NULL`) keeps the claim scan cheap
  as sent history accumulates; the versioned envelope lets consumers dispatch on shape.
- Harder / deferred: at-least-once means **every** consumer must be idempotent (a
  responsibility, not an option); ordering holds only *per aggregate*, not globally;
  the relay's poll cadence adds latency between commit and publish. A breaking envelope
  change requires bumping `schema_version` and dual-reading consumers.
- **The duplicate window, precisely.** The only one is publish-succeeds-then-mark/commit-fails
  → the batch republishes on the next tick. Consumers **must** dedupe on the envelope `id`.
- **Ordering is per-aggregate *and single-instance*.** Key `aggregate_id` + `Hash` balancer +
  a single relay claiming `ORDER BY created_at` preserves a settle→reorg→re-settle sequence
  today. **Latent constraint:** scaling `outboxrelay` past one replica breaks it — `FOR UPDATE
  SKIP LOCKED` can hand two rows of the same aggregate to different workers, and `Hash` only
  orders within one worker's batch. Adding a replica requires an ordering rethink first (e.g.
  partition-by-aggregate assignment).
- **`ORDER BY created_at` has no tiebreaker**, relying on `now()` being distinct per serialized
  transition — true at the current watcher cadence, fragile only under sub-microsecond
  same-aggregate commits, which are not reachable today.
- **A poison row starves the batch:** a row that always fails to publish blocks newer rows via
  `ORDER BY created_at`. Observable via the repeated error log and the row never clearing.
  Mitigation (an `attempts`/quarantine column, or a dead-letter) is a named follow-up, deferred
  for this dev-only slice.
- **The broker is plaintext, no SASL/TLS** — dev/testnet only; production hardening is a named
  follow-up, not wired here.
- **Observability is slog-only:** the relay logs drained row counts and failures, never payloads
  (which carry amounts). A metrics hook (RED on publish, USE gauge on unsent backlog) is the
  future home for `CountUnsentOutbox`.
- **New runtime dependency** `segmentio/kafka-go`, pinned in `go.mod`/`go.sum`.
- **Deferred to M4 slice 3:** the `cmd/webhookd` Kafka consumer plus HMAC-signed delivery with
  backoff and dead-lettering — delivered by [0017].

## Related

[0003] local dev messaging (Redpanda over Kafka) — the bus this publishes onto.
[0004] ledger schema & transaction isolation and [0007] payment → ledger tx composition — the
`ExecTx`/`db.Querier` primitive `Emit` rides. [0014] reorg-safe settlement effects and [0016]
settlement recovery — the settlement transitions that emit three of the five events, left
unchanged by this slice. [0017] webhook consumer — the first consumer, which fulfils this
ADR's deferred slice-3 scope and reuses the `Envelope` and Claim/`SKIP LOCKED` idioms.
