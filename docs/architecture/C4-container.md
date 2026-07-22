# C4 — Level 2: Containers

The runnable pieces of Payment Rail and the stores/rails they talk to. Each container
is a separate binary under `cmd/`. One deliberate wrinkle (ADR-0006): the double-entry
ledger runs **in-process** — `api` links `internal/ledger` so a payment and its journal
entry commit in one atomic Postgres transaction, and `cmd/ledger` stays an idle M0
skeleton held for ADR-0001's target-state gRPC split.

```mermaid
C4Container
    title Container Diagram — Payment Rail

    Person(platformEng, "Platform engineer", "REST + events")
    Person(opsUser, "Ops operator", "CLI")

    System_Boundary(paymentrail, "Payment Rail") {
        Container(api, "api", "Go, REST", "Idempotent payments API; runs the double-entry ledger in-process — payment + journal in one atomic tx")
        Container(ledger, "ledger", "Go", "M0 skeleton, idle. The ledger itself is internal/ledger, linked into api, chainwatcher, outboxrelay, paymentrailctl")
        Container(signer, "signer", "Go, gRPC (loopback-only)", "Network-isolated key holder; signs well-formed payloads only; per-key spend limits")
        Container(chainwatcher, "chainwatcher", "Go", "Per-chain confirmation tracking; reorg-safe finality")
        Container(outboxrelay, "outboxrelay", "Go", "Drains the transactional outbox to Kafka (at-least-once)")
        Container(webhookd, "webhookd", "Go", "HMAC-signed webhook delivery; backoff + dead-letter")
        Container(ctl, "paymentrailctl", "Go, CLI", "Operator CLI: submit, approve, replay-webhook, audit verify, reconcile, loadtest; policy + velocity + four-eyes screening on submit")
    }

    ContainerDb(pg, "PostgreSQL", "Ledger, transactional outbox, idempotency store, webhook deliveries, approvals/velocity, hash-chained audit log")
    ContainerQueue(kafka, "Kafka / Redpanda", "Domain event bus (payment.*)")
    System_Ext(evm, "EVM chains", "Base Sepolia + Ethereum Sepolia")

    Rel(platformEng, api, "Create/get/list/cancel payments", "REST/HTTPS")
    Rel(opsUser, ctl, "Operate & reconcile", "CLI")
    Rel(ctl, api, "Load-tests POST /v1/payments", "REST")

    Rel(api, pg, "Commits payment + journal + outbox event in one tx", "SQL")
    Rel(ctl, signer, "Requests signatures (submit)", "gRPC")
    Rel(ctl, evm, "Broadcasts signed transfers; reads treasury balances", "JSON-RPC")
    Rel(ctl, pg, "Settlement links, approvals, velocity, audit chain", "SQL")
    Rel(chainwatcher, evm, "Polls blocks/receipts", "JSON-RPC")
    Rel(chainwatcher, pg, "Applies settlement effects + outbox events", "SQL")
    Rel(outboxrelay, pg, "Claims unsent outbox rows; marks sent", "SQL")
    Rel(outboxrelay, kafka, "Publishes envelopes verbatim", "Kafka")
    Rel(kafka, webhookd, "Delivers events", "Kafka")
    Rel(webhookd, pg, "Fans out + claims delivery rows; dead-letters", "SQL")
    Rel(webhookd, platformEng, "POSTs HMAC-signed webhooks", "HTTPS")
```

## Notes

- **The ledger is a library, not a service (ADR-0006).** ADR-0001's gRPC-between-services
  topology remains the target end state, but the shipped payment path has no
  api→ledger network hop: a payment and its journal entry must commit in **one**
  Postgres transaction, so callers link `internal/ledger` directly. `cmd/ledger`
  stays an idle skeleton binary reserved for the eventual split.
- **Postgres** owns the ledger, the transactional outbox, the idempotency store,
  webhook delivery state, approval/velocity state, and the append-only hash-chained
  audit log — a single relational boundary makes crash-safety tractable.
- **Outbox → `outboxrelay` → Kafka** gives at-least-once event delivery (ADR-0011):
  each state change appends its event row in the same transaction (payment events
  from `api`, settlement events from `chainwatcher`), and the relay claims,
  publishes verbatim, and marks sent in one transaction. Consumers must be
  idempotent — `webhookd` dedupes on the event id.
- The **signer** is deliberately isolated: no database, no chain access — it only
  signs well-formed payloads under per-key spend limits, on a loopback-only gRPC
  endpoint (mTLS and HSM/MPC custody are the recorded pre-mainnet upgrade,
  ADR-0013). Its only caller is `paymentrailctl`'s submit path, whose chain adapter
  performs the broadcast and aborts on a sender mismatch (ADR-0010) — key material
  never leaves the signer.
