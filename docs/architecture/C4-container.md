# C4 — Level 2: Containers

The runnable pieces of Payment Rail and the stores/rails they talk to. Each container
is a separate binary under `cmd/`.

```mermaid
C4Container
    title Container Diagram — Payment Rail

    Person(platformEng, "Platform engineer", "REST + events")
    Person(opsUser, "Ops operator", "CLI")

    System_Boundary(paymentrail, "Payment Rail") {
        Container(api, "api", "Go, REST", "Idempotent payments API; orchestrates the payment saga")
        Container(ledger, "ledger", "Go, gRPC", "Double-entry ledger; serializable journal transactions")
        Container(signer, "signer", "Go, gRPC", "Network-isolated key holder; per-key spend limits")
        Container(chainwatcher, "chainwatcher", "Go", "Per-chain confirmation tracking; reorg-safe finality")
        Container(webhookd, "webhookd", "Go", "Signed webhook delivery; backoff + dead-letter")
        Container(ctl, "paymentrailctl", "Go, CLI", "Operator tooling")
    }

    ContainerDb(pg, "PostgreSQL", "Ledger + transactional outbox + idempotency store")
    ContainerQueue(kafka, "Kafka / Redpanda", "Domain event bus (payment.*)")
    System_Ext(evm, "EVM chains", "Base + Ethereum Sepolia")

    Rel(platformEng, api, "Create/get/list/cancel payments", "REST/HTTPS")
    Rel(opsUser, ctl, "Operate & reconcile", "CLI")
    Rel(ctl, api, "Drives", "REST")

    Rel(api, ledger, "Holds/settles funds", "gRPC")
    Rel(api, signer, "Requests signatures", "gRPC")
    Rel(ledger, pg, "Reads/writes journal + outbox", "SQL")
    Rel(signer, evm, "Broadcasts signed txs", "JSON-RPC")
    Rel(chainwatcher, evm, "Polls blocks/receipts", "JSON-RPC")
    Rel(chainwatcher, ledger, "Applies confirmation effects", "gRPC")
    Rel(ledger, kafka, "Publishes outbox events", "Kafka")
    Rel(kafka, webhookd, "Delivers events", "Kafka")
    Rel(webhookd, platformEng, "POSTs signed webhooks", "HTTPS")
```

## Notes

- **Postgres** owns the ledger, the transactional outbox, and the idempotency
  store — a single relational boundary makes crash-safety tractable.
- **Outbox → Kafka** gives at-least-once event delivery; consumers must be
  idempotent (documented per milestone M4).
- The **signer** is deliberately isolated: it has no database and only signs
  well-formed payloads, so compromising `api` does not directly move funds.
