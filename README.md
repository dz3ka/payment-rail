# Payment Rail

**Stablecoin payment orchestration rail** — a self-hostable service that treats
blockchains as unreliable settlement rails behind a boring, correct, auditable
payments API. *Traditional business in the front, crypto rails in the back.*

> ⚠️ **Portfolio & learning project.** Payment Rail handles **testnet funds only** and
> is **not audited**. Its purpose is to demonstrate payments-grade engineering —
> idempotency, double-entry accounting, reorg-safe settlement tracking, and
> compliance-aware design. Do not point it at mainnet or real funds.

## Status

Milestone **M4 — event backbone: transactional outbox → Kafka + webhook delivery**
(this cut). Building on M1's double-entry ledger and idempotent REST payments API,
M2's network-isolated signer and EVM chain adapter, and M3's reorg-safe chain-watcher
(poll-based confirmation-depth tracking, reorg-safe settlement effects against the
ledger, and settlement-recovery hardening), M4 makes the event backbone real:

- **Transactional outbox → Kafka** (slice 1): domain events (payment and settlement
  lifecycle) are written to an outbox table inside the *same* Postgres transaction as
  the state change, then a relay drains unsent rows to Kafka/Redpanda at-least-once.
- **Webhook delivery** (slice 3): `webhookd` consumes those events and delivers
  HMAC-SHA256-signed webhooks to subscriber endpoints — fan-out to a durable delivery
  queue, a poll-loop worker with exponential backoff, dead-lettering after N attempts,
  and consumer-side idempotency on the event id. `paymentrailctl replay-webhook`
  re-drives dead-lettered deliveries after a broken endpoint is fixed.

See the [ADRs](docs/adr/) for the decisions behind each milestone.

## Architecture at a glance

Seven binaries in one module (`cmd/` layout), external transport REST. Internal
transport is gRPC as the target end state (ADR-0001), but for M1 the `api` runs
the ledger **in-process** — payment and journal entry commit atomically in one
Postgres transaction with no network hop (ADR-0006). Postgres owns the ledger;
Kafka (Redpanda in dev) carries domain events from M4 on.

| Binary | Role |
|--------|------|
| `api` | External REST payments API (idempotent create/get/list/cancel) |
| `ledger` | Double-entry ledger; internal source of truth |
| `signer` | Network-isolated key holder; signs well-formed payloads only |
| `chainwatcher` | Per-chain confirmation tracking; reorg-safe finality |
| `outboxrelay` | Drains the transactional outbox to Kafka (at-least-once) |
| `webhookd` | Consumes events; HMAC-signed webhook delivery with backoff + dead-letter |
| `paymentrailctl` | Operator CLI (`submit`, `replay-webhook`) |

See [`docs/architecture/`](docs/architecture/) for the C4 diagrams and
[`docs/adr/`](docs/adr/) for the decision records.

## Quickstart (target: < 10 minutes)

**Prerequisites:** Go 1.24+, Docker + Docker Compose, `make`.

```bash
# 1. Bring up the dev stack (Postgres, Redpanda, OTel Collector)
make up

# 2. Build all binaries into ./bin
make build

# 3. Run the test suite (race detector on)
make test

# 4. Boot a service (Ctrl-C to stop; shuts down cleanly)
./bin/api

# 5. Tear the stack down
make down
```

Integration and chaos tests need the dev-stack Postgres and are skipped without it.
Point `PAYMENT_RAIL_TEST_DSN` at the running stack, then:

```bash
# Fault-injection / chaos suite (crash, DB failover, broken RPC, reorg) —
# drives the real payment path and asserts the ledger converges. Local-only.
export PAYMENT_RAIL_TEST_DSN='postgres://payment_rail:payment_rail@localhost:5432/payment_rail?sslmode=disable'
make test-chaos
```

Copy `.env.example` to `.env` to override defaults. No real secrets belong in
either file — see [`docs/threat-model.md`](docs/threat-model.md).

## Repository layout

```
cmd/            one directory per binary (thin entrypoints)
internal/       private packages (config, version, service bootstrap, ...)
deploy/         local infra config (otel-collector.yaml, ...)
docs/
  architecture/ C4 context + container diagrams
  adr/          architecture decision records
  learning/     Go learning journal (the "why", milestone by milestone)
  threat-model.md
```

## Roadmap

| Milestone | Scope |
|-----------|-------|
| **M0** ✅ | Repo skeleton, CI, lint, dev stack, C4, first ADRs |
| **M1** ✅ | Ledger service + payments API with idempotency |
| **M2** ✅ | Signer + EVM chain adapter (testnet submit) |
| **M3** ✅ | Chain-watcher with confirmations + reorg handling |
| **M4** ✅ | Outbox → Kafka + webhook dispatcher |
| **M5** ✅ | Policy engine + audit log |
| M6 | Reconciliation + proof-of-reserves report |
| M7 | Chaos tests, load tests, published benchmark |

## License

[MIT](LICENSE).
