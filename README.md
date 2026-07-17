# Conduit

**Stablecoin payment orchestration rail** — a self-hostable service that treats
blockchains as unreliable settlement rails behind a boring, correct, auditable
payments API. *Traditional business in the front, crypto rails in the back.*

> ⚠️ **Portfolio & learning project.** Conduit handles **testnet funds only** and
> is **not audited**. Its purpose is to demonstrate payments-grade engineering —
> idempotency, double-entry accounting, reorg-safe settlement tracking, and
> compliance-aware design. Do not point it at mainnet or real funds.

## Status

Milestone **M1 — ledger + payments API** (this cut): a double-entry ledger in
Postgres with balances derived from history (never stored), and an idempotent
REST payments API (`create`/`get`/`list`/`cancel`) that composes the payment and
its journal entry in a single transaction. Concurrency safety (no overdraw under
parallel writes) is proven under the race detector against live Postgres. See the
[ADRs](docs/adr/) 0004–0007 for the decisions behind it.

## Architecture at a glance

Six binaries in one module (`cmd/` layout), external transport REST. Internal
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
| `webhookd` | Signed webhook delivery with backoff and dead-lettering |
| `conduitctl` | Operator CLI |

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
| M2 | Signer + EVM chain adapter (testnet submit) |
| M3 | Chain-watcher with confirmations + reorg handling |
| M4 | Outbox → Kafka + webhook dispatcher |
| M5 | Policy engine + audit log |
| M6 | Reconciliation + proof-of-reserves report |
| M7 | Chaos tests, load tests, published benchmark |

## License

[MIT](LICENSE).
