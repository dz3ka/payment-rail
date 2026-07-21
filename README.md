# Payment Rail

**Stablecoin payment orchestration rail** — a self-hostable service that treats
blockchains as unreliable settlement rails behind a boring, correct, auditable
payments API. *Traditional business in the front, crypto rails in the back.*

> ⚠️ **Portfolio & learning project.** Payment Rail handles **testnet funds only** and
> is **not audited**. Its purpose is to demonstrate payments-grade engineering —
> idempotency, double-entry accounting, reorg-safe settlement tracking, and
> compliance-aware design. Do not point it at mainnet or real funds.

## Status

**Feature-complete** — every roadmap milestone (M0–M7) is implemented. Payment Rail
takes a stablecoin payment from an idempotent REST call through a double-entry
ledger, a network-isolated signer, and an EVM chain adapter; tracks it to reorg-safe
finality; emits domain events over a transactional outbox → Kafka → signed-webhook
backbone; screens it through a policy / velocity / four-eyes engine with an
append-only hash-chained audit log; and reconciles on-chain treasury balances against
the ledger with a proof-of-reserves check — all exercised end-to-end by a
fault-injection chaos suite and a published load-test benchmark.

Highlights by milestone:

- **Double-entry ledger + idempotent payments API** (M1): balances derived, never
  stored; every create commits as one atomic Postgres transaction; idempotency-key
  replay safety.
- **Isolated signer + EVM chain adapter** (M2): keys never leave the signer;
  EIP-1559 USDC transfers behind a gap-free, chain-authoritative nonce allocator.
- **Reorg-safe settlement** (M3): poll-based confirmation-depth tracking, reorg
  detection, and settlement effects that stay correct across chain reorgs and
  restarts.
- **Event backbone** (M4): transactional outbox → Kafka/Redpanda (at-least-once) →
  `webhookd` HMAC-SHA256-signed delivery with exponential backoff, dead-lettering,
  and replay.
- **Compliance controls** (M5): policy engine + denylist screening, per-signing-key
  velocity limits, four-eyes approval above a threshold, and an append-only
  hash-chained audit log (`paymentrailctl audit verify`).
- **Reconciliation** (M6): on-chain treasury balances vs. the ledger, with a
  proof-of-reserves report and a tri-state operator exit code.
- **Reliability evidence** (M7): an in-process chaos / fault-injection suite (crash,
  DB failover, broken RPC, reorg) and a published
  [load-test benchmark](docs/benchmark.md).

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
| `paymentrailctl` | Operator CLI (`submit`, `approve`, `replay-webhook`, `audit verify`, `reconcile`, `loadtest`) |

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
  benchmark.md  published load-test benchmark (M7)
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
| **M5** ✅ | Policy engine, velocity limits, four-eyes approval + hash-chained audit log |
| **M6** ✅ | Reconciliation + proof-of-reserves report |
| **M7** ✅ | Chaos tests, load tests, published benchmark |

## License

[MIT](LICENSE).
