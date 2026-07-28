# Architecture Decision Records

Each ADR captures one decision a competent reviewer would ask "why?" about:
the context, the choice, the alternatives weighed, and the consequences. ADRs
are immutable once accepted — to change a decision, add a new ADR that
**supersedes** the old one (and mark the old one Superseded).

Numbering is sequential (`NNNN-kebab-title.md`). Status is one of:
`Proposed` · `Accepted` · `Superseded by ADR-XXXX` · `Deprecated`.

## Template

```markdown
# ADR-NNNN: <title>

- **Status:** Accepted
- **Date:** YYYY-MM-DD
- **Deciders:** <who>

## Context
<the forces at play: requirements, constraints, what makes this non-obvious>

## Decision
<the choice, stated plainly>

## Alternatives considered
<each rejected option and why it lost>

## Consequences
<what becomes easier, what becomes harder, follow-ups triggered>
```

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-service-topology-and-repo-layout.md) | Service topology & repo layout | Accepted (gRPC/ledger-split superseded for M1 by 0006) |
| [0002](0002-persistence-and-data-access.md) | Persistence & data access (sqlc) | Accepted |
| [0003](0003-local-dev-messaging-redpanda.md) | Local dev messaging (Redpanda over Kafka) | Accepted |
| [0004](0004-ledger-schema-and-transaction-isolation.md) | Ledger schema (derived balances) & transaction isolation | Accepted |
| [0005](0005-idempotency-key-store.md) | Idempotency-key store for the payments API | Accepted |
| [0006](0006-in-process-ledger-transport-m1.md) | In-process ledger transport for M1 (supersedes 0001 for M1) | Accepted |
| [0007](0007-payment-ledger-tx-composition.md) | Payment → ledger tx composition & reversing-entry cancel | Accepted |
| [0008](0008-proto-toolchain-buf.md) | Proto toolchain (buf via Go tool directives) | Accepted |
| [0009](0009-signer-key-custody-and-spend-limits.md) | Signer key custody & per-key spend limits | Accepted |
| [0010](0010-chain-adapter-port-and-nonce-strategy.md) | Chain-adapter port, signer-boundary isolation & nonce strategy | Accepted |
| [0011](0011-transactional-outbox-and-event-schema.md) | Transactional outbox & event schema (M4) | Accepted |
| [0012](0012-fake-chain-test-harness-for-reorgs.md) | Fake-chain test harness for scripted reorgs (M3) | Accepted |
| [0013](0013-signer-custody-upgrade-and-mtls.md) | Signer HSM/MPC custody upgrade & mTLS caller auth (M2+) | Proposed (deferred; follow-up to ADR-0009) |
| [0014](0014-reorg-safe-settlement-effects.md) | Reorg-safe settlement effects: clearing account, non-terminal phases (M3) | Accepted (supersedes ADR-0028's terminal-phase consequences) |
| [0015](0015-hermetic-go-dev-tooling.md) | Hermetic Go dev tooling (golangci-lint & sqlc as tool directives) | Accepted |
| [0016](0016-settlement-recovery-resume-anchor-finality-eviction.md) | Settlement recovery: block-anchor Resume, poll-riding retry, finality eviction (M3) | Accepted (supersedes two consequences of ADR-0014) |
| [0017](0017-webhook-consumer-db-queue-delivery.md) | Webhook consumer: DB-queue delivery, lease-claim worker, HMAC signing (M4) | Accepted |
| [0018](0018-policy-engine-seam-denylist-screening.md) | Policy engine seam & destination-address denylist screening (M5) | Accepted |
| [0019](0019-velocity-limits.md) | Per-signing-key sliding-window velocity limits (M5) | Accepted |
| [0020](0020-four-eyes-approval-gate.md) | Four-eyes approval above a threshold (M5) | Accepted |
| [0021](0021-hash-chained-audit-log.md) | Append-only hash-chained audit log (M5) | Accepted |
| [0022](0022-reconciliation-model.md) | Reconciliation model: finalized-only expected, proof-of-reserves (M6) | Accepted |
| [0023](0023-multi-treasury-registry.md) | Multi-treasury registry: per-asset expected, per-address actual (M6) | Accepted |
| [0024](0024-onchain-balance-reader-port.md) | On-chain BalanceReader port & fail-closed balanceOf decode (M6) | Accepted |
| [0025](0025-loadtest-hermetic-migration-bootstrap.md) | Hermetic Go migration bootstrap for the load-test harness (M7) | Accepted |
| [0026](0026-chaos-suite-build-tag-gating.md) | Gating the chaos suite: the repo's first build tag & DSN skip (M7) | Accepted |
| [0027](0027-in-process-fault-injection-via-seams.md) | In-process fault injection through existing seams (M7) | Accepted |
| [0028](0028-chain-watcher-and-reorg-harness.md) | Chain watcher: poll seam, two-pronged reorg detection, simulated-backend harness (M3) | Accepted (terminal-phase consequences superseded by ADR-0014) |
