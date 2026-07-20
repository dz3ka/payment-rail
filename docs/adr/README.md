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

### Planned (not yet written)

The entries below are renumbered to the next free slots (0010 was taken by the
chain-adapter ADR above).

- ADR-0011 — Transactional outbox & event schema (M4)
- ADR-0012 — Fake-chain test harness for scripted reorgs (M3)
- ADR-0013 — Signer HSM/MPC custody upgrade & mTLS caller auth (M2+) — *follow-up to
  ADR-0009 (base custody now decided there)*
