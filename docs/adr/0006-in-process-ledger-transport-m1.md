# ADR-0006: In-process ledger transport for M1

- **Status:** Accepted
- **Date:** 2026-07-16
- **Deciders:** Bogdan Dzekic
- **Supersedes (for M1):** ADR-0001's gRPC-between-services decision

## Context

ADR-0001 set gRPC as the internal transport between services (api → ledger, etc.)
and separate binaries per service. M1 ships only the payments API over the ledger,
and — critically — a payment and its journal entry must commit in **one** database
transaction (ADR-0007). A network hop between `api` and `ledger` would put a
process and connection boundary in the middle of that transaction, forcing a
distributed-commit protocol (or giving up atomicity) purely to honor a topology
decision made before the code existed. The trust boundary that motivated the split
(the signer) is not on the payments path.

## Decision

For M1, `cmd/api` imports `internal/ledger` and `internal/payments` **directly** and
runs the ledger in-process. There is no gRPC, no proto, and no network call on the
payment write path. `cmd/ledger` remains an M0 skeleton binary; the network split is
deferred until there is a real reason to cross a process boundary here (independent
scaling, or a genuine isolation requirement like the signer's).

## Alternatives considered

- **gRPC api → ledger now (per ADR-0001):** honors the original topology but breaks
  single-transaction payment+journal atomicity, or forces two-phase commit / an
  outbox for what is currently one database and one process. Cost with no M1 benefit.
- **Shared-library ledger but still a separate `cmd/ledger` process on the write
  path:** same transaction-boundary problem as gRPC without even the typed contract.

## Consequences

- Easier: payment+journal commit atomically in one local `ExecTx` (ADR-0007); no
  proto scaffolding, no inter-service failure handling, faster M1 delivery.
- Harder / deferred: the api↔ledger seam is now a Go package boundary, not a wire
  contract. Splitting ledger into its own service later means introducing the gRPC
  surface *and* solving the cross-service transaction (outbox/2PC) that this ADR
  avoids — a deliberate, scoped debt. The `internal/ledger` `Store`/`Querier`
  interfaces keep the seam clean so that split stays mechanical, not a rewrite.
- ADR-0001's status line is annotated to point at this M1 supersession.
