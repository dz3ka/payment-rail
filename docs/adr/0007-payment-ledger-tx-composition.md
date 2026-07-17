# ADR-0007: Payment → ledger transaction composition & reversing-entry cancel

- **Status:** Accepted
- **Date:** 2026-07-16
- **Deciders:** Bogdan Dzekic

## Context

Creating a payment writes two related rows: the journal entry (the money movement,
owned by `internal/ledger`) and the `payments` row (the API-level record, owned by
`internal/payments`). If these commit separately, a crash between them leaves a
payment with no ledger entry or vice versa — the exact drift the derived-balances
invariant (ADR-0004) exists to prevent. Cancelling a payment must likewise undo the
money movement without ever mutating history. The two packages own different tables
but must share one transaction.

## Decision

`internal/ledger` exposes **`PostWithin(ctx, q, e)`** — the balance-checking,
ordered-`FOR UPDATE`, balanced-invariant post logic parameterized over a `Querier`
`q` rather than opening its own transaction. `internal/payments.Create` opens one
`tx.ExecTx`, and inside it calls `ledger.PostWithin` (journal entry) **and** inserts
the payment row, so both commit or both roll back. `Service.PostEntry` keeps its own
`ExecTx` wrapper for standalone ledger use; `PostWithin` is the shared inner seam.

**Cancel is a reversing journal entry:** a new entry that debits the original
destination and credits the original source (the mirror of the create), appended in
one tx together with the payment's status flip. Prior entries are never updated or
deleted — cancellation is itself append-only, so derived balances stay correct and
the full history is auditable.

## Alternatives considered

- **Two separate transactions (ledger, then payment):** simplest wiring but leaves a
  crash window where the two tables disagree; unacceptable for money.
- **Nested / savepoint transactions:** Postgres has no true nested transactions;
  savepoints add rollback complexity for no benefit when one flat tx suffices.
- **Mutating or deleting the original entry on cancel:** destroys the audit trail and
  violates the append-only ledger; a reversing entry preserves history and still
  nets the balance to zero effect.

## Consequences

- Easier: payment+journal atomicity with no distributed-commit machinery (enabled by
  the in-process ledger, ADR-0006); cancel composes from the same primitive; the
  ledger's locking/invariant logic lives in exactly one place (`PostWithin`).
- Harder: `PostWithin` must be called *inside* a caller-owned transaction that already
  holds the right isolation — callers can misuse it by passing a non-transactional
  `Querier`. The `Store`/`Querier` seam and the payments service being the only caller
  keep that contained.
