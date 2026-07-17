# ADR-0004: Ledger schema (derived balances) & transaction isolation

- **Status:** Accepted
- **Date:** 2026-07-16
- **Deciders:** Bogdan Dzekic

## Context

The ledger is the source of truth for money and must never report a balance that
never existed, never let an account go negative, and never post a journal
transaction whose debits and credits disagree. Two coupled decisions shape every
write path and had to be settled before the domain code (M1) could be trusted:

1. **Where does a balance live?** Storing a running balance column invites
   drift: any bug or partial write leaves the stored number disagreeing with the
   entry history, and there is no cheap way for the database to prove the two
   consistent.
2. **Under what isolation and locking do concurrent posts run?** The no-negative
   and balanced invariants span multiple rows (all accounts a transaction
   touches), so correctness depends on how concurrent transactions see and lock
   those rows. PRD §6 permits careful, explicit locking rather than mandating the
   strongest isolation.

## Decision

**Balances are derived, never stored.** An account's balance is
`Σ(credit lines) − Σ(debit lines)` computed on demand
(`GetAccountBalance` in `db/query/ledger.sql`). The journal entry history is the
only source of truth; there is no balance column to drift.

**Transactions run at READ COMMITTED with an ordered `SELECT … FOR UPDATE`.**
`Service.PostEntry` collects every account id the entry touches, sorts and
deduplicates them, and locks the rows in that deterministic order
(`GetAccountsForUpdate` — `ORDER BY id … FOR UPDATE`) before reading each
account's derived balance and applying the entry.

**Invariants are split between the database and the application by cost:**

- **Database (cheap, per-row, always-on):** `amount > 0`, a `direction` CHECK
  (`debit`/`credit`), foreign keys, and `UNIQUE(kind, external_ref)` on
  `journal_entries` (ledger-level idempotency — a duplicate post surfaces as a
  23505 unique violation, translated to `ErrDuplicateEntry`).
- **Application (multi-row invariants):** *balanced* (debits == credits, via the
  pure `Balanced`) and *no account goes negative* (via `ApplyToBalances` over the
  freshly-locked balances). These need the values of several rows at once, which
  a single-row CHECK cannot express.

## Alternatives considered

- **Stored / materialized balance column.** O(1) reads, but it is a second source
  of truth that can silently disagree with the entry history, and it is exactly
  what "balances derived, not stored" (the ledger's core invariant) forbids. A DB
  CHECK enforcing `balance >= 0` is only *possible* with such a stored column —
  rejecting the column also rejects that CHECK, pushing no-negative into the app.
- **SERIALIZABLE isolation.** Would let Postgres detect the multi-row conflicts
  for us, but every writer must then handle 40001 serialization failures with a
  retry loop, adding latency and complexity for a workload where the touched
  account set is known up front and can simply be locked. Ordered `FOR UPDATE`
  under READ COMMITTED gives the same safety without a retry loop.
- **Unordered row locking.** Correct in isolation but deadlock-prone under
  concurrency (two transactions grabbing the same two accounts in opposite
  order). Sorting the lock set by id makes the lock acquisition order global and
  deadlock-free.

## Consequences

- Easier: balances can never drift from history; no reconciliation of a stored
  balance against entries is ever needed. No 40001 retry machinery. Deadlocks are
  designed out rather than caught. The DB enforces the cheap invariants for free
  on every path, including ad-hoc writes.
- Harder: balance reads cost an aggregate over an account's lines, so a hot
  account's history growth is a future performance concern (mitigate later with a
  covering index or periodic snapshotting that is *derived from*, not a
  replacement for, the history). The no-negative invariant lives in application
  code, so every write path must go through `Service.PostEntry` — direct SQL
  inserts bypass it. The ordered-lock discipline must be honored by every future
  query that locks accounts.
- Follow-up: the production `Store` implementation (real `*sql.DB` `ExecTx` with
  BEGIN/COMMIT/ROLLBACK and the ordered `FOR UPDATE`) lands in WP5; a concurrency
  test proving no-negative under parallel holds requires `-race` and a live
  Postgres. Idempotency-key storage for the payments API is its own decision
  (planned ADR-0005, WP4).
