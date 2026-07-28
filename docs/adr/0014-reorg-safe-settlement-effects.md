# ADR-0014: Reorg-safe settlement effects: clearing-account model, status-guarded idempotency, non-terminal phases

- **Status:** Accepted. **Supersedes the terminal-Confirmed / terminal-Reorged consequences of [0028].**
- **Date:** 2026-07-20
- **Deciders:** Bogdan Dzekic

## Context
M3 slice 2 (PRD F5) completes reorg-safe settlement: the chain watcher must drive
double-entry *ledger* effects as a payment's on-chain tx confirms, reorgs, and re-mines —
without regressing M1's synchronous `payments.Create`, and provable in CI with no Postgres
and no live testnet. M1's `Create` already moved money (debit source / credit dest) at API
time; the on-chain leg is layered on top, not folded in.

## Decision
- **Additive clearing-account model, not an M1 lifecycle refactor.** A per-asset house
  account `onchain_settlement` is the settlement counterparty. SETTLE (on Confirmed) = debit
  dest / credit house — releasing dest's provisional M1 credit onto the chain; REVERSAL (on
  Reorged) mirrors it; REAPPLY re-settles at the new block. A full settle→reverse→reapply
  cycle nets to a **single** settle, so Create's money is never double-counted. M1's write
  path (`InsertPayment`, `CancelPayment RETURNING *`, `journal_entries`) is byte-identical.
- **Idempotency is anchored on a row-status guard, not on `UNIQUE(kind, external_ref)`.**
  `ledger.PostWithin` validates funds *before* inserting the journal entry, so a redelivered
  confirm on an already-settled row trips `ErrInsufficientFunds` (the provisional credit is
  already released), never the unique constraint. A pre-post status check (mirroring
  `payments.Cancel`) short-circuits every reachable redelivery. The earlier "post-first,
  tolerate `ErrDuplicateEntry`" idea was removed: under real Postgres a 23505 aborts the whole
  transaction, so the following guarded UPDATE would fail with `in_failed_sql_transaction` and
  wedge the row — the opposite of a benign no-op.
- **external_ref is block-hash-scoped** (`settle:<payment_id>:<block_hash>` /
  `reverse:...`): a re-mine at a new block yields a fresh ref so REAPPLY posts cleanly, while
  a genuinely identical block cannot reappear after a reorg. Chosen over a monotonic `epoch`
  counter (razor CUT) — one fewer column, and no counter to double-settle against.
- **`StatusSink` interface owned by `internal/chain/evm`, dispatched from `Run`.** The watcher
  surfaces each `Status` to a caller-supplied sink; `cmd/chainwatcher` injects the concrete
  `settlement.Sink`. `evm` still imports no ledger/settlement/db package (dependency inversion
  preserved, verified by `go list -deps`). A nil sink is log-only (slice-1 behavior).
- **Confirmed and Reorged are now non-terminal** so a tracked tx keeps being observed across
  the reverse+reapply cycle — superseding [0028]'s terminal-drop. The existing
  `lastEmittedPhase`/`lastEmittedDepth` dedupe still prevents re-emitting a steady Confirmed,
  and [0028]'s invariant (a transient RPC error never emits Reorged) is preserved.
- **`Recorder.Link` rejects a tx_hash already bound to a different payment** rather than
  reporting a false idempotent success, so an operator can't silently mis-bind a broadcast tx.

## Consequences
- **Recovery scope is startup-seed-only (deferred to slice 3).** `cmd/chainwatcher` seeds its
  tracking set once from `ListPendingSettlements` (pending + settled rows). Two gaps are
  accepted for this slice, money conservatively safe in both: (1) a settled tx orphaned and
  never re-mined *while the watcher is down* is not reversed on restart (re-seed via `Track`
  resets it to pending with no block anchor); (2) a transient sink failure stalls that
  settlement until a restart re-seeds. Slice-3 follow-up: persist the block anchor so
  re-tracked settled rows resume in Confirmed; add in-process retry / periodic re-scan behind a
  concurrency-safe tracked map.
- Confirmed no longer terminal ⇒ the tracked set grows unbounded (finality-depth eviction is a
  documented follow-up).
- New `settlements` table (dedicated, not columns on `payments`) keeps the M1 surfaces
  untouched and models one payment → N on-chain attempts honestly.
- `paymentrailctl submit` gains an optional `--payment-id` and a Postgres dependency only on
  that path; without the flag it stays print-only (back-compatible).
- Hermetic proof (`internal/settlement` in-memory ledger fake + `evm` `fakeReader` /
  `simulated.Backend`) is the CI-load-bearing coverage; the DSN-gated integration test is not.

## Related
[0028] chain watcher & reorg harness (slice 1) — this ADR builds on its poll/Run seam and
reader interface and supersedes its terminal-phase consequences.
