# ADR-0016: Settlement recovery: block-anchor Resume, poll-riding retry + re-scan, terminal-status finality eviction

- **Status:** Accepted. **Supersedes two consequences of [0014]:** its "Recovery scope is
  startup-seed-only" gaps and its "tracked set grows unbounded" follow-up.
- **Date:** 2026-07-20
- **Deciders:** Bogdan Dzekic

## Context
M3 slice 3 closes the three recovery gaps [0014] deferred. As shipped in slice 2,
`cmd/chainwatcher` seeded its tracked set *once* at startup from
`ListPendingSettlements`, re-seeding every row via `Track` (i.e. as **pending**, no
block anchor). Three consequences: (1) a settled tx orphaned while the watcher was
down was re-observed from scratch, and a re-mine at the same depth could re-emit
settle; (2) a transient sink-delivery failure stalled that settlement until a process
restart; (3) Confirmed being non-terminal ([0014]) meant the tracked set — and the
seed query — grew without bound. All must be provable hermetically (no Postgres, no
testnet) via the existing `fakeReader`/`simulated.Backend` + in-memory fake-querier
seams, with M1 paths and [0014]'s clearing-account/idempotency model untouched.

## Decision
- **`Resume(tx, blockHash, blockNumber)` is a NEW watcher method, not an extended
  `Track`.** It seeds `{phase:PhaseConfirmed, blockHash, blockNumber,
  lastEmittedPhase:PhaseConfirmed, lastEmittedDepth:depth}`, so a re-tracked *settled*
  row does **not** re-emit settle, yet a reorg-while-down still surfaces as Reorged
  through the normal poll path. `Resume` is idempotent (no-op if the key is already
  tracked, mirroring `Track`) so the per-tick re-scan never clobbers a live entry.
  Persisted via migration 0004's nullable `settled_block_hash`/`settled_block_number`
  (written by `settle`, NULLed by `MarkSettlementReorged`). Legacy settled rows with a
  NULL anchor fall back to `Track`-as-pending — money-safe, no backfill.
- **Retry rides the poll loop; no buffer.** On a sink error for a **Confirmed** Status,
  `Run` rolls that entry `PhaseConfirmed→PhaseMined` and zeroes
  `lastEmittedPhase`/`lastEmittedDepth`; the next poll tick re-reads receipt+header and
  re-emits Confirmed (zero extra RPC). The razor-proposed `undelivered` buffer map was
  **CUT** — the existing convergent loop is the retry mechanism. A failed Reorged
  delivery needs no rollback (the row is still `settled` with its anchor; the re-scan
  `Resume`s it and poll re-detects the reorg). Sink `settle`/`reverse`/`finalize` are
  all row-status-idempotent, so redelivery is safe.
- **Periodic re-scan is a goroutine in `cmd/chainwatcher`** (on `WatcherPollInterval`)
  that re-runs `ListPendingSettlements` and re-seeds via the same `Track`/`Resume`
  helper under the watcher's existing mutex. Payments submitted while running become
  visible without a restart. `evm` still imports no db/settlement package.
- **Finality eviction uses a terminal DB status, not in-memory-only skipping**
  (razor CUT overridden). Past `finalityDepth` on the canonical path the watcher emits
  a terminal `PhaseFinalized` Status → Sink `finalize` marks the row `finalized` (one
  guarded UPDATE, **no journal entry** — settle already moved the money) → the row
  leaves `ListPendingSettlements`; the watcher also deletes the key. A durable terminal
  state is *required* to bound the seed query: an in-memory skip still returns unbounded
  rows from the DB, and an age filter would wrongly drop legitimately-old pending rows.
  The watcher is the only holder of head+anchor and its only outward channel is
  `Status`, so a terminal Phase is the minimal dependency-respecting signal.
- **`finalityDepth` is a `NewWatcher` parameter, hardcoded 64 at the call site**
  (Ethereum's ~2-epoch finality), validated `> depth`. No `internal/config` field, no
  env key this slice (razor DEFER). Trigger to promote to a config knob: a second chain
  whose finality differs from Ethereum's.

## Consequences
- Recovery is now Resume + in-process retry + periodic re-scan; [0014]'s
  "startup-seed-only" gaps (1) and (2) are closed.
- The tracked set and the seed query are both bounded by finality eviction + the
  terminal `finalized` status; [0014]'s "grows unbounded" follow-up is closed.
- `settlements` gains two nullable anchor columns and a fourth status `finalized`
  (migration 0004); `finalize` is a pure status transition (no ledger movement).
- `cmd/chainwatcher` adds `golang.org/x/sync/errgroup` (promoted indirect→direct) to
  coordinate `w.Run` + the re-scan goroutine under one context; a clean SIGTERM
  (`context.Canceled`) still exits 0, a genuine error from either still propagates.
- **~~Deferred (out of scope, follow-up ticket)~~ RESOLVED 2026-07-20:** a `reorged`
  row was not in `ListPendingSettlements`, so a process restart *during the reorg
  window* (after detection, before re-settle) lost the tx. Pre-existing since slice 2.
  Fixed by adding `'reorged'` to the `ListPendingSettlements` status filter; `seed`
  already routes any non-settled row to `Track`, so a reorged row re-tracks as pending
  and the convergent re-settle loop resumes on re-confirmation. `Track` (not `Resume`)
  is required: a reorged tx *must* re-emit settle when it re-confirms — the anchor was
  reversed — which the Confirmed-anchor `Resume` path deliberately suppresses.

## Related
[0014] reorg-safe settlement effects (slice 2) — this ADR closes its two deferred
recovery consequences and reuses its `StatusSink`/`Recorder` seams and dedupe fields.
[0028] chain watcher & reorg harness (slice 1) — poll/Run seam and `lastEmitted` dedupe.
