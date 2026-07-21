# M3 — Reorg-safe settlement: a state machine that survives chain reorgs and process restarts, and a mutex that deliberately does *not* span the network

> Scope: this lesson is about *turning a stream of noisy, revisable chain observations
> into ledger truth that stays correct even when a block is orphaned or the process
> dies mid-flight*. The headline achievements are (1) a **poll-based watcher whose
> `PhaseConfirmed` is deliberately non-terminal** — every pass re-verifies the block
> *anchor* `(blockHash, blockNumber)` so a deep reorg reverses a confirmed tx, with a
> **finality-depth eviction** that bounds the map that non-terminality would otherwise
> grow forever; (2) **reorg-safe ledger effects** — settle and reverse are exact mirror
> journal entries whose `external_ref` is keyed by block hash, so a re-mine at a new
> block settles again cleanly, and each effect commits its journal entry *and* the
> settlement-row status flip in **one `ExecTx`**; (3) **idempotency anchored on the
> row-status guard, not the ledger's unique constraint**, because a redelivered confirm
> would trip the *balance* check before the duplicate check — so the guard short-circuits
> before any post; (4) **restart recovery via `Resume` vs `Track`** — a settled row with
> a persisted anchor re-seeds as a `Confirmed` anchor (catching an in-flight reorg without
> re-emitting the settle that already landed), everything else tracks as pending; and
> (5) a **poll-riding retry** that, on a sink failure, rolls a `Confirmed` entry back to
> `Mined` and zeroes its emit-dedupe so the next ordinary tick re-emits — no extra RPC, no
> retry goroutine. The through-line from M2: the watcher reuses the nonce allocator's
> `sync.Mutex` idiom but **inverts its central lesson** — here the mutex is *never* held
> across an RPC, because the network I/O is not the thing being serialized. Skim
> `m2-evm-chain-adapter.md` if the "lock spans the network" reasoning isn't fresh; the
> contrast is the whole point.

## 1. What we built

M3 is the read side of the chain: `internal/chain/evm/watcher.go` is a poll-based
`Watcher` that, given a set of transaction hashes to `Track`, repeatedly asks a node
"is this receipt still there, and how deep is it buried?" and turns the answer into a
small stream of `Status` transitions — `Pending → Mined → Confirmed → Finalized`, with
`Reorged` as a branch off `Mined`/`Confirmed` whenever the block that carried the tx
stops being canonical. The watcher is chain-only: it emits a go-ethereum-free `Status`
(a `chain.TxHash`, plain uints, a phase label) through a `StatusSink` interface it owns,
so nothing in `evm` knows a ledger exists. That is the same dependency inversion the M2
adapter used for its `Signer`/`ethRPC` seams, applied to the *output* edge.

`internal/settlement/settlement.go` is the sink. A `Recorder` writes the payment↔tx-hash
link at submit time; a `Sink` consumes the watcher's `Status` stream and posts the
double-entry effects that move a provisional credit into the `onchain_settlement` house
account when a tx confirms (`settle`), back out when it reorgs (`reverse`), and records
finality as a pure status flip with no money movement (`finalize`). Every effect rides
the M1 ledger's `PostWithin`/`ExecTx` seam so the journal entry and the settlement-row
status change commit in one database transaction — the settlement never stores a balance
and never bypasses double-entry, exactly like payments.

`cmd/chainwatcher/main.go` is the composition root that wires a real `ethclient` node,
a real Postgres-backed sink, and — the part that makes restarts safe — a startup **seed**
plus a periodic **rescan** that re-registers the settlements Postgres still holds as
in-flight. Migrations `0003` and `0004` add the `settlements` table (a `tx_hash UNIQUE`
link, a guarded `status` enum, and — after `0004` — a `(settled_block_hash,
settled_block_number)` anchor and a terminal `finalized` status). The two hard problems
this milestone exists to solve are **reorg-safety** (a confirmed effect must be reversible
if the chain rewrites history under it) and **restart-recovery** (a process that dies must
resume watching the right transactions in the right phase, without double-posting money
or losing a reorg that happened while it was down).

## 2. The design decision

### Decision A: `PhaseConfirmed` is non-terminal, and a reorg is only ever declared on *positive evidence*

**The problem.** A naive confirmation watcher treats "buried N deep" as the finish line:
once a tx has N confirmations, mark it done and stop looking. But proof-of-stake Ethereum
can still reorg blocks that are only a handful deep, and a tx that was 12-confirmations
"settled" can be orphaned onto a fork and vanish. If `Confirmed` is terminal, the money
we moved on that confirmation is now wrong and nothing is watching to fix it. Reorg-safety
*requires* that a confirmed tx stay under observation.

**The chosen approach — re-verify the anchor every pass, and reverse only on proof.**
Each tracked entry records the block it was mined in as an **anchor**: `blockHash` and
`blockNumber`. `PhaseConfirmed` is non-terminal — every poll re-reads the receipt and
re-reads the canonical header *at the recorded height*, and declares a reorg (`reverse`)
in exactly two cases, both of which are *positive evidence* the tx left the canonical
chain:

```go
receipt, err := w.reader.TransactionReceipt(ctx, hash)
if err != nil {
    if errors.Is(err, ethereum.NotFound) {
        w.reverse(t, tx, bHash, bNum, &emitted) // receipt gone: block orphaned
        continue
    }
    // transient: a transport fault is NOT a reorg
    w.log.WarnContext(ctx, "watcher: receipt query failed", ...)
    continue
}
// ...
if hdr.Hash() != bHash {
    w.reverse(t, tx, bHash, bNum, &emitted) // different block at our height
    continue
}
```

The load-bearing invariant is the negative space: a *transient* read failure (any error
that is not `ethereum.NotFound`, or a `nil` header) is logged and skipped, never mistaken
for a reorg. A dropped RPC connection must never manufacture a reversal, because reversing
a settlement moves money — a false reorg would debit the house and credit a destination
that is, in reality, still validly settled. The two reorg triggers are chosen precisely
because they are *unambiguous*: `ethereum.NotFound` on a receipt means the node no longer
has that tx in a canonical block, and a header hash that differs from our anchor at the
same height means a *different* block won that slot. Everything else holds position and
retries next tick.

**Alternative 1: terminal `Confirmed`, trust N confirmations forever.** Simpler, and it's
what most tutorials show. Rejected because it is not reorg-safe at all: a reorg deeper than
the moment we stopped watching is silently un-handled, and the ledger diverges from the
chain with nothing to reconcile it. The entire milestone goal is "correct across reorgs,"
which this forfeits.

**Alternative 2: subscribe to a reorg/head-reorg event feed instead of polling.**
go-ethereum can push new heads; some nodes expose reorg notifications. Rejected for this
slice because it couples correctness to a stateful subscription that silently gaps on
reconnect — you'd still need a poll-based reconciliation to be sure, so you'd own both.
Polling with a re-verified anchor is *self-healing*: every pass re-derives truth from the
node's current canonical view, so a missed tick or a dropped connection costs latency, not
correctness. The cost is RPC load (three reads per tracked tx per tick), bounded by
Decision B.

### Decision B: finality-depth eviction bounds the map that non-terminality would grow forever

**The problem.** Decision A made `Confirmed` non-terminal, so entries never leave the
`tracked` map on their own — and a payment rail accrues settled transactions without
bound. A watcher that re-verifies every historical tx forever is a memory leak and an
ever-growing RPC bill.

**The chosen approach — a second, deeper threshold at which reversal is treated as
impossible, at which point the entry is evicted.** `NewWatcher` takes a `finalityDepth`
that must strictly exceed the confirmation `depth`. On the canonical path, once an anchor
is buried at least `finalityDepth` deep, the watcher emits a terminal `PhaseFinalized` and
`delete`s the entry:

```go
if head >= bNum && head-bNum+1 >= w.finalityDepth {
    w.emit(t, tx, PhaseFinalized, bHash, bNum, head-bNum+1, &emitted)
    delete(w.tracked, tx)
}
```

This is only reachable *after* the anchor re-verification above has passed — a divergent
header already `reverse`d-and-`continue`d — so finality never races a reorg: the code only
finalizes a tx it has just re-confirmed is still canonical this very pass. The
`finalityDepth > depth` guard in `NewWatcher` is a genuine safety check, not a style
nicety: a finality that fired at or before confirmation depth would evict a tx while a
reorg could still legitimately reverse it, reintroducing exactly the bug Decision A closed.
The constructor fails loudly rather than build a watcher that silently drops
still-reversible transactions.

### Decision C: reorg-safe ledger effects are exact mirror entries keyed by block hash

**The problem.** When a tx confirms we move money; when it reorgs we must move it back;
when it re-mines at a *new* block we must move it again. Double-entry accounting forbids
mutating or deleting a posted entry — the ledger is append-only — so "undo" has to be a
*new*, compensating entry, and "redo" has to be distinguishable from a duplicate of the
original.

**The chosen approach — settle and reverse are byte-for-byte mirror postings, and the
`external_ref` carries the block hash.** `settle` debits the destination and credits the
house; `reverse` swaps the two lines. The idempotency key on each entry embeds the block:

```go
// settle
ExternalRef: fmt.Sprintf("settle:%s:%s", sett.PaymentID, st.BlockHash),
// reverse
ExternalRef: fmt.Sprintf("reverse:%s:%s", sett.PaymentID, st.BlockHash),
```

Keying on the block hash is what makes reverse-then-reapply correct. If a tx settles at
block `0xAAA`, reorgs, and re-mines at block `0xBBB`, the re-settle produces
`settle:<pid>:0xBBB` — a *different* `external_ref` than the original `settle:<pid>:0xAAA`,
so the ledger's `UNIQUE(kind, external_ref)` does not mistake the legitimate re-settle for
a duplicate. Had we keyed on payment ID alone, a re-mine would collide with the original
and be silently dropped, leaving the destination un-credited. The compensating reversal is
the standard way to "undo" in an append-only ledger — the same shape a payments
cancellation or a saga compensating action takes.

Both the journal entry and the settlement-row status flip commit in **one** `ExecTx`:

```go
return s.tx.ExecTx(ctx, func(q db.Querier) error {
    sett, err := q.GetSettlementByTxHash(ctx, txHash)
    // ... resolve payment + house account ...
    if st.Phase == evm.PhaseConfirmed {
        return s.settle(ctx, q, sett, pay, house, st) // PostWithin + MarkSettlementSettled
    }
    return s.reverse(ctx, q, sett, pay, house, st)
})
```

If the process dies between posting the entry and flipping the status, the transaction
rolls back and *neither* happened — there is no window where the money moved but the row
still says `pending`. This is the M1 `PostWithin` seam doing exactly what it was built for:
compose an effect and its bookkeeping atomically. `finalize` is deliberately *not* in this
shared transaction — it moves no money (settle already did), so it is a pure guarded
status flip in its own single-statement `ExecTx`, and it resolves neither the payment nor
the clearing account.

### Decision D: idempotency is anchored on the row-status guard, *not* the ledger's unique constraint

**The problem.** The watcher can redeliver a `Confirmed` for a tx that already settled —
after a restart, or the poll-riding retry of Decision F. The ledger *has* a
`UNIQUE(kind, external_ref)` that would reject a duplicate post. The tempting shortcut is
to lean on it: just try to post, and treat the unique violation as "already done."

**The chosen approach — check `sett.Status` and short-circuit *before* posting.** Both
`settle` and `reverse` open with a status guard:

```go
// settle
if sett.Status == "settled" {
    return nil // idempotent no-op, checked before any post
}
// reverse
if sett.Status != "settled" {
    return nil // only a settled tx can reorg
}
```

The reason this is not redundant with the unique constraint is subtle and worth
internalizing: `PostWithin` validates *funds* before it ever hits the unique index. A
redelivered confirm on an already-settled row would try to debit the destination a second
time — but the destination's provisional credit was *already released* by the first
settle, so the second attempt trips `ErrInsufficientFunds` (or debits money that isn't
there) *before* the duplicate `external_ref` is ever detected. The unique constraint is a
correct backstop for a genuinely-identical double-post, but it is the *wrong* guard for
redelivery, because redelivery reaches the balance check first. The row-status guard
short-circuits above all of that, so redelivery is a clean no-op with no post attempted.

There is a second, deeper reason the "catch the unique violation and continue" pattern is
actively wrong under real Postgres: a unique violation *aborts the entire transaction*.
Once it fires, every subsequent statement in the same tx fails with
`in_failed_sql_transaction` — so a `MarkSettlementSettled` after a swallowed duplicate
would itself fail. There is no "benign continue" inside a Postgres transaction after a
constraint trips. The row-status guard is not an optimization; it is the only correct
place to make redelivery idempotent.

The guarded SQL `UPDATE`s reinforce this from the database side. `MarkSettlementSettled`
is `WHERE tx_hash = $1 AND status IN ('pending','reorged')`; a concurrent or repeated
settle matches *no row* and the caller sees `sql.ErrNoRows`, which the sink reads as "already
settled, idempotent no-op." The status column and the guard together form a small state
machine enforced in the database, not just in Go.

### Decision E: restart recovery seeds `Resume` for a settled anchor, `Track` for everything else

**The problem.** The watcher's `tracked` map is in-memory. A restart loses it entirely.
The settlements table is the durable truth, but re-registering every in-flight row as a
fresh `Pending` track would *re-emit the settle* for transactions that already settled
before the crash — double-posting money.

**The chosen approach — branch the recovery path on the persisted row state.** `seed` in
`cmd/chainwatcher` chooses:

```go
func seed(w txTracker, row db.Settlement) error {
    tx := chain.TxHash(row.TxHash)
    if row.Status == "settled" && row.SettledBlockHash.Valid {
        return w.Resume(tx, row.SettledBlockHash.String, uint64(row.SettledBlockNumber.Int64))
    }
    return w.Track(tx)
}
```

`Resume` seeds the entry as a `PhaseConfirmed` anchor with `lastEmittedPhase =
PhaseConfirmed` and `lastEmittedDepth = w.depth`. That pre-loaded dedupe state is the
whole trick: on the next canonical poll the tx re-verifies as still-confirmed, and the
emit-dedupe (Decision F's `emit`) *suppresses* a duplicate `Confirmed` — so the settle
that already landed is not re-emitted. But if the anchor has diverged while the watcher was
down, the very same re-verification surfaces `Reorged`, and the reversal that was owed gets
posted on restart. `Resume` re-establishes exactly enough state to catch a reorg without
replaying a settle.

Everything else — a `pending` row, a `reorged` row, or a legacy `settled` row whose anchor
columns predate migration `0004` and are still `NULL` — falls through to `Track` as
`Pending`, the money-safe default. Note `ListPendingSettlements` deliberately includes
`reorged` rows (`WHERE status IN ('pending','settled','reorged')`): a process that restarts
*inside the reorg window* must re-seed the reorged tx so its eventual re-mine is still
observed, rather than dropping it. `finalized` is terminal and excluded — there is nothing
left to watch. The honest limitation, documented in the package doc: a tx orphaned *and
never re-mined* while the watcher is DOWN is not auto-reversed on restart, because the
re-seed resets it to `pending` with no anchor; money stays conservatively safe (the
destination keeps its provisional credit) but a slice-3 reconciliation pass is the real
fix.

### Decision F: the poll-riding retry — recover a dropped sink effect on the next ordinary tick

**The problem.** `Run` logs and *swallows* a sink error so the watcher keeps polling
(a sink failure must never stop confirmation tracking). But if the swallowed error was a
`Confirmed` delivery, the settlement row never learns the tx confirmed — the effect is
lost until a restart re-seeds. In-process retry would normally mean a retry queue, backoff,
a goroutine — real machinery.

**The chosen approach — mutate the tracked entry back a phase so the *existing* poll loop
re-emits, no new machinery.** When a `Confirmed` sink call fails, `Run` rolls the entry
back to `Mined` and zeroes its emit-dedupe:

```go
if s.Phase == PhaseConfirmed {
    w.mu.Lock()
    if t, ok := w.tracked[s.TxHash]; ok && t.phase == PhaseConfirmed {
        t.phase = PhaseMined
        t.lastEmittedPhase = PhasePending // zero value: force re-emit
        t.lastEmittedDepth = 0
    }
    w.mu.Unlock()
}
```

Next tick, the `Mined` branch re-reads the receipt and header (which it would do anyway)
and, finding the tx still buried past `depth`, re-emits `Confirmed` — because the dedupe
state was reset, the emit is not suppressed. The retry costs *zero* extra RPC beyond the
normal poll cadence; it rides the poll that was going to happen regardless. The two guards
are load-bearing: `ok && t.phase == PhaseConfirmed` ensures a poll that already advanced
the entry (say, to `Reorged`) between the emit and the failure handler is never clobbered
back to `Mined` — we only roll back an entry that is *still* where we left it. Only
`Confirmed` failures are rolled back; `Reorged` and `Finalized` failures need no rollback
because their recovery anchor is the persisted row, re-derived on the next pass anyway.

## 3. Language deep-dive

### 3a. The mutex is snapshotted, never held across I/O — the inverse of the nonce lesson

The M2 nonce allocator held its `sync.Mutex` *across* the sign-and-broadcast RPC, and that
was the whole lesson: the I/O *was* the critical section. The watcher uses the same mutex
idiom to reach the opposite conclusion, and understanding *why* they differ is the point.

```go
w.mu.Lock()
keys := make([]chain.TxHash, 0, len(w.tracked))
for tx := range w.tracked {
    keys = append(keys, tx)
}
w.mu.Unlock()

for _, tx := range keys {
    w.mu.Lock()
    t, ok := w.tracked[tx]
    // copy the fields we need out under the lock
    phase, bHash, bNum := t.phase, t.blockHash, t.blockNumber
    w.mu.Unlock()
    if !ok { continue }
    // ... RPC calls happen HERE, unlocked ...
    receipt, err := w.reader.TransactionReceipt(ctx, hash)
    // ... then re-acquire only to mutate ...
    w.mu.Lock()
    t.phase = PhaseConfirmed
    w.emit(t, tx, PhaseConfirmed, ...)
    w.mu.Unlock()
}
```

The lock protects exactly one thing — the `tracked` map and the `*tracked` structs it
points to — and nothing about a receipt lookup mutates that state. So the idiom is
"snapshot the keys under the lock, do all the network I/O unlocked, re-acquire only to
apply the result." Here the standard advice ("never hold a lock across I/O") is *correct*,
because unlike the nonce allocator, the network read is not the thing being serialized —
a `Track` call from another goroutine must not stall behind a slow node round-trip. The
lesson generalizes: **hold a lock across I/O only when the I/O is itself the invariant you
are protecting; otherwise snapshot and release.** Both watcher and allocator use one
`sync.Mutex` guarding one piece of state — the difference is entirely in *what the state
is* and *whether the RPC touches it*.

One Go subtlety in the snapshot: `keys := make([]chain.TxHash, 0, len(w.tracked))`
pre-sizes the slice's capacity to the map length so the `append` loop never reallocates.
And because we release the lock between snapshotting keys and processing each one, the
per-tx `t, ok := w.tracked[tx]` re-checks presence — an entry may have been evicted (say,
finalized) by a concurrent path in between, and `ok == false` cleanly skips it. Snapshot
concurrency means "the set I iterate may be slightly stale," and the re-check is how the
code stays correct under that.

### 3b. `emit` and the zero-value dedupe: how a steady poll stays quiet

A poll every 15 seconds observes the *same* confirmed tx over and over. Without dedupe the
sink would be hammered with identical `Confirmed` statuses every tick. `emit` is the gate:

```go
func (w *Watcher) emit(t *tracked, tx chain.TxHash, phase Phase, bHash common.Hash, bNum, depth uint64, out *[]Status) {
    if phase == t.lastEmittedPhase && depth <= t.lastEmittedDepth {
        return // same phase, not deeper: nothing new to say
    }
    t.lastEmittedPhase = phase
    t.lastEmittedDepth = depth
    // ... append Status to *out ...
}
```

A `Status` is surfaced only on a real transition: a *phase change*, or a *strictly deeper*
confirmation than the last one emitted. The `depth <= lastEmittedDepth` half is why a
`Mined` tx whose depth climbs 3 → 4 → 5 emits progress but a `Confirmed` tx sitting at a
steady depth stays silent. This connects directly to `reverse`, which resets the dedupe by
leaving `lastEmittedPhase = PhaseReorged, lastEmittedDepth = 0`, and to the poll-riding
retry (3a's cousin), which sets `lastEmittedPhase = PhasePending` — literally the enum's
zero value — as a deliberate "force the next emit through" signal. The comment
`// zero value: force re-emit` names the idiom: because `PhasePending` is `iota`'s zero and
no real emit ever leaves the dedupe at pending-with-depth-0 in a confirmed entry, writing
the zero value is a guaranteed cache-miss that re-opens the gate.

`out *[]Status` is a **pointer to a slice** so `emit` can `append` into the caller's
slice and have the growth (a possible reallocation) visible to the caller. A plain
`[]Status` argument would append into a *copy* of the slice header; the caller's header
would keep the old length and the appended element would be lost. Passing `*[]Status` is
Go's idiom for "an append sink an inner function writes into" — the same reason you pass
`*bytes.Buffer` rather than `bytes.Buffer`.

### 3c. `errgroup.WithContext`: two long-lived goroutines, one cancellation, one error

`cmd/chainwatcher` runs the poll loop and the rescan loop concurrently, tied to one
context, with clean shutdown semantics:

```go
g, gctx := errgroup.WithContext(ctx)
g.Go(func() error { return rescan(gctx, querier, w, cfg.WatcherPollInterval, log) })
g.Go(func() error { return w.Run(gctx, sink) })
if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
    return err
}
return nil
```

`errgroup.WithContext` returns a derived context `gctx` that is cancelled when *any*
`g.Go` function returns a non-nil error *or* when the parent `ctx` is cancelled. So a fatal
error in either goroutine tears down the other, and an external shutdown signal (through
`ctx`) stops both. `g.Wait()` blocks until both return and yields the *first* non-nil
error. The convention each goroutine follows is the standard one: return `ctx.Err()` on
cancellation. `rescan` does exactly that (`case <-ctx.Done(): return ctx.Err()`), and
`w.Run` returns `nil` on cancel. The `!errors.Is(err, context.Canceled)` filter is what
turns "we were asked to shut down" into a clean exit rather than a reported failure — a
cancelled context surfaces as `context.Canceled`, and treating that as an error would make
every graceful shutdown look like a crash. This is Go's idiomatic structured concurrency:
`errgroup` is the closest thing the stdlib-adjacent ecosystem has to "join these tasks,
cancel-all-on-first-failure," and it is the right tool the moment you have more than one
long-lived goroutine sharing a lifetime.

### 3d. Nullable columns cross the `database/sql` boundary as `sql.Null*`, not pointers

The `settlements` anchor columns are nullable — they stay `NULL` until a tx settles — and
Go models that with the `sql.Null*` wrapper types, visible where `settle` writes the anchor
and where `seed` reads it:

```go
// writing the anchor on settle
SettledBlockHash:   sql.NullString{String: st.BlockHash, Valid: true},
SettledBlockNumber: sql.NullInt64{Int64: int64(st.BlockNumber), Valid: true},

// reading it back on restart
if row.Status == "settled" && row.SettledBlockHash.Valid {
    return w.Resume(tx, row.SettledBlockHash.String, uint64(row.SettledBlockNumber.Int64))
}
```

`sql.NullString` is a two-field struct — `{String string; Valid bool}` — where `Valid`
distinguishes SQL `NULL` from the empty string. This is Go's answer to the "an `int64`
can't be null" problem that languages with nullable value types (C# `long?`, Kotlin
`Long?`) solve at the type level: Go has no nullable primitives, so the standard library
carries the null-ness in a companion boolean. The `seed` branch checks `.Valid` explicitly
before dereferencing `.String`/`.Int64` — reading `.String` on an invalid `NullString`
gives you `""`, which would be a silently wrong (all-zero) block hash. The explicit
`.Valid` gate is why a legacy settled row with `NULL` anchor columns correctly falls
through to `Track` rather than `Resume`-ing against a zero anchor. Note also `int64(...)`
and `uint64(...)` conversions at the boundary: Postgres `BIGINT` is signed (`int64` in Go),
but a block number is naturally `uint64`, so the code converts explicitly at each crossing —
Go never converts integer types implicitly, which is exactly what forces you to notice the
signedness mismatch at the boundary rather than discovering it as a wrap-around bug.

## 4. What would break

- **A false reorg from a transient RPC error (the money bug).** If the watcher treated
  *any* receipt-lookup error as "block orphaned," a dropped connection would `reverse` a
  validly-settled tx: debit the house, credit a destination that never lost its settlement.
  Avoided by declaring a reorg *only* on `ethereum.NotFound` (positive absence) or a
  divergent canonical header — every other error is logged and skipped, holding position.

- **Double-posting money on restart.** Re-tracking a settled tx as fresh `Pending` would
  re-emit `Confirmed` and post the settle a second time. Avoided by `seed` branching to
  `Resume` (a pre-deduped `Confirmed` anchor) for settled rows, so the already-landed
  settle is suppressed while a real reorg still surfaces.

- **A redelivered confirm tripping the balance check instead of a benign no-op.** Leaning
  on the ledger's unique constraint would reach `PostWithin`'s funds validation *first*
  (the provisional credit is already released), failing with `ErrInsufficientFunds` — and
  under Postgres a unique violation would then abort the whole transaction, failing the
  subsequent `Mark`. Avoided by the row-status guard that short-circuits before any post.

- **An unbounded `tracked` map.** Non-terminal `Confirmed` means entries never leave on
  their own; without eviction the map and the RPC load grow without bound. Avoided by the
  finality-depth eviction — and the `finalityDepth > depth` constructor guard stops an
  operator from setting finality so shallow it evicts still-reversible transactions.

- **A lost `Confirmed` effect after a sink failure.** A swallowed sink error would strand
  the settlement until a restart. Avoided by the poll-riding retry: roll the entry back to
  `Mined` and zero its dedupe so the next ordinary tick re-emits, guarded on the entry
  still being `Confirmed` so a concurrent advance is never clobbered.

- **A re-mine silently dropped as a duplicate.** If `external_ref` were keyed on payment ID
  alone, a re-settle at a new block would collide with the original and be swallowed by
  `UNIQUE(kind, external_ref)`, leaving the destination un-credited. Avoided by embedding
  the block hash in the key so `0xAAA` and `0xBBB` settles are distinct entries.

- **A partial commit leaving money moved but the row still `pending`.** A crash between the
  journal post and the status flip would desync the ledger and the settlement. Avoided by
  running both inside one `ExecTx` — they commit together or roll back together.

- **A leaked API key in the logs.** go-ethereum wraps HTTP failures in `*url.Error`, whose
  `Error()` embeds the full endpoint URL — and managed nodes carry the API key in that URL.
  `RedactRPCError` reduces the URL to `scheme://host` before any RPC error is logged, and
  the composition root never logs `ChainRPCURL` at all.

## 5. Compared to what you know

- **The watcher is an event-sourced projection with reversible effects.** If you've built a
  read-model that consumes an event stream and posts compensating entries on a retraction,
  this is the same shape: the chain is the (revisable) event log, `Status` transitions are
  the events, and `reverse` is a retraction handler. The twist a chain adds over a Kafka
  topic is that history itself is mutable — a "past event" can be un-happened by a reorg —
  which is why the projection must *re-verify* rather than trust an append-only offset.

- **`Resume` vs `Track` is rehydrating an actor/aggregate from a snapshot.** A settled row
  with an anchor is a persisted snapshot; `Resume` reconstitutes the in-memory phase from
  it, exactly like loading an aggregate's last snapshot before replaying newer events.
  Where the analogy breaks: there is no event replay here — the "newer events" are re-derived
  live from the node each poll, because the chain is the source of truth, not a local log.

- **The row-status guard is optimistic-concurrency by state, not by version.** Instead of a
  `WHERE version = $expected` compare-and-swap, the guarded `UPDATE ... WHERE status IN
  ('pending','reorged')` uses the *state itself* as the precondition, and `sql.ErrNoRows`
  is the "someone else already advanced this" signal. If you've used
  `@Version`/optimistic locking in JPA/Hibernate, this is the same idea with the status
  column playing the version's role.

- **`errgroup` is `structured concurrency` / a scoped `CompletableFuture.allOf` with
  cancellation.** The closest Java analogue is a scope that cancels all children on first
  failure (JEP 453 structured concurrency, or a hand-rolled `ExecutorService` +
  `CompletionService`). `errgroup.WithContext` bundles "run these, cancel-all-on-first-error,
  join, return first error" into four lines — the discipline you'd otherwise assemble by
  hand around a thread pool.

- **`sql.NullString` is `Optional<String>` at the DB boundary — but a struct, not a
  wrapper class.** C#'s `long?` or Kotlin's `Long?` bake nullability into the type; Go has
  no nullable primitives, so `database/sql` carries null-ness in a companion `Valid bool`.
  You must check `.Valid` before trusting the value, the same way you'd `.isPresent()`
  before `.get()`.

## 6. Gotchas & idioms

- **`iota`'s zero value is a usable signal.** `PhasePending` is `iota` == 0, and the
  poll-riding retry exploits that: writing `lastEmittedPhase = PhasePending` is a
  guaranteed dedupe cache-miss that forces the next emit through. Zero values are load-
  bearing in Go — design your enums so the zero is a sane default or an intentional sentinel.

- **Pass `*[]Status` to append from an inner function.** Appending to a `[]Status`
  parameter mutates a *copy* of the slice header; the caller never sees the new element.
  A pointer-to-slice is the idiom for an "append sink."

- **`sql.ErrNoRows` is a return value, not always an error.** A guarded `UPDATE ...
  RETURNING` that matches no row yields `sql.ErrNoRows`, which the sink reads as an
  idempotent no-op (`errors.Is(err, sql.ErrNoRows) { return nil }`). The same sentinel that
  means "not found" on a `SELECT` means "precondition already satisfied" on a guarded write.

- **A Postgres unique violation poisons the whole transaction.** After a constraint fires,
  every later statement in the tx fails with `in_failed_sql_transaction` — so you cannot
  "catch and continue" inside a transaction. Guard *before* the write, not after.

- **`ethclient.DialContext` is lazy for http(s).** Dialing a dead endpoint succeeds; the
  failure surfaces on the first poll. A malformed URL can still error synchronously, which
  is why the code still redacts and handles the `Dial` error — but a "connected" watcher
  isn't proof the node is reachable.

- **Explicit `int64`↔`uint64` conversions at the DB boundary.** Postgres `BIGINT` is signed;
  a block number is unsigned. Go forces the conversion, which is a feature: it makes the
  signedness crossing visible instead of a silent wrap.

## 7. Check yourself

1. `PhaseConfirmed` is non-terminal, so the watcher re-verifies every settled tx forever —
   except it doesn't, because of one mechanism. Name it, and explain why the
   `finalityDepth > depth` constructor check is a correctness guard rather than a style rule.
2. Walk through what `Resume` seeds into a `tracked` entry and *why* those specific
   `lastEmittedPhase`/`lastEmittedDepth` values prevent a double-settle on restart while
   still allowing a reorg-during-downtime to be caught.
3. The sink checks `if sett.Status == "settled" { return nil }` before posting. The ledger
   already has `UNIQUE(kind, external_ref)`. Construct the concrete failure that happens if
   you *delete* the status guard and rely on the unique constraint for a redelivered confirm.
4. The M2 nonce allocator held its mutex across the signing RPC; the M3 watcher never holds
   its mutex across a receipt lookup. Both guard one `sync.Mutex` over one piece of state.
   What is the single question that decides which discipline is correct?
5. A tx settles at block `0xAAA`, reorgs, and re-mines at block `0xBBB`. Trace the two
   `external_ref` values produced and explain what would go wrong if the key were
   `settle:<paymentID>` with no block hash.
6. The poll-riding retry sets `t.phase = PhaseMined` but guards on `ok && t.phase ==
   PhaseConfirmed`. Construct the interleaving that guard defends against, and say what
   would break without it.

<details>
<summary>Answers</summary>

1. Finality-depth eviction: once an anchor is buried `>= finalityDepth` deep on the
   canonical path, the watcher emits terminal `PhaseFinalized` and `delete`s the entry,
   bounding the map. `finalityDepth > depth` is a correctness guard because a finality that
   fired at or before confirmation depth would evict a tx while a reorg shallower than
   finality could still legitimately reverse it — reintroducing the un-handled-reorg bug.
   The constructor fails loudly rather than build such a watcher.

2. `Resume` seeds `phase = PhaseConfirmed`, the persisted `blockHash`/`blockNumber` anchor,
   and `lastEmittedPhase = PhaseConfirmed`, `lastEmittedDepth = w.depth`. Those emit-dedupe
   values mean the next canonical poll re-confirms the tx but `emit` suppresses a duplicate
   `Confirmed` (same phase, not deeper) — so the already-landed settle is not replayed. If
   the anchor has diverged during downtime, the same re-verification finds a `NotFound`
   receipt or a mismatched header and surfaces `Reorged`, posting the owed reversal.

3. Without the guard, a redelivered confirm on an already-settled row calls `PostWithin`,
   which validates funds *before* the unique index. The destination's provisional credit was
   already released by the first settle, so the second debit trips `ErrInsufficientFunds`
   (or debits money that isn't there). Worse, if it reached the unique index, that violation
   aborts the whole transaction, so the following `MarkSettlementSettled` fails with
   `in_failed_sql_transaction`. The status guard short-circuits before any of this.

4. Does the network I/O mutate the state the mutex protects? For the nonce allocator, the
   sign+broadcast *is* the critical section — the nonce isn't safe to reuse until the prior
   one is broadcast — so the lock must span it. For the watcher, a receipt lookup touches
   nothing in the `tracked` map, so holding the lock across it would only stall concurrent
   `Track` calls for no benefit; snapshot-and-release is correct.

5. Original settle at `0xAAA` → `external_ref = settle:<pid>:0xAAA`. Re-settle at `0xBBB` →
   `external_ref = settle:<pid>:0xBBB`. Distinct keys, so `UNIQUE(kind, external_ref)` lets
   both through and the re-mine credits the destination again. With `settle:<pid>` alone,
   the re-settle would collide with the original entry and be dropped as a duplicate — the
   destination stays un-credited after a legitimate re-mine.

6. Between the failed sink call and the retry handler acquiring the lock, a concurrent poll
   pass could advance the entry — e.g. it re-verifies the anchor, finds it diverged, and
   sets `phase = PhaseReorged` (resetting to pending semantics). Without the `t.phase ==
   PhaseConfirmed` guard, the retry handler would then clobber that `Reorged`/pending state
   back to `Mined`, discarding the reorg the poll just detected. The guard ensures the
   rollback only applies to an entry still sitting where the failed emit left it.

</details>

## 8. Further reading

- [Go blog — Errors are values](https://go.dev/blog/errors-are-values) — the mindset behind
  treating `sql.ErrNoRows` as a control-flow signal rather than an exceptional failure.
- [`golang.org/x/sync/errgroup` package docs](https://pkg.go.dev/golang.org/x/sync/errgroup) —
  `WithContext`, the cancel-all-on-first-error semantics, and the `g.Go`/`g.Wait` contract.
- [`database/sql` — handling NULL](https://pkg.go.dev/database/sql#NullString) — the
  `sql.Null*` wrapper types and why Go models nullable columns with a companion `Valid` bool.
- [Ethereum.org — Proof-of-stake finality and reorgs](https://ethereum.org/en/developers/docs/consensus-mechanisms/pos/#finality) —
  why confirmation depth is probabilistic and finality (~2 epochs / 64 blocks) is the depth
  past which a reversing reorg is treated as economically impossible.
- [go-ethereum `ethclient/simulated`](https://pkg.go.dev/github.com/ethereum/go-ethereum/ethclient/simulated) —
  the in-process backend the watcher's reorg tests drive through `poll` deterministically.
