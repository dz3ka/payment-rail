# M5 — Compliance controls: a fail-closed policy pipeline, a per-key velocity limiter that charges inside the lock, a four-eyes state machine, and a hash-chained audit log that re-derives its own integrity

> Scope: this lesson is about the four controls that stand between a validated payment
> intent and a broadcast transaction — (1) **denylist screening** behind a `Screener`
> port whose every implementation and every caller is *fail-closed*: any error, denial or
> operational, blocks the payment; (2) a **per-signing-key velocity limiter** that does
> `{lock → sum window → decide → record}` as one atomic critical section under a Postgres
> *advisory* lock, recording the spend event **inside** the same transaction that admits
> it; (3) a **four-eyes approval state machine** — `submit` parks the full intent as a
> `pending` row, a *distinct* `approve` operator claims it under `SELECT … FOR UPDATE`
> and a status-guarded `UPDATE`, then broadcasts — enforcing separation of duties the
> code cannot bypass; and (4) an **append-only, hash-chained audit log** where every row
> commits `sha256(prev_hash ‖ canonical(fields))` inside the caller's transaction, so any
> later edit, deletion, or reorder breaks the chain, and `audit verify` re-derives the
> whole chain from genesis to prove it. The recurring shape is the same `{check → act →
> commit}` critical section the M2 nonce allocator and signer spend-bucket used — now
> applied three more times, twice across a Postgres transaction. Skim
> `m2-evm-chain-adapter.md` if that pattern isn't reflexive yet.

## 1. What we built

M5 is the compliance layer of the rail: four independent controls the payment path must
clear before value moves. `internal/policy` holds the pure decision logic — a `Screener`
interface with a file-backed `Denylist`, a `VelocityLimiter` that enforces sliding-window
caps against a `VelocityStore` port, and an `ApprovalGate` that decides whether a payment
needs four-eyes and whether a given approver may clear it. `internal/audit` holds the
hash-chained log: `Append` (one row, chained, inside the caller's transaction) and
`Verify` (a *pure* function that re-derives the chain and reports the first tamper). The
composition root (`cmd/paymentrailctl`) owns everything that touches Postgres — the two
store implementations (`pgVelocityStore`, `pgApprovalStore`), the `submit`/`approve`/`audit
verify` commands, and all the `*big.Int`↔config-string and domain↔SQL mapping.

The spine is the same domain/adapter split every prior milestone used: the policy packages
are **db-free and go-ethereum-*light*** (they name `common.Address` for normalization but
know nothing of RPC, `*sql.DB`, or proto), and the stores that back them live alone in the
composition root behind interfaces the policy packages own. That seam is what lets the
whole control stack be unit-tested with in-memory fakes and separately integration-tested
against a real Postgres — the `*_integration_test.go` files stand up real transactions and
prove the atomicity claims under concurrency.

One environmental fact drives more of this milestone's shape than any design preference:
**`paymentrailctl` is a one-shot process.** It starts, validates, screens, signs,
broadcasts one payment, and exits. That rules out the obvious first instinct for both the
velocity cap and four-eyes — in-process state behind a mutex — because an in-memory counter
is born at zero on every invocation and would enforce nothing (you could bypass any cap by
running the CLI in a loop), and because a process that exits cannot wait for a second human
to walk over. Compare the signer service, which *does* hold a `spendBucket` behind a mutex:
that works there precisely because the signer is a **daemon**, so the state outlives the
thing it counts. In-process enforcement is viable exactly when the enforcement seam is a
process that outlives what it's counting; when it isn't, the state has to go into a
datastore. Every "why Postgres?" answer below is a consequence of that one sentence.

The two things to study hardest are the **velocity limiter's `decide` callback** and the
**audit chain**. The limiter reuses the M2 "caller-supplied critical section" idiom, but
now the lock is a *database* advisory lock and the critical section spans a SQL
`SELECT … INSERT`, so the interesting question moves from "goroutine safety" to
"transaction atomicity and what a rolled-back transaction leaves behind." The audit chain
is the milestone's centerpiece: it is a tiny blockchain-of-one — genesis anchor, canonical
length-prefixed preimage, `sha256(prev ‖ fields)` links, app-assigned gap-free sequence —
and `Verify` is the proof-of-correctness that re-hashes every row and cascades any single
edit into a detectable mismatch.

## 2. The design decision

### Decision A: every control is fail-closed, and the *ordering* is load-bearing

**The problem.** A compliance control that fails *open* — that lets a payment through when
it can't reach its backing store, or when its manifest is missing — is worse than no
control, because it gives false assurance. And several controls run in sequence; the order
they run in has security meaning.

**The chosen approach — fail closed at every layer, screen before velocity before
four-eyes, and never dial the network for a payment a cheaper check already rejects.**
`policy.Load` rejects a missing file, malformed JSON, or a bad address rather than starting
allow-all; callers treat *any* `Screen` error — a denial (`errors.Is ErrDenied`) **or** an
operational failure of the backing store — as a block. The submit path runs the controls
in a deliberate order (`submit.go`, `broadcast.go`):

```
1. denylist screen   (in-memory, no I/O)      ── submit.go, before any dial
2. four-eyes gate     (in-memory)              ── submit.go: park & return, or fall through
3. velocity charge    (one Postgres txn)       ── broadcast.go, before any signer/chain dial
4. signer + chain dial, VerifyChainID, Submit  ── the actual broadcast
```

The cheapest, most-certain rejections run first and *short-circuit before any network
dial*. A denied destination never causes a signer dial, a `VerifyChainID`, or a broadcast;
a velocity breach happens *before* the signer/chain dial too. This is defense-in-depth
ordered by cost and certainty: an in-memory set lookup gates a `*sql.DB` open, which gates
a gRPC dial, which gates an irreversible on-chain send.

**Alternative 1: start allow-all when the manifest is missing.** This is the newcomer
default (`if err != nil { return emptyList }`) and it's the exact fail-open trap. A typo in
the manifest path, a bad deploy that drops the file — and every sanctioned address sails
through with no signal. `Load` instead returns the error; the one deliberate open door is
`Load("")`, which is an *explicit operator choice* to disable screening, not an accident.

**Alternative 2: screen after signing, or velocity-check after dialing.** Cheaper to code
(one straight-line path) but it burns a nonce and a signer round-trip on a payment that was
never going to be allowed, and — worse — it means a denied payment has already been *signed*
(a live, broadcastable artifact) sitting in memory. Screening before any signing means a
denied payment never becomes a signed transaction at all.

**The cost, stated honestly.** Each control that needs Postgres opens its own short-lived
pool (`sql.Open` / `defer Close`) rather than sharing one long-lived handle. For a one-shot
CLI that's the right call — the process lifetime *is* the request — but in a long-running
service you'd inject a shared `*sql.DB`. The code is explicit that this mirrors "the path
that needs Postgres opens and closes its own handle," so the shortcut is named, not hidden.

**Fail-closed is enforced by the *return shape*, not by discipline.** `Load` has no branch
that returns a usable screener *and* an error — every failure path is `nil, err`, so a
caller physically cannot proceed with a half-loaded denylist, and `TestLoadFailClosed`
asserts `d == nil` to pin exactly that. Note the two "no denylist available" situations are
deliberately *opposite* outcomes: `Load("")` is an explicit operator choice to disable
screening and must not error, while `Load("/missing.json")` means screening was *requested*
and could not be honored, which must fail closed. Same surface symptom, opposite intent.

**And one more thing has to be fail-closed that isn't about errors at all: the *matching*.**
Ethereum addresses carry an optional EIP-55 mixed-case checksum, so
`0xAbC…01` and `0xabc…01` are the *same* address spelled two ways. If the manifest stored
raw strings and `Screen` compared raw strings, a checksummed manifest entry would silently
fail to match a lower-cased `--to` — a screening **bypass**, with no error anywhere. Both
sides therefore go through `common.HexToAddress` to a canonical 20-byte `common.Address`.
The rule generalizes past Ethereum: **a matching control must normalize the stored value and
the queried value through the identical function**, or an attacker simply picks the encoding
you forgot to canonicalize.

That normalization has a sharp edge of its own, and closing it was a real fix in this
milestone. **`common.HexToAddress` never fails.** Given garbage it decodes what hex it can
and left-pads or truncates to the right-most 20 bytes, handing back a well-formed but
*wrong* address. So an un-validated `--to` would be silently coerced into some address that
is (almost certainly) not on the denylist, and would sail through the screen. The guard is
`if !common.IsHexAddress(*toFlag)` **before** normalization: **validate the format before
you normalize-and-match**, because a lenient normalizer turns malformed input into a
confident wrong answer.

### Decision B: the velocity limiter charges *inside* the lock, on attempt, not on success

**The problem.** A velocity cap ("no more than N transfers or M total wei per key per
rolling window") is a check-then-act: read the in-window usage, decide, and if allowed,
record this spend so the *next* check sees it. If two payments for the same key run
concurrently and both read usage *before* either records, both can pass a cap they'd
jointly breach — a classic TOCTOU race, now at a money boundary.

**The chosen approach — one Postgres advisory lock per key, held across
`SELECT-sum → decide → INSERT`, all in one transaction, recording the event on *attempt*.**
`pgVelocityStore.Charge` (composition root) owns the transaction and sequences it; the
*decision* lives in a `decide` closure the policy limiter supplies:

```go
// policy.VelocityLimiter.Charge builds the decision; the store runs it under the lock.
decide := func(usage Usage) error {
    if l.caps.MaxCount > 0 && usage.Count+1 > l.caps.MaxCount {
        return fmt.Errorf("velocity count %d+1 > cap %d: %w", usage.Count, l.caps.MaxCount, ErrVelocityExceeded)
    }
    if l.caps.MaxAmount != nil {
        total := new(big.Int).Add(usage.Sum, amount) // fresh alloc; never mutate usage.Sum
        if total.Cmp(l.caps.MaxAmount) > 0 {
            return fmt.Errorf("velocity amount over cap: %w", ErrVelocityExceeded)
        }
    }
    return nil
}
return l.store.Charge(ctx, keyID, amount, l.caps.Window, now, decide)
```

```go
// pgVelocityStore.Charge — the atomic critical section, mirroring the M2 charge shape.
if err := q.AcquireVelocityLock(ctx, advisoryKey(keyID)); err != nil { ... } // pg_advisory_xact_lock
since := now.Add(-window)
row, err := q.SumVelocityWindow(ctx, db.SumVelocityWindowParams{KeyID: keyID, Since: since})
usage := policy.Usage{Count: uint64(row.Count), Sum: big.NewInt(row.Sum)}
if err := decide(usage); err != nil {
    return err // unchanged: ErrVelocityExceeded must survive errors.Is
}
if err := q.InsertVelocityEvent(ctx, ...); err != nil { ... }
return tx.Commit()
```

Two decisions inside deserve names. First, the lock is **`pg_advisory_xact_lock`**, a
transaction-scoped advisory lock keyed on `fnv64a(keyID)` — it releases automatically at
commit or rollback (no explicit unlock, no leak on an error path), and it serializes only
same-key charges (different keys hash to different lock values and never contend). This is
the database analogue of M2's per-sender `sync.Mutex`: the same `{check → act → commit}`
shape, the same "keyed so unrelated work doesn't serialize," lifted across a process
boundary into Postgres. Second, **record-on-attempt**: the event is inserted in the same
transaction that admits the payment, *before* the downstream broadcast can fail. So a
payment that passes velocity but fails to broadcast still consumes window budget. That
over-counts, but only in the safe direction — the cap can never be *under*-counted into a
breach. (The repo records this as ADR-0019.)

**Alternative 1: `SELECT` and `INSERT` with no lock (or two separate transactions).** The
TOCTOU race above: two concurrent charges both read stale usage and both pass. A unique
constraint can't save you here because the events are legitimately distinct rows; only
serializing the read-decide-write closes it.

**Alternative 2: record on *success* (after broadcast).** Feels fairer — you only "spend"
budget for payments that actually send. But it reopens the race window across the entire
signer+chain round-trip, and it means a burst of concurrent submits can all pass the check
and *then* all broadcast, blowing the cap. Record-on-attempt trades a little fairness
(a failed broadcast still costs budget, recoverable by waiting out the window) for a cap
that actually holds under concurrency.

**Alternative 3: `SERIALIZABLE` isolation instead of an advisory lock.** Correct, but it
turns a lost race into a *serialization failure* the caller must catch and retry, and it
serializes far more than this one check. An advisory lock keyed on the signing key is the
surgical tool: it serializes exactly the charges that must not interleave and nothing else.

**Alternative 4: Redis.** The PRD even pre-authorizes it — sorted sets scored by timestamp
are a textbook sliding window. Rejected on operational surface: it means standing up,
securing, and operating a new piece of infrastructure, and the enforcement seam here is a
one-shot CLI invoked by a human or a batch job, not a high-QPS service. The marginal latency
win does not pay for the extra thing to run. Postgres is already present, already durable,
already transactional, and already has advisory locks.

**Why there is no `SELECT … FOR UPDATE` here.** The reflexive way to serialize a
check-then-act in SQL is to row-lock the row you are about to modify. That is unavailable in
this design, and the reason is structural: the window state is an **append-only event log**,
so the thing being protected is "the set of rows in the window" — which is not a lockable
object. Some of those rows do not exist yet, and locking the ones that do would not stop a
*new* insert from landing. When the protected thing is not a row, you need an
application-defined named mutex, and `pg_advisory_xact_lock` is exactly that. (Decision C
faces the opposite situation and therefore makes the opposite choice — the contrast is the
lesson.)

That append-only shape pays for itself twice more: there is no in-place mutation to race on,
and old rows age out of the window automatically because the "window" is a
`WHERE occurred_at >= now - window` filter rather than a decrementing counter. A pruning job
can trim history later, and correctness never depends on it having run.

**The `decide` callback is not a style choice — it is the only shape that satisfies both
constraints.** Two rules are in tension. (1) `internal/policy` must not import `internal/db`:
it is a pure decision package, unit-testable with a fake, and dragging SQL into its
dependency graph would end that. (2) the `SELECT`-sum and the `INSERT` must be **one
transaction**, or the TOCTOU race reopens. The caps live in policy; the transaction lives in
the composition root. So control is inverted: the store *lends its transaction* to the
decider, calling `decide(usage)` while the tx is still open and the lock still held, and the
closure receives a plain `Usage` value — never a `*sql.Tx`. The naive alternative is a
two-method store, `WindowUsage(keyID, since)` then `InsertEvent(keyID, amount)`, with policy
calling one, deciding, then calling the other. It reads beautifully and **forfeits
atomicity**: those are now two transactions with a gap, and a concurrent submission slips
its own sum-and-insert into the gap. The callback is the *only* shape that keeps the
decision in the pure package **and** inside a single transaction. This is the same "lend me
your transaction" seam as `ledger.PostWithin`/`ExecTx` — Go's flavour of the loan pattern.

**Two guards keep money intact across the `*big.Int` → `BIGINT` boundary.** Amounts are
arbitrary-precision in the application but `int64` in the schema. `!amount.IsInt64()` fails
closed *before* any write, because storing an oversized value would truncate and then
under-count every future check for that key. And the query casts `SUM(amount)::bigint`, so
if the *running window total* overflows, Postgres raises `bigint out of range` rather than
wrapping to a negative number — a wrapped sum would look *smaller* than reality and admit
payments it should block. The cast converts a silent under-count into a loud failure.

**Disabled-when-unset preserves the legacy no-DB contract.** `submit` historically needed no
database at all; velocity is the first thing on that path to want Postgres, so it is strictly
opt-in. `VelocityCaps.Enabled()` is true only when `Window > 0` **and** at least one cap is
set (a positive window with no caps would enforce nothing, so there is no reason to pay for a
round-trip), and the composition root only calls `sql.Open` when `caps.Enabled()`. Note the
matching fail-fast on the other side: a *negative* `WINDOW_SECONDS` is rejected by
`config.Load` as a misconfiguration rather than coerced into "disabled" — otherwise a
fat-fingered `-3600` would silently turn the control off while looking configured.

### Decision C: four-eyes is a state machine the code cannot short-circuit

**The problem.** Separation of duties says a single operator must not both *propose* and
*release* a high-value payment. Enforcing that in application code is easy to get subtly
wrong: check the approver, then update the row — and two approvers race, or the same person
approves their own proposal through a second CLI invocation.

**The chosen approach — a three-state machine (`pending → approved → broadcast`) whose
every transition is a status-guarded SQL `UPDATE` under a row lock, with the distinctness
check run *inside* the locked transaction.** At/above the threshold, `submit` refuses to
broadcast; it parks the frozen intent as a `pending` row and returns
(`Propose`). A *distinct* operator runs `approve`, which calls `Claim`:

```go
row, err := q.GetApprovalForUpdate(ctx, id) // SELECT … FOR UPDATE: concurrent approvers block here
...
if row.Status != "pending" { return ..., ErrAlreadyApproved }
if err := decide(pa); err != nil { return ..., err } // four-eyes authz, INSIDE the lock
rows, err := q.MarkApprovalApproved(ctx, ...)         // UPDATE … WHERE id=$1 AND status='pending'
if rows == 0 { return ..., ErrAlreadyApproved }       // guarded update matched nothing → lost race
```

The `decide` closure is `gate.Authorize(pa.Proposer, approver)`, which enforces two things
the gate owns: the approver must be an allowlisted identity, and it must **differ from the
proposer** (`ErrSelfApproval`). Running it *inside* the `FOR UPDATE` transaction means an
unauthorized approver never flips the row — the authorization and the state transition are
one atomic unit. Belt-and-suspenders: even under the row lock, the `UPDATE` is *also*
guarded `WHERE status = 'pending'`, and a zero-rows-affected is treated as a lost race
(`ErrAlreadyApproved`). Two independent mechanisms (the lock and the guarded predicate)
both have to fail before a double-claim slips through.

There's a subtle separation-of-duties completeness check at *propose* time too: `submit`
requires the proposer to itself be an allowlisted approver (`gate.KnownApprover`). Why?
Because a proposal that no valid *pair* of eyes could ever clear is a trap — it parks value
that can never be released. Requiring the proposer to be in the allowlist guarantees at
least one *other* allowlisted approver exists to clear it (given a coherent allowlist), so
a parked payment is always clearable by someone.

**The park-then-broadcast gap is deliberate and irreducible.** The DB commit that marks a
row `approved` must land *before* the irreversible on-chain send — you cannot broadcast
inside the approval transaction, because a broadcast can't be rolled back. So `approve`
does two steps: `Claim` (commit `approved`), then `broadcastIntent`. If the broadcast then
fails, the design distinguishes two cases by the returned tx hash:

- **empty hash** → failure happened *before* `adapter.Submit` (config, velocity, dial):
  nothing was sent, so `Reopen` reverts the row to `pending` for a retry — guarded
  `WHERE status='approved' AND tx_hash IS NULL`, so a *sent* payment can never be reopened.
- **non-empty hash** → the tx *was* broadcast; only the bookkeeping link failed. Reopening
  would risk a **double-send**, so the code refuses to reopen and tells the operator to
  reconcile by hand.

**Alternative: a boolean `approved` column, no lock, check-then-update in app code.** Two
approvers race between the `SELECT` and the `UPDATE` and both broadcast the same payment.
The `FOR UPDATE` lock plus the guarded-predicate `UPDATE` (a compare-and-swap in SQL) is
what makes the transition exactly-once even under concurrent approvers. The integration
test makes it observable: 20 goroutines race to claim one approval, each with a distinct
valid approver, and **exactly one wins while the other 19 get `ErrAlreadyApproved`**.

**Alternative: reuse velocity's advisory lock.** Tempting for symmetry, and *worse* here for
two reasons. First, an advisory lock is a **proxy** for the data — a hashed key standing in
for something unlockable — whereas an approval *is* one concrete row with a primary key, so
`FOR UPDATE` locks the object at risk directly, with no hash and no collision caveat to
carry. Second, advisory locks live in **one global int64 namespace** shared across the whole
database, and velocity already occupies it; adding a second unrelated consumer risks two
features hashing to the same value and serializing against each other for no reason. Row
locks have no shared namespace at all. The rule the two decisions teach together: **lock the
row when there is one; reach for an advisory lock only when the thing you are protecting is
not a lockable object.**

**Ordering precedence inside `Authorize` is a deliberate, tested choice.** The unknown-approver
check runs *before* the self-approval check, so `mallory` approving her own proposal while
not being on the allowlist gets `ErrUnknownApprover`, not `ErrSelfApproval`. "You are not a
recognized approver at all" is the more fundamental disqualification; reporting
`ErrSelfApproval` first would imply that finding a different payment to approve would fix
it, when in fact she would still be rejected. Report the more basic rejection first — and
keep the two as distinct sentinels so the CLI can message them differently.

**A note on what the schema does *not* have.** There is no `'broadcast'` status. The table
carries `CHECK (status IN ('pending','approved'))` and "broadcast" is a *derived* state:
`status = 'approved' AND tx_hash IS NOT NULL`. That is why both guards key on
`tx_hash IS NULL` rather than on a status transition, and why the same three-column predicate
protects against two different disasters: on `MarkApprovalBroadcast` it makes a **second
broadcast match no row**, and on `ReopenApproval` it makes **resurrecting a sent payment
impossible**. Two columns, three meaningful states, and the guards read them directly. Change
the representation of "broadcast" and you must revisit both guards together.

**And after a successful send, recording the hash is deliberately best-effort.** A
`MarkApprovalBroadcast` failure logs loudly and warns on stderr but does **not** fail the
command, because the money already moved: returning an error there would report an
irreversible success as a failure and tempt an operator into retrying a payment that already
went out. The principle is worth naming — **never let a bookkeeping failure masquerade as the
primary operation failing.** The same reasoning makes `Reopen` fault-tolerant in the other
direction: if reopen itself fails, the code does not pretend it recovered; it prints the
stronger "reconcile manually" message, surfaces why reopen failed, and returns the original
broadcast error. It degrades from "auto-recovered" to "human, look here" — never to
"silently lost".

### Decision D: the audit log is a hash chain, and `Verify` re-derives it from genesis

**The problem.** An audit log an operator can quietly edit — change an amount, delete the
row that records a bad approval, reorder events — is not evidence. You want *tamper-evidence*:
not that alteration is impossible (a DBA with write access can change any byte), but that
any alteration is *detectable* after the fact.

**First, though: where the row is written.** The audit row is appended **inside the same
transaction as the effect it records** — co-located with the existing `outbox.Emit` call in
`payments.Create`/`Cancel` and `settlement.settle`/`reverse`/`finalize`, and inside the
claim/propose transaction in the four-eyes store. The tempting alternative is to make audit
just another outbox consumer: emit the event, let a downstream worker write the audit row.
Rejected, because an async consumer is at-least-once and *eventually* consistent, which
means a real window in which a committed state change exists with **no audit row yet** — and
if the consumer is down or the message is lost, that window is unbounded. "Is every effect
recorded?" is the auditor's first question, and "eventually, probably" is the wrong answer.
So this is the transactional-outbox idea with the consumer collapsed *into* the producer's
transaction: outbox trades a delivery guarantee for decoupling; here we want the *record*
guarantee, so we do not decouple. Be precise about what that buys — not that the log records
"the truth," only that the log entry and the DB effect commit or roll back together. The
honest cost: every audited write now pays a `sha256`, one extra INSERT, and serialization on
a single global lock, all on the critical path. Fine at this throughput; a high-fanout hot
path would need a sharded chain or an accepted async gap for low-value events.

**Second: one global chain, not one per aggregate.** A hash chain is inherently serial —
row N+1 needs row N's hash — so this is a fork in the road. Per-entity chains parallelize
writes (two payments never contend) but give you N little chains, each verifiable only in
isolation, with nothing ordering events *across* entities and "verify everything" becoming
"walk N chains." The global chain gives one authoritative timeline and one `audit verify`
walk, at the cost of a serialization point on every audited write. For an audit log, one
linear history beats write throughput.

**The chosen approach — chain every row to its predecessor by hash, so a single edit
cascades a mismatch to the head, and ship a `Verify` that re-derives the whole chain.**
Each row stores `entry_hash = sha256(prev_hash ‖ canonical(fields))`, where `prev_hash` is
the previous row's `entry_hash` (genesis = 32 zero bytes). Because each row's hash feeds
the next row's preimage, editing row *k* changes row *k*'s `entry_hash`, which no longer
matches row *k+1*'s stored `prev_hash`, which breaks the link — and re-stitching *that*
changes *k+1*'s hash, cascading all the way to the head. `Append` computes this inside the
caller's transaction; `Verify` (`audit verify` CLI) reads the whole chain seq-ascending and
recomputes every hash, returning the first violation as a typed `*TamperError` with a
`Kind`:

```go
// Verify's walk, per row after the genesis check.
if row.Seq != prevSeq+1 { return ..., &TamperError{Seq: row.Seq, Kind: KindGap} }         // deleted/reordered
if !bytes.Equal(row.PrevHash, prevEntryHash) { ..., KindBrokenLink }                       // re-stitched links
pre := canonical(row.Seq, row.Actor, ..., row.OccurredAt.UTC().UnixMicro(), row.Payload)
want := entryHash(row.PrevHash, pre)
if !bytes.Equal(row.EntryHash, want) { ..., KindHashMismatch }                             // a field was edited
```

Four failure kinds, each catching a distinct attack: `KindGap` (a row deleted or reordered
— seq is non-contiguous), `KindBrokenLink` (a `prev_hash` doesn't match the predecessor's
`entry_hash`, or row 1 isn't genesis — links re-stitched), `KindHashMismatch` (a stored
`entry_hash` doesn't match a fresh recompute of that row's own fields — a column edited in
place), and `KindHeadMismatch` (the walk is internally consistent but the head doesn't
match a caller-supplied anchor — see below).

**Two decisions make this actually sound.** First, **`seq` is app-assigned under the chain
advisory lock, not a `SERIAL`/identity column.** A `SERIAL` burns its value on a rolled-back
transaction, leaving a permanent gap — which `Verify` would read as a *deleted row* (a
false tamper alarm). App-assigning `seq = head.seq + 1` under `pg_advisory_xact_lock` on a
single global key makes the sequence gap-free by construction: a rollback assigns nothing.
This is the *opposite* choice from velocity's per-key lock — the audit chain has exactly
*one* head, so *all* writers must serialize on *one* well-known constant:

```go
const chainAdvisoryKey int64 = 8516588576188130913 // fnv64a("payment-rail.audit.chain.v1")
```

pinned as a literal so it is greppable and cannot drift. And note the *other* thing that
lock prevents, which is subtler than the gap-free `seq`. `Append` does read-then-write: read
the head, compute `seq = head.seq + 1` and `prev_hash = head.entry_hash`, then insert. If two
transactions both read head N before either inserts, they both chain off the *same*
`prev_hash` — the chain **forks**. The primary key on `seq` would turn that into a
lost-update error for one writer (fail-closed, but only after wasted work), and it would do
nothing at all if `seq` were independent of the chain. Holding the lock from *before* the
head-read until the transaction ends is what makes head-read-plus-insert mutually exclusive.
Gap-free `seq`, rollback atomicity, and the advisory lock are not three features — they are
one mechanism, and dropping any one breaks the contiguity check.

(Use the **transaction-scoped** `pg_advisory_xact_lock`, not the session-scoped
`pg_advisory_lock`: the latter must be explicitly unlocked and *survives a rollback*, which
is a leak waiting to happen on an error path.)

Second, the **canonical preimage is length-prefixed**:

```go
writeUint64(uint64(seq)); writeUint64(uint64(occurredMicros))
writeField([]byte(actor)); writeField([]byte(action)); ... writeField(payload)
// writeField = writeUint64(len(b)) then the bytes
```

Without the length prefix, `actor="ab", action="c"` and `actor="a", action="bc"` serialize
to identical bytes — a trivial forgery that swaps field boundaries without changing the
hash. Prefixing every variable-length field with its length makes the frame unambiguous, so
distinct field values always produce distinct preimages. This is exactly why naïve string
concatenation is unsafe for any hash-committed structure — it is TLV framing without the
type tag, the standard way to make a concatenation injective.

**Third — and this one is easy to miss — the payload is hashed as *stored bytes*, never as a
re-marshaled value.** The naive answer to "what gets hashed?" is "marshal the row to JSON and
`sha256` that," and it is a trap: `encoding/json` gives *some* stability (struct fields in
declaration order, map keys sorted) but the moment a field is added, a shape is driven by
`any`, a custom `MarshalJSON` appears, or escaping shifts across Go versions, the bytes
change while the logical value does not — and every historical `entry_hash` becomes
permanently unverifiable. So the payload is marshaled **once, at append time**, and those
exact bytes are stored verbatim in a `BYTEA` column — deliberately *not* `jsonb`, which
would re-normalize on read. `Verify` rehashes `row.Payload` as it came back from the
database and never re-marshals the Go value. JSON is a convenient serialization for storage
here; it is never the thing whose canonicality the chain depends on. The chain's canonical
form is `canonical()`'s byte framing, full stop.

**The anchor closes the one hole chaining alone can't — and it is worth being honest about
how big that hole is.** Chaining gives tamper-*evidence against partial tampering*. It does
not give tamper-*prevention*, and it does not detect an attacker willing to redo all forward
work. Someone with DB write access who edits row K can recompute row K's hash, then row
K+1's `prev_hash` and hash, forward to the head, and produce a *perfectly valid chain of a
falsified history*. Tail-truncation is the cheap version of the same attack: lop off the last
K rows and rows 1..N−K still verify perfectly among themselves, because there is nothing
*after* the new head to mismatch. So `Verify` accepts an optional `WithExpectedHead(hash)`:
record the head hash out-of-band after a good run, feed it back next time, and both the
truncation and the full rewrite fail `KindHeadMismatch`. The CLI leans into this by printing
the head hash on every successful verify, so a clean run always produces the next run's
anchor:

```
AUDIT CHAIN VALID: 128 entries; head seq=128 hash=9f86d0...
```

The anchor is only ever as good as the out-of-band recording discipline — which is the
honest limit of the whole control. Note an expected-*count* would be redundant: `seq` is
inside every preimage, so the head hash already pins the chain's prefix length.

**Alternative 1: a plain append-only table with a DB trigger blocking `UPDATE`/`DELETE`.**
The migration *does* add that trigger (`audit_log_immutable`) — but as *defense-in-depth*,
not the guarantee. A trigger is enforced by the same database an attacker with sufficient
privilege controls; they can drop the trigger, or restore from a doctored backup. The hash
chain is evidence that survives *outside* the database's access control: anyone holding a
past head hash can prove the current chain is or isn't a faithful extension of it.

**Alternative 2: sign each row with a private key (a Merkle/notary scheme).** Stronger —
it authenticates the *writer*, not just detects edits — but it needs key management,
rotation, and a signing oracle in the hot path. For an internal operator audit log where
the threat is "someone quietly edits history," a hash chain plus an externally-recorded
anchor is the right weight. Row signing is the production upgrade if you need to prove *who*
wrote each entry, not just *that* it's unaltered.

### Decision E: which seams got a port, which didn't, and the rule that decides

M5 adds four controls in a row, which makes it the best place in the codebase to see that
"add an interface at the seam" is a *judgment*, not a reflex. Four decisions went four
different ways, and the rule that produced them is worth more than any of them individually.

**Kept: `policy.Screener`, a one-method interface with exactly one implementation.**

```go
type Screener interface {
	Screen(ctx context.Context, address string) error
}
```

A razor pass argued to cut it — YAGNI, one concrete type, call it directly — and was
overridden. A one-method interface with a single impl earns its keep when **there is a
documented requirement for a second implementation at the same seam, and that second impl
needs machinery the first does not.** The file-backed denylist is explicitly the "mock
included"; the intended second impl is a real sanctions-screening API over the network. That
is why `ctx context.Context` stays in the signature even though an in-memory map lookup
ignores it, and why the return is `error` even though a map lookup cannot fail
operationally: **the interface is shaped for the impl you know is coming**, and you cannot
retrofit a `ctx` into an interface method without breaking every implementer. Contrast a
speculative interface added "in case there's ever a second impl" — that one does not earn
its keep. The distinction is a *documented, imminent* second consumer, not a conceivable one.

**Cut: the `Request`/`Decision` value objects around that same call.** The first draft
wrapped it up:

```go
// REJECTED
type Request struct{ To string }
type Decision struct{ Allowed bool; Reason string }
func (s Screener) Screen(ctx context.Context, r Request) (Decision, error)
```

`Request` was a one-field struct around a `string`, and `Decision` re-encoded a
yes/no-plus-reason that Go already has an idiomatic channel for: the error. Allow is `nil`;
deny is `fmt.Errorf("address on denylist (%s): %w", reason, ErrDenied)`; an operational
failure is some other error. The trade-off deserves naming honestly, because collapsing an
*expected negative business outcome* into the error channel is not always right. It is right
**here** because every caller's response to a denial and to a failure is identical — abort —
so a `Decision{Allowed:false}` would just be an error the caller re-`if`s into an error. The
rule: **model an outcome as an error when every caller's response to it is to stop; model it
as a value when a caller might carry on** (route to manual review, record a soft decline,
keep processing a batch).

**Kept: `policy.VelocityStore`, a named port.** Because `internal/policy`'s `VelocityLimiter`
is itself the **consumer** — it *calls* `store.Charge(...)` — the port lives next to a real
domain-side caller and lets a unit test fake that caller's dependency.

**Cut: `policy.ApprovalStore`, and this is the sharpest lesson in the milestone.** The
obvious move for four-eyes was to mirror velocity exactly. We deliberately didn't, because
**nothing in `internal/policy` ever calls the approval store.** The `cmd` layer orchestrates
(`runSubmit`→`Propose`, `runApprove`→`Claim`); policy only supplies db-free decision types
(`Intent`, `PendingApproval`, the stateless `ApprovalGate`). A `policy.ApprovalStore`
interface would have exactly one implementation and **zero domain-side consumers** — an
abstraction with no caller to abstract *for*, added purely to look symmetric with last
week's slice. So the **mechanism** was kept (the `decide` callback still runs policy's
verdict inside the store's row lock, still receiving a plain `PendingApproval` and never a
`*sql.Tx`) and the **ceremony** was dropped (`Claim` takes a bare `func` parameter). The
rule: **mirror a prior pattern only where the same forces apply — port the mechanism, not
the boilerplate. Symmetry with prior code is not a reason to add an abstraction.**

**And one deliberate non-narrowing: `audit.Append` takes the fat generated `db.Querier`.**

```go
func Append(ctx context.Context, q db.Querier, e Entry) error
```

Not a hand-rolled three-method port. The repo's other seams (`StatusSink`, `Screener`,
`Producer`) are narrow *because the point was to keep a domain package from importing
`internal/db` at all*. `internal/audit` already imports `internal/db` for the param and row
types, so a narrower interface would buy no decoupling and cost a layer — the narrowing
reflex is only worth paying for when there is an import to actually avoid.

## 3. Language deep-dive

### 3a. A nil pointer receiver as a valid "disabled" object

`VelocityLimiter` and `ApprovalGate` both make a nil pointer a *legal, meaningful* value —
the disabled control — so callers never branch on nil before calling:

```go
func (l *VelocityLimiter) Charge(ctx context.Context, keyID string, amount *big.Int) error {
    if l == nil || !l.caps.Enabled() { return nil } // disabled: no-op, no store call, no dial
    ...
}

func (g *ApprovalGate) enabled() bool {
    return g != nil && g.threshold != nil && g.threshold.Sign() > 0
}
func (g *ApprovalGate) Required(amount *big.Int) bool {
    if !g.enabled() || amount == nil { return false }
    return amount.Cmp(g.threshold) >= 0
}
```

This surprises engineers coming from Java/C#, where calling a method on a `null` reference
is an instant NPE. In Go, a method with a **pointer receiver** is just a function whose
first argument is the pointer; calling it with a nil pointer is completely legal *as long
as the method body doesn't dereference the nil*. `g.enabled()` reads `g != nil` *first*, so
`g == nil` short-circuits before any field access. The payoff is that the composition root
can write `gate.Required(amount)` unconditionally — a disabled gate (nil threshold, or even
a nil `*ApprovalGate`) safely reports "not required" — instead of sprinkling `if gate != nil`
at every call site. Contrast a *value* receiver: `func (g ApprovalGate)` would copy the
struct and a nil pointer couldn't call it at all (nothing to copy). The idiom only works
with pointer receivers plus a disciplined nil check at the top of every method.

Note the subtle distinction: `Required` treats a nil `amount` as "not required" (nothing to
gate), whereas `Charge` treats a nil/non-positive `amount` as a *fail-closed error* (a
charge with no amount is a bug, and the control must block rather than silently pass). Same
"nil input" surface, opposite dispositions — because one is a query and the other is an
enforcement point.

### 3b. `errors.Is` reads *through* the `decide` callback so policy sentinels survive

The velocity and approval stores both take a caller-supplied `decide` closure and run it
inside their transaction. The critical contract is that when `decide` returns a *policy*
error, the store returns it **unchanged** — no `%w` wrapping — so the outer caller's
`errors.Is` still matches:

```go
// pgVelocityStore.Charge and pgApprovalStore.Claim both do this:
if err := decide(usage); err != nil {
    return err // unchanged: ErrVelocityExceeded / ErrUnknownApprover must survive errors.Is
}
```

```go
// broadcast.go — the outer caller matches the sentinel through the whole stack:
if err := limiter.Charge(ctx, in.KeyID, in.Amount); err != nil {
    if errors.Is(err, policy.ErrVelocityExceeded) {
        logger.Warn("payment rejected by velocity policy", "key_id", in.KeyID, "error", err)
    } else {
        logger.Error("velocity check failed; failing closed", "key_id", in.KeyID, "error", err)
    }
    return fmt.Errorf("broadcast: velocity check: %w", err)
}
```

This is the same wrapping model M2 leaned on, but with a twist: the sentinel is minted deep
inside a closure, travels *out through* the store's transaction machinery, and must arrive
at the top identity-intact so the caller can tell a *policy denial* (log at `Warn`, expected)
apart from an *operational failure* (log at `Error`, the control is degraded). If the store
had wrapped the decide error — `fmt.Errorf("velocity: %w", err)` — `errors.Is` would *still*
match (it walks the chain), so why the explicit "unchanged"? Because the store must **not**
accidentally *convert* a policy denial into something that looks operational, and returning
it verbatim is the clearest way to guarantee the identity is preserved with zero surface for
a mistake. The razor here: policy sentinels (`ErrVelocityExceeded`, `ErrUnknownApprover`,
`ErrSelfApproval`) live in `internal/policy` because they describe a *decision*; store-state
sentinels (`ErrApprovalNotFound`, `ErrAlreadyApproved`) live in the composition root because
they describe the *store's* state. Two error vocabularies, matched separately.

### 3c. The canonical preimage: `bytes.Buffer`, `binary.BigEndian`, and a scratch array

`canonical` hand-builds the deterministic hash preimage. Three Go details make it correct
and allocation-tight:

```go
func canonical(seq int64, actor, action, aggType, aggID string, occurredMicros int64, payload []byte) []byte {
    var buf bytes.Buffer
    var scratch [8]byte
    writeUint64 := func(v uint64) {
        binary.BigEndian.PutUint64(scratch[:], v)
        buf.Write(scratch[:])
    }
    writeField := func(b []byte) { writeUint64(uint64(len(b))); buf.Write(b) }
    writeUint64(uint64(seq)); writeUint64(uint64(occurredMicros))
    writeField([]byte(actor)); writeField([]byte(action)); writeField([]byte(aggType))
    writeField([]byte(aggID)); writeField(payload)
    return buf.Bytes()
}
```

Line by line: `var scratch [8]byte` is a fixed-size **array** (a value, stack-allocated,
zeroed), reused across every `writeUint64` — `scratch[:]` reslices it into the `[]byte` that
`PutUint64` and `buf.Write` want, so there's *one* 8-byte buffer for the whole preimage
rather than an allocation per integer. `binary.BigEndian.PutUint64` writes the integer in a
**fixed, endianness-explicit** layout: big-endian is chosen so the wire form is
byte-identical on any host regardless of CPU endianness — never rely on the machine's native
byte order in a hash preimage. The `uint64(seq)` conversion of a signed `int64` is a
**bit-reinterpretation**, not a range check: it's lossless (all 64 bits preserved) and
`Verify` does the exact same `uint64(...)` on the way back, so the round-trip is symmetric.
`writeField` is the length-prefix framing from Decision D — the whole reason field
boundaries are unambiguous. And `bytes.Buffer` grows its internal slice as needed; returning
`buf.Bytes()` hands back the accumulated preimage that flows straight into `sha256`.

The critical property: this function is the chain's *canonical wire form* and **must never
change without a version bump**. Every stored `entry_hash` was computed over exactly these
bytes; alter the field order, the framing, or the integer width and every historical row
suddenly "fails" verification. That's why it's a small, dependency-free, heavily-commented
function rather than, say, `json.Marshal` of a struct (whose field order and whitespace are
not guaranteed stable across Go versions or struct changes).

### 3d. Microsecond truncation: making a lossy round-trip lossless *on purpose*

```go
// Append, before hashing AND before storing:
occurred = occurred.Truncate(time.Microsecond)
...
// Verify, reconstructing the same integer from the stored row:
row.OccurredAt.UTC().UnixMicro()
```

This is a small line with a big correctness role. Go's `time.Time` carries **nanosecond**
precision; Postgres `TIMESTAMPTZ` carries **microsecond** precision. If `Append` hashed the
nanosecond value but stored the (silently microsecond-truncated) value, then `Verify` would
read back a *different* time than was hashed, recompute a different `entry_hash`, and report
a **false `KindHashMismatch`** on a perfectly honest row. Truncating to microseconds
*before* both hashing and storing makes the store→read round-trip byte-exact: the value that
goes into the hash is the same value the database can faithfully return. This is a general
lesson for any hash-committed value that passes through a lower-precision store — commit to
the *stored* representation, not the in-memory one. The `.UTC()` on the read path is the
matching discipline: normalize the zone before extracting `UnixMicro()` so a row read back in
a different session timezone still reconstructs the same instant.

There is a second, independent reason `UnixMicro()` is the right thing to hash rather than a
formatted time or the `time.Time` itself. A `time.Time` carries a wall-clock instant, a
sometimes-present monotonic reading, **and a `*time.Location`** — so two values denoting the
same instant can compare unequal and format differently (`2026-07-21T12:00:00Z` versus
`…T14:00:00+02:00`). Postgres returns `TIMESTAMPTZ` in the connection's timezone, so a
store→read round-trip can legitimately hand `Verify` a different-*looking* `time.Time` for
the same instant. `UnixMicro()` is an absolute, location-independent `int64`: both spellings
above yield the same number, and it flows through `BigEndian.PutUint64` as fixed,
reproducible bytes.

### 3e. `q := db.New(tx)` — the one binding that makes all the atomicity claims true

Both stores open with the same two lines, and the second one is doing more work than it
looks like:

```go
tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
defer func() { _ = tx.Rollback() }()
q := db.New(tx)
```

sqlc's `db.New` accepts a `DBTX` interface that *both* `*sql.DB` and `*sql.Tx` satisfy.
Passing `tx` binds **every** subsequent query — the advisory lock, the window sum, the event
insert; or the `FOR UPDATE` read and the guarded `UPDATE` — to the same transaction and
therefore the same connection and the same lock. If any one of them accidentally ran on
`s.db` (the pool), it would execute on a *different* pooled connection, outside the
transaction and outside the lock, and the serialization would silently evaporate. Nothing
would error. It would pass every test that isn't concurrent, and produce a heisenbug under
load. The single `db.New(tx)` binding is what structurally prevents mixing the two — which
is exactly why the stores never keep `s.db` in a local variable alongside `q`.

The deferred rollback is the other half. `defer` runs it on *every* return path — bad id,
row not found, a `decide` rejection, a lost race — so the locked read is discarded and, for
four-eyes, the row stays `pending` and claimable by a valid distinct approver. On the happy
path `tx.Commit()` runs first and the trailing `Rollback()` returns a benign `sql.ErrTxDone`,
which is why the error is discarded with `_ =`. Commit-then-harmless-rollback is the
canonical Go transaction shape; don't "fix" it into a conditional rollback.

### 3f. Streaming `hash.Hash` beats `sha256.Sum256(append(...))` — and not only for allocations

```go
func entryHash(prevHash, canonicalBytes []byte) []byte {
	h := sha256.New()
	h.Write(prevHash)
	h.Write(canonicalBytes)
	return h.Sum(nil)
}
```

SHA-256 is defined over a byte *stream*, so two `Write`s produce a digest identical to one
`Write` of the concatenation — the streaming form simply avoids allocating the joined slice.
The reason to prefer it is sharper than performance. The newcomer's version,
`sha256.Sum256(append(prevHash, canonicalBytes...))`, has an aliasing bug: if `prevHash` has
spare capacity, `append` writes *into its backing array*, mutating the very `prev_hash` you
meant to chain from. The two-`Write` form never touches its inputs.

Two more mechanics in the same neighborhood. `h.Sum(b)` **appends** the digest to `b` and
returns the result — it does not reset the hash, and passing a non-empty `b` would prepend
garbage; `Sum(nil)` is the "just give me the digest" form. And note `canonical` deliberately
uses a `bytes.Buffer` rather than streaming straight into the hash: the preimage's byte
layout is a documented *contract* we reason about and test, so it is worth having as one
inspectable artifact.

### 3g. `Verify` is a pure function, and functional options keep it that way

```go
func Verify(rows []db.AuditLog, opts ...VerifyOpt) (Result, error)
```

No `context.Context`, no DB handle, no I/O — a deterministic function from `[]db.AuditLog`
to `(Result, error)`. All the database access lives in the CLI, which is also the only place
the seq-ascending ordering is guaranteed (`ScanAuditChain`'s `ORDER BY seq ASC`). That split
is deliberate twice over: the interesting logic (genesis check, contiguity, link check,
rehash, anchor) is exercised by feeding hand-built row slices with no database, *and* the
ordering contract is explicit and external. `Verify` **assumes** seq-ascending input and
documents who provides it rather than sorting defensively — because a defensive sort would
mask a query that forgot its `ORDER BY`.

`opts ...VerifyOpt` is the **functional-options** pattern: `WithExpectedHead(hash)` returns a
`func(*verifyConfig)` that sets a field. It is Go's idiomatic answer to optional,
self-documenting parameters in a language with neither named nor default arguments — and
note the `hasExpectHead` bool alongside the field, which exists because "option absent" and
"option supplied with the zero value" are genuinely different and a single field cannot
express both. For one option it is arguably overkill; it earns its place by leaving room for
future anchors without breaking the signature.

On the error side, `TamperError` is a **struct** error carrying `Seq` and `Kind`, retrieved
with `errors.As` rather than by string-matching:

```go
var te *audit.TamperError
if errors.As(err, &te) {
	fmt.Fprintf(os.Stderr, "AUDIT CHAIN INVALID: tamper at seq %d: %s\n", te.Seq, te.Kind)
}
```

The `Kind` constants are the stable API tests assert on; `Error()` is for humans. Sentinel
values matched with `errors.Is` and typed structs extracted with `errors.As` are the two
halves of Go's error vocabulary, and this milestone uses both deliberately: a *decision* is a
sentinel (nothing to carry), a *tamper report* is a struct (there is data to carry).

## 4. What would break

- **Fail-open screening (the newcomer bug).** `if err != nil { return &Denylist{} }` on a
  missing manifest starts allow-all and silently passes every sanctioned address. `Load`
  returns the error instead; only the explicit `Load("")` disables screening. Callers block
  on *any* `Screen` error, denial or operational.

- **A velocity TOCTOU race.** `SELECT`-sum then `INSERT` with no lock lets two concurrent
  same-key charges both read stale usage and both pass a shared cap. The
  `pg_advisory_xact_lock(fnv64a(keyID))` held across the whole read-decide-write closes it;
  keying on the signing key means unrelated keys never serialize.

- **A double-send in four-eyes.** A boolean `approved` flag checked-then-set in app code
  lets two approvers race and both broadcast. `SELECT … FOR UPDATE` plus a guarded
  `UPDATE … WHERE status='pending'` (a SQL compare-and-swap) makes the claim exactly-once;
  a zero-rows-affected is a lost race, surfaced as `ErrAlreadyApproved`.

- **A resurrected sent payment.** Reopening an approval after its tx already broadcast would
  let a second `approve` re-send it. `Reopen` is guarded `WHERE status='approved' AND
  tx_hash IS NULL`, and `approve` only reopens when the returned hash is *empty* (nothing
  sent) — a non-empty hash always routes to reconcile-by-hand.

- **Self-approval / an un-clearable parked payment.** Without the distinctness check a
  proposer could clear their own high-value payment; without the propose-time
  `KnownApprover` check a proposal could be parked that no valid pair of eyes could release.
  `Authorize` (approver ≠ proposer, approver allowlisted) and the propose-time proposer-
  allowlist check close both.

- **A false audit-tamper alarm from a `SERIAL` gap.** A `SERIAL` seq burns values on
  rollback, and `Verify` would read the gap as a deleted row. App-assigning
  `seq = head+1` under the chain advisory lock is gap-free by construction.

- **A false audit-tamper alarm from timestamp precision.** Hashing a nanosecond time but
  storing a microsecond one makes every row fail `Verify`. Truncating to microseconds before
  both hash and store makes the round-trip exact.

- **A field-boundary forgery in the audit preimage.** Concatenating fields without length
  prefixes lets `actor+action` be repartitioned without changing the hash. `writeField`'s
  length prefix makes every frame unambiguous.

- **Tail-truncation of the audit chain.** Lopping off the last K rows leaves a
  self-consistent shorter chain. Only the externally-recorded `WithExpectedHead` anchor
  catches it (`KindHeadMismatch`).

- **A screening bypass through the encoding you forgot to canonicalize.** Comparing raw
  address strings lets a checksummed (EIP-55) manifest entry miss a lower-cased `--to` — a
  silent bypass with no error anywhere. Both sides normalize through `common.HexToAddress`.

- **Malformed input coerced into a confident wrong answer.** `common.HexToAddress` never
  fails; it left-pads or truncates garbage into a well-formed address that is almost
  certainly not on the denylist. `common.IsHexAddress` validates the format *before*
  normalization.

- **A forked audit chain.** Without the advisory lock held from *before* the head-read
  through the insert, two concurrent appends read the same head and chain off the same
  `prev_hash`. The `seq` primary key turns that into a wasteful lost-update error at best,
  and would not catch it at all if `seq` were independent of the chain.

- **A false tamper alarm from re-marshaled JSON.** Rehashing `json.Marshal(e.Data)` in
  `Verify` instead of the stored `payload` bytes makes any struct-shape or encoder change
  break every historical row. Storing the exact preimage bytes in `BYTEA` (not `jsonb`,
  which re-normalizes on read) closes it.

- **A corrupted `prev_hash` from `append` aliasing.** `sha256.Sum256(append(prev, canon...))`
  can write into `prev`'s backing array, mutating the very hash being chained from. The
  two-`Write` streaming form never touches its inputs.

- **Atomicity silently lost by reaching for the pool.** Running any one query on `s.db`
  instead of the `tx`-bound `q` puts it on a different connection, outside the transaction
  and outside the lock. Nothing errors; the race just comes back under concurrency.

- **A wrapped window sum admitting payments it should block.** An overflowing `SUM` that
  wrapped negative would look *smaller* than reality. `SUM(amount)::bigint` makes Postgres
  raise `bigint out of range` instead, failing the transaction closed.

- **A double-broadcast on a mistaken retry.** `MarkApprovalBroadcast`'s
  `AND tx_hash IS NULL` guard makes a second broadcast attempt match no row, so even a
  wrong-headed re-run of `approve` fails closed rather than sending twice.

## 5. Compared to what you know

- **The velocity advisory lock is a distributed `synchronized(key)` that lives in the
  database.** In a JVM you might reach for a `ConcurrentHashMap<String, Lock>` and lock per
  key — but that only serializes within one process. `pg_advisory_xact_lock` serializes
  across *every* process that talks to the same Postgres, and it auto-releases at
  transaction end (no `finally { lock.unlock() }` to forget). The mental model is a
  named mutex whose scope is the transaction, not the code block.

- **The four-eyes guarded `UPDATE` is a compare-and-swap.** `UPDATE … WHERE id=$1 AND
  status='pending'` returning rows-affected is exactly `AtomicReference.compareAndSet(PENDING,
  APPROVED)`: the predicate is the "expected" value, the `SET` is the "new" value, and
  zero-rows-affected is the CAS-failed branch. The `FOR UPDATE` row lock is a *second*
  guard that makes the read-modify-write blocking rather than optimistic; the two together
  are belt-and-suspenders.

- **The audit hash chain is a git commit graph / a one-branch blockchain.** Each git commit
  hashes its parent's hash plus its own content; changing any historical commit changes
  every descendant SHA. The audit chain is the same construction with a linear parent
  (`prev_hash`) and a SHA-256 over `prev ‖ canonical(fields)`. The `WithExpectedHead` anchor
  is like pinning a known-good commit hash and verifying `git log` still descends from it.
  Where the analogy breaks: git is content-addressed *storage* (the hash *is* the address);
  here the hash is a *column* the DB could in principle overwrite — which is exactly why the
  external anchor and the re-derivation exist.

- **`errors.Is` reading through the `decide` callback is checked-exception propagation done
  by value, not by type.** Java would `throw PolicyDeniedException` up the stack and `catch`
  it by class. Go passes the sentinel *value* up as a return and matches identity with
  `errors.Is`; the "unchanged return" from the store is the discipline that keeps the
  identity intact through the layers, playing the role Java's exception-type preservation
  plays automatically.

- **The `decide` callback is `TransactionTemplate.execute(callback)` / `sequelize.transaction(t
  => …)`** — you hand a lambda to the thing that owns BEGIN/COMMIT. The twist worth keeping:
  the lambda receives a plain *value* (`Usage`, `PendingApproval`), never the transaction
  handle, so the pure package literally cannot touch SQL even if it wanted to. Where the
  analogy breaks: Go closures capture *variables* (not snapshots), and there is no
  exception-driven rollback — control flow is an explicit `return err` plus `defer`.

- **The velocity window is a sliding-window rate limiter over an append-only log**, not a
  token bucket. If you've built rate limiting with Redis sorted sets scored by timestamp,
  this is the same structure expressed as `WHERE occurred_at >= now - window`. The
  append-only shape is what removes the in-place mutation that would otherwise be the thing
  to race on.

- **`nil` as a valid disabled control is the Null Object pattern, for free.** You do not
  allocate a no-op implementation; you pass `nil` and let the method's first line handle it.
  In Java a null receiver is always an NPE; in Go it is a design tool — and the cost is that
  the nil check is *your* discipline, since nothing enforces it.

- **A self-consistent full-chain rewrite is `git filter-branch`.** Rewriting a commit forces
  you to rewrite every descendant SHA — which is exactly what an attacker with DB write
  access can do to the audit chain. Git's defense is that someone *else* holds a ref or a
  signed tag; ours is `--expect-head-hash` recorded out of band. Same shape, same limit.

- **Functional options are the builder pattern** (Java/TS) or keyword arguments with
  defaults (Python), implemented as closures over a private config because Go has neither
  named nor default parameters.

## 6. Gotchas & idioms

- **`map[string]struct{}` is Go's set.** `ApprovalGate.approvers` is a
  `map[string]struct{}` with `set[a] = struct{}{}`. The empty struct occupies zero bytes, so
  the map stores only keys — the idiomatic set. Membership is `_, ok := set[id]`. A
  `map[string]bool` works too but wastes a byte per entry and invites the `set[x] == false`
  ambiguity (absent vs present-and-false).

- **`sql.NullString` / `uuid.NullUUID` for nullable columns.** `approver`, `tx_hash`, and
  `payment_id` are nullable, so they map to `sql.NullString{String: v, Valid: true}` and
  `uuid.NullUUID{...}`. Forgetting `Valid: true` writes SQL `NULL` regardless of the value —
  a classic silent bug. The read path checks `.Valid` before reading `.String`/`.UUID`.

- **`big.Int.IsInt64()` guards the Postgres `BIGINT` boundary.** Both stores reject an
  amount that won't fit `int64` *before* inserting, because storing it would overflow or
  truncate — and an approval must replay the *exact* amount. The value itself is never
  surfaced in the error (it may be sensitive); only the fact of overflow.

- **The advisory-lock key is an explicit `fnv64a`, not Postgres `hashtext()`.** `hashtext`'s
  algorithm is undocumented and has changed across major versions; pinning the hash in Go
  keeps the key stable and reviewable independent of the server version. A hash *collision*
  only over-serializes two unrelated keys — safe, never a correctness bug.

- **`defer func() { _ = tx.Rollback() }()` is the universal transaction idiom.** Rollback
  after a successful `Commit` is a benign no-op (`sql.ErrTxDone`), so deferring it
  unconditionally guarantees cleanup on *every* error path without a rollback-on-each-return.
  The `_ =` explicitly discards the expected `ErrTxDone`.

- **`genesisPrevHash` is a package-level `make([]byte, 32)` — don't mutate it.** It's shared
  (the first row's `prev_hash` and `Verify`'s genesis check both read it). `Append`/`Verify`
  only *compare* against it (`bytes.Equal`) and pass it into a hash, never mutate it, so the
  shared slice is safe. A newcomer who did `genesisPrevHash[0] = 1` anywhere would corrupt
  every genesis check globally.

- **A blank `_ context.Context` parameter is contract-mandated presence, not laziness.**
  `func (d *Denylist) Screen(_ context.Context, address string) error` names the parameter
  `_` to say "required by `Screener`, unused by this impl." The instinct from other
  languages — delete the unused parameter — is wrong here: removing it breaks interface
  satisfaction, and `var _ Screener = (*Denylist)(nil)` is what makes that break a compile
  error in `policy.go` rather than a puzzle at a distant call site.

- **`SetString(s, 10)` returns `(*big.Int, bool)`, not an error.** The `bool` is Go's comma-ok
  parse-success flag, same shape as a map read or a type assertion. Skipping the `ok` check
  leaves an unusable value and a silent mis-parse — easy to forget if you're used to parsers
  that throw.

- **Config keeps money as a `string` on purpose.** `PolicyVelocityMaxAmount` and
  `PolicyApprovalThreshold` stay strings so `internal/config` never imports `math/big`; the
  composition root parses them, exactly as `ChainMaxFeePerGasCapWei` already did. Don't
  "improve" config by parsing there — reading strings from the environment is config's job,
  and interpreting them into domain types is the composition root's.

- **`COALESCE(SUM(x), 0)` is load-bearing, not defensive noise.** `SUM` over zero rows is SQL
  `NULL`, and scanning `NULL` into a Go `int64` errors. The `COALESCE` (with the `::bigint`
  cast) is what lets `big.NewInt(row.Sum)` promise a never-nil `Usage.Sum`, which is why
  `decide` can call `usage.Sum.Add(...)` with no nil check. Drop it and the empty-window case
  breaks.

- **`sql.Open` does not connect.** It constructs the pool handle; the first query is where a
  bad `DatabaseURL` actually fails. That is why the fail-closed test asserts on `Charge`
  failing rather than on `sql.Open` failing.

- **`:execrows` + a guarded `WHERE` is a concurrency primitive, not just a query.** The
  generated method returns `(int64, error)`, and `rows == 0` means "the precondition was not
  true" — which the Go code *must* branch on. Treating these `UPDATE`s as fire-and-forget
  silently discards the double-broadcast, lost-race, and not-found protections.

- **`:exec` on a `SELECT` when you only want the side effect.** `AcquireAuditChainLock` is a
  `SELECT pg_advisory_xact_lock(...)` declared `:exec` because the result is discarded;
  sqlc generates an `ExecContext` returning just `error`.

- **Log hygiene is encoded in the error strings themselves.** The velocity *count* breach
  message carries the numbers (counts aren't sensitive); the *amount* breach message carries
  neither the running sum nor the amount. `key_id` is logged (a signing-key handle, useful
  for the audit trail); `--to` and amounts are not.

- **Reading a `nil` map is safe; writing to one panics.** `v, ok := m[k]` on a nil map
  returns the zero value and `false`. Worth filing away coming from Java's NPE-on-`null.get`
  — though `Load` always initializes `denied`, even on the disabled path, so it never
  arises here.

- **`t.TempDir()` + `t.Setenv` are the hermetic-test pair.** Each test writes its manifest to
  an auto-cleaned per-test directory and scopes the env var to the test with automatic
  restore. Note `t.Setenv` also forces the test to be non-parallel — correct, since process
  environment is global state.

- **An empty audit chain is valid.** `Verify(nil)` returns `Result{OK:true}` unless a head
  was *expected*, so a fresh install passes. "Expected head on an empty chain" is encoded as
  `KindHeadMismatch` at `Seq: 0`, which is what catches "someone truncated the whole table"
  when you hold an anchor.

- **The append-only trigger needs two triggers.** `BEFORE UPDATE OR DELETE` fires per row;
  `TRUNCATE` does not fire row triggers at all, so a separate `BEFORE TRUNCATE` statement
  trigger is required. Both raise, so there is no honest path to mutate the table through
  ordinary SQL — a superuser dropping them is out of scope, which is what the anchor is for.

## 7. Check yourself

1. `pgVelocityStore.Charge` records the spend event *before* the payment broadcasts (in the
   same transaction). Walk through what a competitor design — record *after* a successful
   broadcast — does when five concurrent submits for one key each pass a `MaxCount: 3` check.
   Which design can breach the cap, and why?
2. The audit `seq` is app-assigned under an advisory lock instead of a `SERIAL` column. A
   colleague argues `SERIAL` is simpler and "gaps don't matter." Construct the exact
   sequence of events where a `SERIAL` seq makes `audit verify` report a tamper on an
   untampered chain.
3. `canonical` length-prefixes every variable field. Give two distinct `(actor, action)`
   pairs that would hash identically *without* the prefix, and show why the prefix separates
   them.
4. `approve` reopens the approval row to `pending` only when `broadcastIntent` returns an
   *empty* tx hash. Why is reopening on a *non-empty* hash a double-send hazard, and what
   does the guarded `Reopen` `WHERE` clause guarantee even if the caller got this wrong?
5. `Verify` already checks every link and every row's hash. Why is `WithExpectedHead` still
   necessary — what specific tamper does a clean structural walk fail to catch, and why can't
   an "expected row count" substitute for it?
6. `ApprovalGate.Required` is called on a possibly-nil `*ApprovalGate` and returns `false`
   without panicking. Explain, in terms of how Go dispatches a pointer-receiver method, why
   this is safe here but would panic if `enabled()` read `g.threshold` before `g != nil`.
7. Velocity got a `policy.VelocityStore` interface; four-eyes deliberately got no
   `policy.ApprovalStore`. Name the single *structural* difference between the two slices
   that justifies the divergence — and say what such an interface would have, and lack, that
   makes it speculative here.
8. Velocity serializes with `pg_advisory_xact_lock`; four-eyes with `SELECT … FOR UPDATE`.
   Derive each choice from the *shape of the data being protected*, then explain why swapping
   them would be impossible in one direction and merely worse in the other.
9. A two-method velocity store (`WindowUsage`, then `InsertEvent`) is individually correct in
   each method and still cannot enforce the cap. What does the single `Charge`-with-`decide`
   shape provide that two calls cannot — and which of the two layering rules would a
   `*sql.Tx` parameter have broken instead?
10. `Verify` rehashes `row.Payload` — the bytes as stored — rather than re-marshaling the Go
    value. Construct the sequence of perfectly innocent code changes that would make the
    re-marshaling version report tamper on the entire historical chain, and say why `jsonb`
    would not have saved it.

<details>
<summary>Answers</summary>

1. Record-after-success: all five concurrently pass the `SELECT`-sum check (usage reads 0 <
   3 for each, because none has recorded yet), then all five broadcast — six-way breach of a
   3-cap. Even serialized reads don't help if the record happens after the broadcast round-
   trip, because the check-to-record window spans the whole send. Record-on-attempt under
   the per-key advisory lock serializes read-decide-*insert* as one unit: submit #4 reads
   usage=3, `3+1 > 3`, and is rejected before it ever dials. The cap holds; the price is that
   a failed broadcast still consumed budget (safe direction).
2. Two writers append concurrently; writer B's transaction gets `seq = 7` from the `SERIAL`,
   then rolls back (a constraint error, a signer failure, anything). The `SERIAL` value 7 is
   *consumed* and never reissued; writer C gets `seq = 8`. Now the stored chain is
   …5, 6, 8… — `Verify` sees `row.Seq (8) != prevSeq+1 (7)` and reports `KindGap`, i.e. a
   deleted row, on a chain nobody tampered with. App-assigning `seq = head.seq + 1` under
   the chain lock means B's rollback assigns nothing and C gets 7 — gap-free.
3. `actor="ab", action="c"` and `actor="a", action="bc"` both concatenate to bytes `abc` in
   the actor+action region — identical preimage, identical hash, so one can be forged into
   the other. With `writeField`, the first frames as `\x00…\x02 a b \x00…\x01 c` and the
   second as `\x00…\x01 a \x00…\x02 b c`: the length prefixes differ, so the preimages differ
   and the hashes differ.
4. A non-empty hash means `adapter.Submit` already broadcast the transaction; only the
   optional payment-id link failed. Reopening to `pending` would let a *second* `approve`
   claim and re-broadcast the *same* payment — a double-send moving value twice. So the code
   never reopens on a non-empty hash. Even if a caller wrongly tried, `Reopen`'s
   `WHERE status='approved' AND tx_hash IS NULL` guards it — but note the hash is recorded by
   the best-effort `MarkBroadcast`, so the definitive protection is the caller's empty/non-
   empty branch; the `WHERE` clause backstops the *pre*-send case.
5. Tail-truncation: delete the last K rows. Rows 1..N−K still form a perfect chain from
   genesis — every link matches, every hash recomputes — so the structural walk passes. Only
   comparing the head against an *externally recorded* prior head (`WithExpectedHead`) reveals
   the chain got shorter. An expected *count* can't substitute because an attacker who
   truncates can't be assumed to leave the count intact; and it's redundant anyway, since
   `seq` is inside every preimage, so a matching head hash already pins the prefix length.
6. A Go pointer-receiver method is compiled to a function taking the receiver pointer as its
   first argument; invoking it with a nil pointer is legal — the panic only comes from
   *dereferencing* nil. `enabled()` evaluates `g != nil` first and `&&` short-circuits, so
   `g.threshold` is never read when `g == nil`. Reorder it to `g.threshold != nil && g != nil`
   and the `g.threshold` load dereferences a nil pointer before the nil check runs → panic.
   Order of the nil guard is the whole trick.
7. In velocity, the *domain* package is the consumer: `policy.VelocityLimiter` calls
   `store.Charge(...)`, so the port sits next to a real caller and lets a unit test fake that
   caller's dependency. In four-eyes, **no policy code calls the store at all** — `cmd`
   orchestrates (`runSubmit`→`Propose`, `runApprove`→`Claim`) and policy only supplies db-free
   decision types. A `policy.ApprovalStore` would have exactly one implementation and zero
   domain-side consumers: an abstraction with no caller to abstract for. The mechanism that
   actually mattered — running policy's verdict inside the store's lock via a callback — was
   kept as a plain `func` parameter; only the named interface was cut.
8. Velocity protects "the set of rows in a time window" over an append-only log. That is not
   a lockable object: some of the rows do not exist yet, and locking the ones that do would
   not stop a new insert — so `FOR UPDATE` cannot close the race at all, and an
   application-defined named mutex is the only option. An approval *is* one row with a
   primary key, so `FOR UPDATE` locks the object at risk directly. Swapping is therefore
   impossible for velocity and merely worse for approval: an advisory lock there would hash
   into the same global int64 namespace velocity already uses (cross-feature collision risk)
   to protect something that was directly lockable.
9. Two methods means two transactions with a gap between them, and a concurrent submission
   slips its own sum-and-insert into the gap — both read a pre-insert window, both pass.
   `Charge` + `decide` keeps read, decision, and write inside **one** transaction under
   **one** advisory lock, so nothing can observe or mutate the window between the check and
   the insert. Passing a `*sql.Tx` into policy would have achieved the atomicity but broken
   the *other* rule: `internal/policy` must not import `internal/db`. The callback satisfies
   both because it carries only a plain `Usage` value across the boundary.
10. Any of: adding a field to the event struct, switching a field to `any`, giving a type a
    custom `MarshalJSON`, or upgrading Go to a version whose encoder escapes differently. Each
    changes the *bytes* `json.Marshal` produces without changing the logical value — so every
    row whose `entry_hash` was computed over the old bytes now recomputes to something else,
    and `Verify` reports `KindHashMismatch` on the whole honest chain. `jsonb` would not save
    it and would in fact add a second copy of the same problem: Postgres re-normalizes `jsonb`
    on write and read (key order, whitespace, number formatting), so even the *stored* bytes
    would no longer be the bytes that were hashed. Storing the marshaled bytes verbatim in
    `BYTEA` and rehashing exactly those is what makes the round-trip byte-exact.

</details>

## 8. Further reading

- [Go blog — Working with Errors in Go 1.13](https://go.dev/blog/go1.13-errors) — `%w`,
  `errors.Is`/`errors.As`, and the sentinel-through-callback pattern the stores rely on.
- [PostgreSQL — Advisory Locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)
  — `pg_advisory_xact_lock` and why xact-scoped locks auto-release at commit/rollback.
- [PostgreSQL — `SELECT … FOR UPDATE` (Row-Level Locking)](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE)
  — the row lock behind the four-eyes claim, and how a guarded `UPDATE` acts as a CAS.
- [`encoding/binary` package docs](https://pkg.go.dev/encoding/binary#ByteOrder) — `BigEndian.PutUint64`
  and why an explicit byte order matters for a portable, reproducible hash preimage.
- [`crypto/sha256` package docs](https://pkg.go.dev/crypto/sha256) and
  [`hash.Hash`](https://pkg.go.dev/hash#Hash) — the streaming `Write`/`Sum` contract
  `entryHash` uses to chain `prev_hash ‖ canonical`, and why `Sum(nil)` is the right call.
- [PostgreSQL — Transaction Isolation](https://www.postgresql.org/docs/current/transaction-iso.html)
  — Read Committed versus Serializable, and the serialization-failure retry contract the
  advisory lock exists to avoid pushing onto callers.
- [`math/big` — `Int.IsInt64`](https://pkg.go.dev/math/big#Int.IsInt64) — the check-before-you-
  convert discipline at the arbitrary-precision → `BIGINT` boundary (Java's
  `BigInteger.longValueExact()` throws; Go makes you ask first).
- [`time.Time.UnixMicro`](https://pkg.go.dev/time#Time.UnixMicro) — the location-independent
  absolute instant the audit preimage commits to, instead of a formatted or zone-bearing time.
- [EIP-55 — mixed-case checksum addresses](https://eips.ethereum.org/EIPS/eip-55) and
  [go-ethereum `common`](https://pkg.go.dev/github.com/ethereum/go-ethereum/common) —
  why a screening control must normalize both sides through the same function, and why
  `HexToAddress` needs `IsHexAddress` in front of it.
