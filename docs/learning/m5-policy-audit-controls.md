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
what makes the transition exactly-once even under concurrent approvers.

### Decision D: the audit log is a hash chain, and `Verify` re-derives it from genesis

**The problem.** An audit log an operator can quietly edit — change an amount, delete the
row that records a bad approval, reorder events — is not evidence. You want *tamper-evidence*:
not that alteration is impossible (a DBA with write access can change any byte), but that
any alteration is *detectable* after the fact.

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
*one* head, so *all* writers must serialize on *one* well-known constant. Second, the
**canonical preimage is length-prefixed**:

```go
writeUint64(uint64(seq)); writeUint64(uint64(occurredMicros))
writeField([]byte(actor)); writeField([]byte(action)); ... writeField(payload)
// writeField = writeUint64(len(b)) then the bytes
```

Without the length prefix, `actor="ab", action="c"` and `actor="a", action="bc"` serialize
to identical bytes — a trivial forgery that swaps field boundaries without changing the
hash. Prefixing every variable-length field with its length makes the frame unambiguous, so
distinct field values always produce distinct preimages. This is exactly why naïve string
concatenation is unsafe for any hash-committed structure.

**The anchor closes the one hole chaining alone can't.** A self-consistent chain can still
be *tail-truncated*: lop off the last K rows and rows 1..N−K still verify perfectly among
themselves. Chaining can't detect that, because there's nothing *after* the new head to
mismatch. So `Verify` accepts an optional `WithExpectedHead(hash)`: record the head hash
out-of-band after a good run, feed it back next time, and a shortened chain fails
`KindHeadMismatch`. Note an expected-*count* would be redundant — `seq` is inside every
preimage, so the head hash already pins the chain's prefix length.

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
- [`crypto/sha256` package docs](https://pkg.go.dev/crypto/sha256) — the streaming `Write`/`Sum`
  interface `entryHash` uses to chain `prev_hash ‖ canonical`.
