# M1 — The Double-Entry Ledger: derived balances, sentinel errors, and a fakeable transactor seam

> Scope: this lesson is about *domain modeling and testing in Go*, using the
> double-entry ledger as the vehicle. The headline achievements are (1) a domain
> package that runs its full logic against Postgres in production and an in-memory
> map in tests, through one interface; and (2) an error model where callers
> discriminate outcomes without ever string-matching. Both are Go idioms worth
> internalizing — you will reuse them in every service that follows.

## 1. What we built

M1/WP1 laid down the ledger's storage shape as three Postgres tables
(`db/migrations/0001_init.up.sql`): `accounts`, `journal_entries`, and
`entry_lines`. The load-bearing decision is that **an account's balance is never
stored**. There is no `balance` column anywhere. A balance is always *derived* by
summing that account's lines: `Σ(credit) − Σ(debit)` (see `GetAccountBalance` in
`db/query/ledger.sql`). Journal entries are immutable once written, so the ledger
is an append-only log and every balance is a pure function of that log. Two
uniqueness rules ride along: `UNIQUE(name, asset)` on accounts, and
`UNIQUE(kind, external_ref)` on journal entries — the latter is ledger-level
idempotency, so a retried operation collapses to one posting instead of
double-charging.

WP2 built the domain logic on top, in `internal/ledger`. `balance.go` holds two
*pure* functions — `Balanced` (are the lines a valid, balanced posting?) and
`ApplyToBalances` (project lines onto a copy of current balances, refusing to go
negative). `ledger.go` holds the orchestration: `Service.PostEntry` validates an
entry, locks the touched accounts, checks funds and asset/status, then writes the
journal entry and one row per line — all inside a single transaction, so either
every row lands or none does.

The part to study hardest is *how this was verified*. There is no live database in
the test run. The domain talks to storage through a two-method seam — a `Store`
interface (`ExecTx`) and sqlc's generated `Querier` interface — and the tests
supply an in-memory `fakeQuerier` that recomputes balances from a slice of lines,
exactly as the SQL does. Table-driven tests plus `testing/quick` property tests
(`property_test.go`) exercise every branch of `PostEntry` — overdraw, duplicate,
cross-asset, frozen account, cancelled context — with zero I/O.

## 2. The design decision

### Decision A: balances are derived, never stored

**The problem.** A ledger must answer "what is account X's balance?" cheaply while
guaranteeing that balance is *always* consistent with the history that produced it.

**The chosen approach — event-sourced balances.** The `entry_lines` table is the
source of truth. Balance is a query: `SUM(CASE WHEN direction = 'credit' THEN
amount ELSE -amount END)`. The database enforces cheap *local* invariants with
`CHECK` constraints — `amount > 0` and `direction IN ('debit','credit')` — plus
referential integrity via foreign keys. The expensive *global* invariants
(debits == credits across a posting; no account goes negative) live in the Go
domain, because they span multiple rows and require reading balances under lock.

**Alternative 1: a stored `balance` column, updated in place.** Rejected. Now you
have two sources of truth — the running total and the line history — and every
write must keep them in lockstep inside the same transaction. Any bug, crash, or
out-of-band `UPDATE` silently corrupts money. Reconciliation ("does the column
equal the sum of lines?") becomes a permanent operational chore. Derived balances
make that class of drift *unrepresentable*.

**Alternative 2: enforce non-negativity in the database** (a trigger or a
`CHECK` on a materialized balance). Rejected for M1: a per-row `CHECK` can't see a
sum, and a trigger that recomputes and compares on every insert is both slower and
harder to test than a Go function. The split we chose — DB does O(1) local checks,
app does the multi-row arithmetic under `FOR UPDATE` — keeps the sharp business
rules in fast, exhaustively unit-testable code. The honest trade-off: correctness
of the global invariant now depends on *every writer* going through `PostEntry`.
A second code path that inserts lines directly could overdraw an account. In
production you'd defend that boundary (restricted DB role, or a deferred
constraint trigger as a backstop); M1 accepts it because there is exactly one
writer.

**The performance caveat.** Deriving balance is `O(lines for that account)`. With
`idx_entry_lines_account` it's an index scan, fine for thousands of lines, but a
hot account accumulating millions of lines will eventually need rollup snapshots
(a periodic "balance as of entry N" checkpoint you sum forward from). That's a
later milestone; the append-only log makes adding it non-breaking.

### Decision B: the `Store` / `Querier` transactor seam

**The problem.** `PostEntry` must run several queries inside one transaction, yet
the domain package must not import a live `*sql.DB` (it deliberately contains no
production wiring — that's deferred to WP5) and its tests must not need Postgres.

**The chosen approach — a function-scoped transaction seam.** Two interfaces:

```go
// Store is the transaction-boundary seam.
type Store interface {
    ExecTx(ctx context.Context, fn func(q db.Querier) error) error
}
```

`ExecTx` owns the transaction *lifecycle*; the caller supplies a closure that does
the *work* against whatever `db.Querier` it's handed. The production `Store` (WP5)
will `BEGIN`, build a `Querier` bound to that `*sql.Tx`, run `fn`, then `COMMIT`
on nil / `ROLLBACK` on error-or-panic. The domain never sees any of that — it only
sees "give me a Querier, I'll return an error if you should roll back." This is the
**unit-of-work** pattern expressed as a higher-order function, and it's the M1
learning centerpiece.

**Alternative 1: pass `*sql.DB` / `*sql.Tx` into the Service.** Rejected. It
couples the domain to a concrete driver, forces tests to spin up Postgres (slow,
flaky, and now your "unit" test is an integration test), and leaks transaction
mechanics into business code.

**Alternative 2: begin/commit manually inside `PostEntry`.** Rejected. The
commit/rollback/panic-recovery dance would be copy-pasted into every future
write method and is exactly the kind of thing you get subtly wrong (forgetting
rollback on an early return leaks a connection). Hoisting it into `ExecTx` means
it's written and tested once.

## 3. Language deep-dive

### 3a. sqlc `emit_interface` is *the* seam — the fake is only possible because of it

`sqlc.yaml` sets `emit_interface: true`. That single flag makes sqlc emit, next to
the concrete `*Queries` struct, an interface capturing every query:

```go
// internal/db/querier.go  (generated)
type Querier interface {
    CreateAccount(ctx context.Context, arg CreateAccountParams) (Account, error)
    GetAccount(ctx context.Context, id uuid.UUID) (Account, error)
    GetAccountBalance(ctx context.Context, accountID uuid.UUID) (int64, error)
    GetAccountsForUpdate(ctx context.Context, ids []uuid.UUID) ([]Account, error)
    InsertEntryLine(ctx context.Context, arg InsertEntryLineParams) (EntryLine, error)
    InsertJournalEntry(ctx context.Context, arg InsertJournalEntryParams) (JournalEntry, error)
}

var _ Querier = (*Queries)(nil)  // compile-time proof the real impl satisfies it
```

`Store.ExecTx` hands the closure a `db.Querier`, *not* a `*db.Queries`. Because Go
interface satisfaction is **implicit/structural** — a type satisfies an interface
merely by having the methods, no `implements` keyword — the test can define its own
type with the same six methods and it *is* a `Querier`, with no dependency edge
back to the fake:

```go
// internal/ledger/fake_test.go
type fakeQuerier struct {
    accounts map[uuid.UUID]db.Account
    entries  map[uuid.UUID]db.JournalEntry
    lines    []db.EntryLine
    seenRef  map[string]struct{} // enforces UNIQUE(kind, external_ref)
    nextLine int64
}

var _ db.Querier = (*fakeQuerier)(nil)  // same trick, on the fake
```

That `var _ db.Querier = (*fakeQuerier)(nil)` line is a Go idiom worth adopting
everywhere: it's a zero-cost compile-time assertion. It allocates nothing
(the value is a typed `nil`) and produces no runtime code; it exists purely so the
compiler yells the moment `fakeQuerier` drifts out of sync with the interface —
e.g. when a future query adds a method to `Querier`. Coming from Java/C#, this is
`implements Querier` checked at the point *you* choose, on a type that never names
the interface.

The payoff: `fakeStore.ExecTx` just runs the closure against shared state, no
transaction at all:

```go
// A real Store would BEGIN/COMMIT with ctx; the fake runs fn against shared state.
func (s *fakeStore) ExecTx(_ context.Context, fn func(q db.Querier) error) error {
    return fn(s.q)
}
```

The `_` receiver-side parameter name for `ctx` is deliberate: the fake ignores the
context *at the transaction boundary* (there's no real tx to bind it to) but each
`fakeQuerier` method still checks `ctx.Err()`, mirroring how `QueryContext`
observes cancellation. That's why the "cancelled context propagates" test passes
against the fake — see 3d.

### 3b. Pure core: copy-in semantics and why `map` is a reference under the hood

`ApplyToBalances` is the heart of the no-negative invariant, and its first three
lines encode a subtle Go fact:

```go
func ApplyToBalances(cur map[uuid.UUID]int64, lines []Line) (map[uuid.UUID]int64, error) {
    next := make(map[uuid.UUID]int64, len(cur))
    for id, bal := range cur {
        next[id] = bal            // copy every entry into a fresh map
    }
    // ... mutate next, never cur ...
```

A Go `map` is a *reference type*: the `map` header you pass by value points at the
same underlying hash table, so writing `cur[id] = x` inside the function would be
visible to the caller. That's a classic newcomer trap — "I passed it by value, why
did it change?" The function defends against it by building `next` and only ever
mutating `next`. The test `"does not mutate input"` pins this behavior: it asserts
`cur[a]` is unchanged after the call. Contrast with a slice or a struct passed by
value — those copy the header too, but a *struct* copy is a real deep-ish copy of
its fields, whereas a map/slice copy still shares the backing store. Knowing which
built-ins alias is essential Go literacy.

Note also the credit/debit convention is defined in exactly two places and they
agree by construction: the SQL `GetAccountBalance` (`credit → +amount`,
`debit → −amount`) and this Go loop (`Credit: +=`, `Debit: -=`). The fake's
`GetAccountBalance` re-implements the same rule, which is *why* the property test
can trust it as an oracle.

### 3c. `%w` wrapping + sentinel discrimination, including the `errors.As` driver translation

The error model is the other big idiom. Package-level *sentinel* errors are
declared once:

```go
var (
    ErrUnbalanced        = errors.New("ledger: entry is unbalanced")
    ErrInsufficientFunds = errors.New("ledger: insufficient funds")
    ErrDuplicateEntry    = errors.New("ledger: duplicate entry")
    ErrInvalidEntry      = errors.New("ledger: invalid entry")
)
```

Return sites wrap them with the `%w` verb, which produces an error that both
carries human context *and* remembers its cause in an unwrappable chain:

```go
return fmt.Errorf("ledger: debits %d != credits %d: %w", debits, credits, ErrUnbalanced)
```

`%w` (as opposed to `%v`) is what makes `errors.Is(err, ErrUnbalanced)` return true
several layers up. Callers never compare `err.Error()` strings — they ask
`errors.Is`, which walks the `Unwrap` chain. `logResult` is the canonical consumer:

```go
switch {
case err == nil:
    s.log.InfoContext(ctx, "journal entry posted", attrs...)
case errors.Is(err, ErrInvalidEntry):
    s.log.InfoContext(ctx, "journal entry rejected: invalid", attrs...)
case errors.Is(err, ErrInsufficientFunds):
    s.log.WarnContext(ctx, "journal entry rejected: insufficient funds", attrs...)
// ... ErrDuplicateEntry (warn), ErrUnbalanced (error) ...
}
```

The subtle move is translating a *driver-specific* error into a domain sentinel.
Postgres reports a unique-constraint violation as SQLSTATE `23505`. The `lib/pq`
driver surfaces that as a concrete `*pq.Error`. `mapInsertError` reaches into the
error chain to find it:

```go
func mapInsertError(err error) error {
    var pqErr *pq.Error
    if errors.As(err, &pqErr) && pqErr.Code == "23505" {
        return fmt.Errorf("ledger: entry already posted: %w", ErrDuplicateEntry)
    }
    return fmt.Errorf("ledger: insert journal entry: %w", err)
}
```

Use `errors.Is` when you're matching a *sentinel value*; use `errors.As` when you
need the *concrete type* out of the chain (here to read `.Code`). `errors.As`
takes a pointer-to-the-target (`&pqErr`) and, on a match, assigns the unwrapped
error into it — the two-star dance (`var pqErr *pq.Error`, pass `&pqErr`) is
idiomatic and initially surprising. Crucially this keeps working even after WP5's
real `Store` wraps the driver error, because `errors.As` unwraps. And it's fully
testable without Postgres: the fake's `InsertJournalEntry` *fabricates* a
`&pq.Error{Code: "23505", ...}` on a duplicate key, so the whole translation path
is exercised in memory (see the `"duplicate external_ref"` test).

### 3d. Context propagation Service → Store → Querier

`ctx` threads through every layer: `PostEntry(ctx, e)` → `store.ExecTx(ctx, fn)` →
inside `fn`, every call is `q.GetAccountsForUpdate(ctx, ids)`,
`q.GetAccountBalance(ctx, id)`, etc. This is Go's answer to cancellation and
deadlines — there's no thread-local, no ambient request scope; the context is an
explicit first argument by convention. The fake honors it at the leaf:

```go
func (q *fakeQuerier) GetAccountBalance(ctx context.Context, accountID uuid.UUID) (int64, error) {
    if err := ctx.Err(); err != nil {
        return 0, err
    }
    // ...
}
```

`ctx.Err()` returns non-nil (`context.Canceled` or `context.DeadlineExceeded`)
once the context is done. That single check is what lets the `"cancelled context
propagates"` test cancel *before* calling `PostEntry` and then assert
`errors.Is(err, context.Canceled)` — the error bubbles up through the wrapped
`fmt.Errorf("...: %w", err)` at the lock step, and `errors.Is` still finds the
standard-library sentinel underneath. Against a real DB, `QueryContext` does the
same thing at the driver boundary; the fake is faithful to that contract.

## 4. What would break

- **Storing balances would invite drift.** By deriving, the drift bug can't exist.
  The residual risk (a writer bypassing `PostEntry`) is named honestly in §2A.
- **A partial write on an aborted posting.** `PostEntry` checks funds/asset/status
  *before* any `Insert*` call, so all business rejects return before writing. The
  tests assert this directly (`len(q.entries) == 0`, `len(q.lines)` unchanged after
  overdraw/cross-asset/frozen). In production the surrounding real transaction is
  the belt-and-suspenders: even a failure *after* the first insert rolls back.
- **Deadlocks between concurrent transfers.** Two postings touching accounts A and
  B in opposite orders would deadlock on `FOR UPDATE`. `accountIDs` de-duplicates
  and **sorts** the ids (`slices.SortFunc` with `bytes.Compare` over the UUID
  bytes), and the SQL `ORDER BY id FOR UPDATE` locks in that same total order, so
  all transactions grab locks in one direction. A newcomer would likely lock in
  "whatever order the lines came in" and ship an intermittent deadlock.
- **Leaking the connection / forgetting rollback.** Hoisting begin/commit into
  `ExecTx` means the domain physically *cannot* forget rollback — it just returns
  an error and the seam handles it. Hand-rolled tx code is where those leaks live.
- **Newcomer bug: mutating the caller's map.** Covered in 3b — `ApplyToBalances`
  copies in. Without the copy, a caller reusing its balance map would see it
  silently corrupted.
- **Newcomer bug: string-matching errors.** `if strings.Contains(err.Error(),
  "duplicate")` is brittle and locale/driver-dependent. `errors.Is/As` on
  sentinels is the robust form, and it survives WP5 wrapping the driver error.
- **Leaking sensitive data in logs.** `logResult` deliberately logs *category +
  external_ref* and never the amounts or `err.Error()` — because the wrapped
  sentinel messages embed monetary values (`"balance -5 < 0"`,
  `"debits 30 != credits 20"`). Attaching the raw error would leak exactly what the
  attribute list withholds. Callers who need detail get the *returned* error.

## 5. Compared to what you know

- **`Store.ExecTx(ctx, func(q Querier) error)`** is Spring's
  `TransactionTemplate.execute(status -> {...})` or a Rails
  `ActiveRecord::Base.transaction do ... end` block — the framework owns
  begin/commit/rollback, your closure owns the work. The difference: here it's a
  plain interface + higher-order function, no annotations, no proxies, no hidden
  thread-local. The transaction handle is passed *explicitly* as `q`.
- **Implicit interface satisfaction** is structural typing like TypeScript's
  `interface`, or Python duck typing — *except* it's checked at compile time.
  `fakeQuerier` never says it implements `Querier`; the compiler proves it. Unlike
  Java/C#, you never edit the concrete type to "add an interface."
- **`errors.Is` / `%w`** is Java's exception `getCause()` chain, or Python's
  `raise ... from ...` and `__cause__`. `errors.As` is `catch (PqException e)` /
  `except pq.Error as e` — pull the typed cause out to inspect a field. The
  analogy breaks down in that Go errors are *values you return*, not control flow
  you throw; there's no stack unwinding, so the `switch` in `logResult` is just a
  normal branch, not a catch ladder.
- **Derived balances** are event sourcing: `entry_lines` is the event log, balance
  is a fold over it. The `SUM(CASE...)` is the projection.
- **`testing/quick`** is QuickCheck / Hypothesis / jqwik — you assert a *property*
  ("conservation of value holds for any amount") and the framework generates inputs.

## 6. Gotchas & idioms

- **`map[uuid.UUID]struct{}` as a set.** `accountIDs` and `seenRef` use
  `struct{}` values — the empty struct occupies zero bytes, so this is the
  idiomatic Go set. `seen[id] = struct{}{}` adds; `_, ok := seen[id]` tests.
- **Zero values are meaningful.** `PostEntry` returns `db.JournalEntry{}` (the zero
  struct) alongside a non-nil error — Go has no "null struct," so the idiom is
  "zero value + error, and the caller must check the error first." `NewService`
  leans on the same idea: a `nil` `*slog.Logger` triggers the `slog.Default()`
  fallback, so callers who don't care can pass `nil`.
- **`Direction string` is a named type, not an alias.** It documents intent and
  lets the `switch` in `Balanced`/`ApplyToBalances` have a `default` that catches
  a bogus `"sideways"` direction (tested). Note the boundary conversions:
  `string(l.Direction)` when writing to the DB, `string(Credit)` when comparing in
  the fake — Go won't implicitly convert a named type to its underlying type.
- **Reading balances *under lock*.** The order in `PostEntry` matters:
  `GetAccountsForUpdate` (acquire locks) *then* `GetAccountBalance` (read). Reading
  before locking would be a TOCTOU race — another transaction could post between
  your read and your write. The fake can't demonstrate this (no real locks) but the
  code structure is correct for when the real `Store` arrives.
- **`emit_empty_slices: true`.** sqlc will return `[]Account{}` rather than `nil`
  for a `:many` that finds nothing. The domain doesn't strictly depend on it here
  (it maps by id and checks membership), but it removes the nil-vs-empty ambiguity
  that trips up JSON encoders and `len` assumptions.
- **The fake has no rollback — and that's fine, deliberately.** Its doc comment
  says so: every abort path under test returns *before* writing, so "no partial
  write" holds without rollback. That's an honest scoping call, not an oversight;
  true rollback semantics belong to WP5's real `Store` and its integration tests.

## 7. Check yourself

1. `GetAccountBalance` is `O(n)` in an account's line count. Sketch the schema
   change that keeps balances cheap for a hot account *without* abandoning the
   derived-balances invariant. What makes the append-only log make this
   non-breaking?
2. Why does `mapInsertError` use `errors.As` rather than `errors.Is`? What
   would `errors.Is(err, &pq.Error{Code:"23505"})` actually do, and why is it
   wrong here?
3. `ApplyToBalances` copies `cur` into `next`. Write the one-line bug you'd
   introduce by "optimizing away" the copy, and name the failing test.
4. Concurrent transfer T1 moves money A→B, T2 moves B→A, at the same instant.
   Explain precisely how `accountIDs` + the SQL `ORDER BY id FOR UPDATE` prevent
   a deadlock. What symptom would you see if `accountIDs` didn't sort?
5. Why does `logResult` refuse to log `err.Error()`, even though it's *returned*
   to the caller unredacted? What's the threat model difference between a log sink
   and a function return value?

<details>
<summary>Answers</summary>

1. Add a periodic **snapshot/checkpoint** row per account — "balance as of
   `entry_line.id = N`" — and compute current balance as `snapshot + Σ(lines with
   id > N)`. It's non-breaking because the log is immutable and append-only: a
   snapshot is a derived cache you can rebuild or discard at will; nothing that
   already works changes.
2. `errors.As` extracts the concrete `*pq.Error` from the chain so you can read
   `.Code`. `errors.Is` compares for *equality/`Is`-ness* against a target value;
   `&pq.Error{...}` is a fresh pointer that won't equal the driver's error, and
   `pq.Error` doesn't implement an `Is` method, so it'd essentially never match.
   You need the *type*, not a value comparison.
3. `next := cur` (aliasing the same map) instead of copying. Now the debit/credit
   loop mutates the caller's map. Failing test:
   `TestApplyToBalances/does_not_mutate_input`.
4. Both transactions compute the *same sorted* lock set `[min(A,B), max(A,B)]` and
   `FOR UPDATE` acquires row locks in that identical order, so one fully acquires
   before the other starts — no cyclic wait. Without sorting, T1 could lock A then
   wait on B while T2 locks B then waits on A: a classic deadlock, which Postgres
   would detect and abort one transaction with a `40P01` error (intermittent,
   load-dependent).
5. Logs are broadly readable (aggregation systems, ops, third parties) and persist
   indefinitely, so leaking amounts there widens exposure. The returned error goes
   to exactly one caller who already has the entry in hand and decides how to
   surface it. Same bytes, very different blast radius.

</details>

## 8. Further reading

- [Go blog — Working with Errors in Go 1.13](https://go.dev/blog/go1.13-errors)
  (`%w`, `errors.Is`, `errors.As`).
- [`errors` package docs](https://pkg.go.dev/errors) — the `Is`/`As`/`Unwrap`
  contracts, and how to implement `Is`/`As` on your own types.
- [`context` package docs](https://pkg.go.dev/context) — cancellation propagation
  and the "context is the first argument" convention.
- [sqlc documentation](https://docs.sqlc.dev/) — `emit_interface`,
  `emit_empty_slices`, type overrides, and how generated code maps to queries.
