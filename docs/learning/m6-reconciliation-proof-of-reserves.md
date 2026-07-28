# M6 — Reconciliation & proof-of-reserves: two orthogonal truths, a tri-state exit code, and a keyset walk over a big table

> Scope: this lesson is about *asking two independent questions of the same treasury*
> — "does the chain hold exactly what the ledger says it should?" (reconciliation) and
> "does the chain hold at least what we owe our users?" (proof of reserves) — and
> reporting the answer through the one channel a cron job actually reads: the process
> **exit code**. The headline decisions are (1) a **tri-state `int` exit code** (0 clean
> / 1 discrepancy / 2 operational) that `runReconcile` returns *directly* and `main`
> passes straight to `os.Exit`, because "reserves are short" is a distinct outcome from
> "the job could not run" and collapsing both into `err→exit(1)` would blind the monitor;
> (2) a **reconciliation identity that treats confirmed-but-not-final funds as a bridge
> term, not a discrepancy** — `Discrepancy = ActualOnChain − ExpectedFinalized −
> ConfirmedPending` — kept strictly separate from the proof-of-reserves verdict, which is
> a *different* inequality (`ActualOnChain ≥ Liabilities`) that can fail even when the
> discrepancy is zero; (3) a **keyset (not OFFSET) walk** over a large `settlements` table
> whose termination condition is a short page, so each round-trip is one bounded index
> range scan that stays correct under concurrent inserts; and (4) **mixed `int64`/`*big.Int`
> arithmetic** — ledger sums are `int64` minor units, on-chain balances are `*big.Int`
> `uint256`, and `BuildReport` bridges them with the same fresh-allocation discipline the
> M2 adapter used. This builds on the M1 derived-balance lesson (balances are never
> stored, always folded) and the M2 `*big.Int` discipline (methods mutate the receiver).

## 1. What we built

M6 is the operator's answer to "prove the money is there." `internal/reconcile` is a
dependency-light package (stdlib + `internal/db` only, like `internal/audit`) whose core
is a set of *pure functions over a narrow querier seam*, so the whole report can be built
and asserted without a database. It folds three inputs into one `Report`: the ledger's
per-asset settlement expectation (paged out of a large `settlements` table), the on-chain
balance of each treasury address (read via a read-only `balanceOf` `eth_call`), and the
per-asset user liabilities (a single SQL sum). The output is a human-readable text report
on stdout *and* — the part that matters for automation — a tri-state exit code.

The command lives in `cmd/paymentrailctl/reconcile.go`. `runReconcile` mirrors the shape
of the existing `audit verify` command — its own `flag.FlagSet`, `config.Load`, a
validate-before-dial order, a short-lived DB pool under a signal-cancel context — but with
one deliberate divergence: it returns an `int`, not an `error`, and `main` calls
`os.Exit(runReconcile(...))` on it directly. That single divergence is the spine of the
milestone, because a discrepancy is a *successful run with a distinct code*, not a failure.

The part to study hardest is the split between the two verdicts. Reconciliation and
proof-of-reserves look like the same check — both compare on-chain reality to a
ledger-derived number — but they answer different questions and can disagree. A treasury
can reconcile perfectly (every wei accounted for) and still be undercollateralized (it
holds less than users are owed), or reconcile *badly* (an unexplained gap) while still
holding enough to cover liabilities. `BuildReport` computes both independently and only
reports `Clean` when *both* pass for *every* asset. Conflating them would let a real
solvency problem hide behind a clean reconciliation, or vice versa.

## 2. The design decision

### Decision A: a tri-state `int` exit code, returned by the command and threaded through `main`

**The problem.** This command runs unattended — a cron job, a Kubernetes `CronJob`, a
monitoring probe. The consumer of its result is not a human reading stdout; it is a shell
`if` or an alerting rule gating on `$?`. Such a consumer needs to distinguish three
outcomes: *clean* (do nothing), *discrepancy / undercollateralized* (page the treasury
team — the money is wrong), and *operational failure* (page the on-call SRE — the job
couldn't even run: bad config, DB down, RPC unreachable). The conventional Go CLI shape —
a function returning `error`, with `main` doing `if err != nil { os.Exit(1) }` — has only
*two* states and cannot tell "reserves are short" from "the database is down." Those demand
different runbooks and different humans.

**The chosen approach — `runReconcile(args []string) int` with three named codes, returned
directly; `main` does `os.Exit(runReconcile(...))`.**

```go
const (
	reconcileExitClean       = 0 // ran clean: every asset reconciled and collateralized
	reconcileExitDiscrepancy = 1 // ran fine but found a discrepancy or UNDERCOLLATERALIZED asset
	reconcileExitError       = 2 // usage / config / DB / RPC / operational failure
)
```

Every fail-closed branch in `runReconcile` (`config.Load` failed, registry bad, DB ping
failed, RPC dial failed, a query errored) prints to stderr and `return
reconcileExitError`. The one *success* path prints the report and returns
`reconcileExitCode(report.Clean)`, which maps the boolean onto 0 or 1. Code 2 is *never*
returned from the pure core — it belongs exclusively to the operational branches, so the
distinction "the numbers are wrong" vs "we couldn't get the numbers" is structural, not a
guess made after the fact. `main` threads it through with the minimum ceremony:

```go
if len(os.Args) > 1 && os.Args[1] == "reconcile" {
	os.Exit(runReconcile(os.Args[2:]))
}
```

**Alternative 1: return `error`, let `main` map it to `exit(1)`.** This is the repo's
convention for `submit`, `approve`, `audit verify` — and it's right for them, because for
those commands "it didn't work" is the only failure mode worth signalling. It fails here
because a discrepancy is *not an error*: the job ran to completion, produced a valid
report, and the report happens to say the money is wrong. Modelling that as an `error`
would force `main` to either collapse it into the same `exit(1)` as a DB outage (losing the
distinction the whole command exists to surface) or sniff the error with `errors.Is` to
recover the code — reconstructing a tri-state from a two-state type the long way round.

**Alternative 2: signal the outcome on stdout (print `CLEAN` / `DISCREPANCY`) and always
`exit(0)` unless the process crashes.** Machine-parseable text is fragile: a monitor now
has to grep stdout, and any change to the report format silently breaks the alert. The exit
code is the OS-level contract every scheduler already understands; `$?` needs no parser and
no format-stability promise. stdout carries the *human* artifact (with amounts); the exit
code carries the *machine* verdict. Keeping those on separate channels is the same
stdout-artifact / stderr-structured-log split the command also applies to logging.

**The cost, stated honestly.** `runReconcile` returning `int` breaks the repo's dominant
`func(...) error` idiom, so a reader has to notice *why* this one command is shaped
differently — the doc comment says so explicitly. That's a deliberate, localized
exception, not drift: exit-code semantics are the command's entire contract, so the command
owns its exit code rather than laundering it through an `error`.

### Decision B: reconciliation and proof-of-reserves are two independent checks; `Clean` is their AND

**The problem.** Both checks compare on-chain balance to a ledger number, so it's tempting
to write one comparison and call it done. But they answer different questions:

- **Reconciliation** asks: *is every unit of on-chain value explained by a settlement the
  ledger knows about?* An unexplained surplus or deficit means the ledger and the chain
  have drifted — a bug, a missed event, an out-of-band transfer. This is an *accounting
  integrity* check.
- **Proof of reserves** asks: *does the treasury hold at least what users are owed?* This
  is a *solvency* check. It doesn't care whether every wei is explained — it cares whether
  there are enough weis, full stop.

These genuinely disagree. A treasury can reconcile to exactly zero and still be
undercollateralized if liabilities exceed the tracked settlement value (e.g. a credit was
posted without a corresponding on-chain settlement). Conversely a treasury can carry an
unexplained surplus (nonzero discrepancy) while comfortably covering liabilities. A single
comparison cannot express both.

**The chosen approach — compute both per asset, keep them in separate fields, and fold
`Clean` as the AND of all of them.** The reconciliation identity is:

```go
// expected = finalized + confirmed-pending (bridge); Discrepancy is the
// signed gap between on-chain reality and that expectation.
expected := big.NewInt(s.FinalizedMinor)
expected.Add(expected, big.NewInt(s.ConfirmedMinor))
discrepancy := new(big.Int).Sub(actual, expected)

// Proof of reserves: actual on-chain must cover user liabilities.
verdict := verdictOK
if actual.Cmp(big.NewInt(liab)) < 0 {
	verdict = verdictUndercollateral
}

if discrepancy.Sign() != 0 || verdict != verdictOK {
	r.Clean = false
}
```

The subtle term is `ConfirmedMinor` — settlement value that is confirmed on-chain but not
yet *finalized* in the ledger. Those funds are **already on-chain** (so they show up in
`actual`) but the ledger hasn't marked them final. If you compared `actual` against only
the *finalized* sum, every in-flight settlement would look like a surplus discrepancy. So
confirmed-pending is a **bridge term on the expected side**: adding it to `expected` means
a treasury holding exactly its tracked settlement funds — some final, some merely confirmed
— nets to `Discrepancy == 0`. This is the accounting analogue of an in-transit reconciling
item on a bank reconciliation: not a discrepancy, a timing bridge.

**Alternative: one check — `actual == expected`.** This is what a newcomer writes, and it
silently drops the solvency question. A system that reconciles perfectly can still be
insolvent; a monitor watching only reconciliation would report "all clean" while the
treasury can't cover a withdrawal. The two checks are orthogonal by construction, and
`Clean` is deliberately the *conjunction* over both dimensions and all assets — one bad
asset on either axis flips the whole run to not-clean, which is exactly what maps to exit
code 1.

### Decision C: page the settlements table by keyset, terminate on a short page

**The problem.** The ledger's expectation is `Σ amount` over every `settled`/`finalized`
settlement — potentially a very large table. You cannot `SELECT ... ` it all into memory,
and you must sum it *correctly* even while new settlements are being inserted concurrently.

**The chosen approach — a two-query keyset walk ordered by `(created_at, id)`, oldest
first, continuing past the last row's cursor until a page comes back short.** The SQL uses
a row-value comparison, which Postgres executes as a single index range scan:

```sql
WHERE s.status IN ('settled', 'finalized')
  AND (s.created_at, s.id) > (sqlc.arg(after_created_at)::timestamptz, sqlc.arg(after_id)::uuid)
ORDER BY s.created_at, s.id
LIMIT sqlc.arg(page_limit);
```

and the Go loop terminates on the first short page:

```go
n := len(first)
// ... accumulate first page, set cursor ...
for n == int(pageSize) {
	page, err := q.ListSettlementsForReconcileAfter(ctx, ...)
	// ... accumulate, advance cursor ...
	n = len(page)
}
```

**Why the sort key is `(created_at, id)` and not just `created_at` — this is *the* keyset
bug.** `created_at` is not unique; many settlements can share a millisecond. Cursor on it
alone and you are stuck between two broken options: `> cursor_at` **skips every other row
that shares the cursor's timestamp** (a silent undercount), while `>= cursor_at` **re-reads
the whole timestamp bucket forever** (an infinite loop on a tie). Appending the unique `id`
makes the sort key *total*, so `>` advances past exactly the rows already seen and no
others.

And the comparison has to be a **row-value comparison**, not two `AND`ed scalar ones.
`(a, b) > (x, y)` means lexicographic order — `a > x`, OR (`a = x` AND `b > y`) — which is
not what `a > x AND b > y` says. The naive `AND` form requires *both* to be strictly
greater, so with the cursor at `(10:00:00, id=5)` a row at `(10:00:00, id=9)` evaluates
`created_at > 10:00:00` as false and is dropped entirely. Postgres executes the row-value
form against a composite index on `(created_at, id)` as a **single index range scan starting
at the cursor**, so each page costs O(page size) regardless of how deep into the table you
are — which is the performance half of the argument below.

**Alternative: `LIMIT/OFFSET` pagination.** `OFFSET N` makes Postgres scan and discard the
first `N` rows on every page, so the walk degrades to O(N²) over the table, and — worse — an
insert or delete during the walk *shifts the offset window*, so a row can be skipped or
double-counted. At a money boundary, double-counting a settlement is a fabricated surplus.
Keyset pagination reads from an index position, not a row count: an insert during the walk
either lands *behind* the cursor (already summed on this run, invisible now) or *ahead* of
it (picked up on this run or a later one). Because the reconcile job pages forward and
oldest-first, a concurrent insert is never double-counted — it lands past the cursor and is
simply seen once. This is the same keyset discipline as the M1 payments pagination lesson,
now applied to a summation rather than a user-facing list.

Note the termination condition. `for n == int(pageSize)` continues *only while the last
page was full*; a short (or empty) page means the last index range was exhausted, so the
table end is reached. A subtlety worth internalizing: if the table length is an exact
multiple of `pageSize`, the final full page is followed by one more query that returns
*zero* rows (`n = 0`), which then fails the loop condition. That one extra empty round-trip
is the price of not tracking a separate "is there more" flag — cheap and unambiguous.

**And the alternative nobody should skip past: one `GROUP BY` aggregate.** A single
`SELECT p.asset, s.status, SUM(p.amount) … GROUP BY p.asset, s.status` would produce exactly
these numbers, in one round-trip, with less code to get wrong. The keyset walk was chosen
here **deliberately as the exercise**, and that is worth stating rather than dressing up: if
all you ever need is the scalar sum, the grouped aggregate is the better engineering. The
cursor earns its place the moment the job needs to *stream* rows — to emit per-row
discrepancies, checkpoint progress across a long run, or bound memory over a table that
does not fit — and reconcile is the archetype of a job that grows those needs. The
transferable skill is the pattern; the honest note is that this instance does not yet need
it.

**A related "why not read the obvious number" worth recording.** `expected` is computed by
summing settlement *rows* rather than by reading the house clearing account's live balance —
even though that balance is right there and would be one query. The house balance fuses
`confirmed` and `finalized` value together and cannot isolate the finalized portion, and
that split is precisely what the bridge term in Decision B needs.

### Decision D: `int64` ledger, `*big.Int` chain, negate the liability sum in Go

**The problem.** Ledger amounts are `int64` minor units — that's the domain type
everywhere in this repo. On-chain balances are `uint256` — they *do not fit* an `int64` and
must be `*big.Int`. The report has to compare and combine both without losing precision or
overflowing.

**The chosen approach — keep each side in its natural type and bridge at the comparison.**
`AssetSums` carries `int64`; `AddressBalance.Actual` and every derived figure
(`ActualOnChain`, `Discrepancy`) is `*big.Int`. `BuildReport` lifts the `int64` sums into
`*big.Int` exactly where they meet the chain figures (`big.NewInt(s.FinalizedMinor)`), so
all money math happens in arbitrary precision. The liability query adds one more twist:

```sql
SUM(CASE WHEN el.direction = 'credit' THEN el.amount ELSE -el.amount END)
```
```go
liabilities[asset] = -sum
```

`SumNonHouseLiabilities` returns the *signed* sum of user-facing balances (credits minus
debits over every non-house account). A user account with a positive balance is money the
system *owes* — a liability — so the signed balance is *negative liability*. The Go caller
negates it once to turn "what users hold" into "what we owe them," a positive number the
proof-of-reserves inequality can compare against `actual`. Doing the negation in Go rather
than SQL keeps the query a plain signed balance sum (reusable, obvious) and puts the
domain-specific sign flip next to the comment that explains it.

**Where the sign comes from, so the negation isn't cargo cult.** Double-entry means that for
one asset every credit on one account is a debit on another, so all balances sum to zero.
Split them into the house settlement account and everyone else:

```
houseBalance + Σ(non-house balances) = 0
⟹  Σ(non-house balances) = −houseBalance
```

Users are net creditors of the system, so the signed non-house sum is *negative* — they are
owed. Liabilities is that magnitude expressed positively, hence exactly one negation. This
is the classic off-by-a-sign bug and it is invisible to code review, because both signs look
plausible on the page: **negate zero times and an undercollateralized treasury reports
healthy; negate twice and a healthy one reports a breach.** It can only be pinned by a test
with a concrete number, which is what the integration test does — it seeds a lone *debit* of
5000 on a non-house account (so the query returns `−5000`) and asserts
`row.LiabilitiesMinor == 5000`, proving the negation is present, applied once, and in the
right direction, end to end through real Postgres.

### Decision E: a stdlib-only `BalanceReader` port, and fail-closed decoding of untrusted bytes

**The problem.** The `balanceOf` result comes back from a chain node the process does not
control, decoding a value an attacker can influence — they control token contracts and
holder balances. And the caller (`internal/reconcile`, the CLI, the tests) should not have
to import go-ethereum just to ask "what is this address's balance?"

**The chosen approach — split the seam and quarantine the untrusted decoding.** The port
`chain.BalanceReader` is one method over **stdlib only** (`context`, `math/big`): no
go-ethereum, no address types, no RPC. The adapter `internal/chain/evm.BalanceReader` is
where every go-ethereum type and every fail-closed guard lives, so the untrusted-bytes
handling is confined to one file that one set of tests covers.

**The razor cut worth naming: the adapter did *not* get its own `contractCaller` seam.** It
reused the existing `ethRPC` interface by adding one method, `CallContract` — so the same
client that broadcasts payments now answers balance queries. A parallel seam would have
doubled the fakes and the wiring for no isolation benefit, since the two paths already share
a client and a redaction helper. **Add a method to the seam you have before you grow a
second seam.**

**And the registry that feeds it fails closed too.** Treasuries load from a JSON manifest
(`PAYMENT_RAIL_RECONCILE_TREASURIES`) following the same bare-top-level-array, fail-closed
convention as the denylist and the keyring, with a single-entry fallback derived from
`Chain*` config when the path is empty. The same razor pass folded away a `--format json` /
`WriteJSON` output (no consumer yet), an `UnexplainedSurplus` report field, and a parallel
proof-of-reserves slice — deferred, not designed out, but absent until something needs them.

## 3. Language deep-dive

### 3a. `runReconcile` returns `int`; `main` is a bare `os.Exit` passthrough

```go
func runReconcile(args []string) int {
	// ... fail-closed branches all: return reconcileExitError ...
	report, err := reconcileReport(ctx, db.New(sqlDB), br, reg, reconcilePageSize, time.Now())
	if err != nil {
		fmt.Fprintf(os.Stderr, "reconcile: %v\n", err)
		return reconcileExitError
	}
	report.WriteText(os.Stdout)
	logReconcileResult(ctx, logger, report, reg, time.Since(start))
	return reconcileExitCode(report.Clean)
}
```

Line by line, the instructive part is what the function *doesn't* do: it never calls
`os.Exit` itself. That's deliberate and idiomatic. `os.Exit` terminates the process
*immediately* — it does **not** run deferred functions. `runReconcile` has three live
defers at the point it returns: `stop()` (the signal-context cancel), `sqlDB.Close()`, and
`closeChain()`. If the function called `os.Exit` internally, all three would be skipped —
the DB pool and the RPC connection would leak on every run, and the signal handler would
never be unregistered. By *returning* an `int` and letting `main` do the single
`os.Exit(runReconcile(...))`, every defer fires during the normal return unwinding *before*
the process exits. This is the Go idiom "let `main` be the only place that calls
`os.Exit`," and it exists precisely because `os.Exit` and `defer` don't compose. Contrast a
language with no defer semantics (Java's `System.exit` also skips `finally`-via-shutdown
subtleties) — the hazard is the same, and the fix is the same: compute the code, return it,
exit once at the top.

`reconcileExitCode` is a two-line helper rather than an inline `if` because it names the
*clean/discrepancy* half of the tri-state and documents that code 2 is emitted elsewhere:

```go
func reconcileExitCode(clean bool) int {
	if clean {
		return reconcileExitClean
	}
	return reconcileExitDiscrepancy
}
```

Splitting the mapping out keeps the operational-error code (2) and the numeric-outcome
codes (0/1) provably disjoint in the source: 2 only ever appears next to a stderr print,
0/1 only ever come out of this pure function.

### 3b. `accumulate`: a map-of-struct fold and the value-copy-back idiom

```go
func accumulate(sums map[string]AssetSums, status, asset string, amount int64) {
	s := sums[asset]
	switch status {
	case "finalized":
		s.FinalizedMinor += amount
	case "settled":
		s.ConfirmedMinor += amount
	default:
		return
	}
	sums[asset] = s
}
```

Three Go facts are load-bearing here. First, `s := sums[asset]` on a **missing key returns
the zero value** — `AssetSums{0, 0}` — so a first-seen asset needs no initialization; the
fold just starts from zero. (In Java you'd get `null` and an NPE; in Python a `KeyError`.)
Second, and the one newcomers trip on: `s` is a **copy** of the struct, not a reference into
the map. Go map values are not addressable — you *cannot* write `sums[asset].FinalizedMinor
+= amount` (it's a compile error, because the map entry isn't a settable lvalue). So the
idiom is read-into-a-local, mutate the local, **write the local back** with `sums[asset] =
s`. Forgetting the final write-back is a classic silent bug: the increment happens on a
copy that's then discarded, and every sum stays zero with no error. Third, the `default:
return` means a status the query didn't filter out is *ignored* rather than misclassified —
but crucially it returns *before* the write-back, so an unknown status doesn't even touch
the map. Since the SQL already filters to `('settled','finalized')`, `default` is
defense-in-depth, not expected control flow.

### 3c. `BuildReport`: fresh `*big.Int` allocation and the mutate-vs-allocate split

```go
actual := big.NewInt(0)
for _, ab := range addrs {
	if ab.Actual != nil {
		actual.Add(actual, ab.Actual)
	}
}

expected := big.NewInt(s.FinalizedMinor)
expected.Add(expected, big.NewInt(s.ConfirmedMinor))
discrepancy := new(big.Int).Sub(actual, expected)
```

This is the M2 `*big.Int` discipline in a new setting, and the two different call shapes are
the lesson. `actual.Add(actual, ab.Actual)` deliberately mutates `actual` in place — it's a
*local accumulator we own*, initialized fresh with `big.NewInt(0)`, so mutating it is
correct and allocation-free per iteration. But `discrepancy := new(big.Int).Sub(actual,
expected)` allocates a **fresh** receiver: had it been written `actual.Sub(actual,
expected)`, it would have *overwritten* `actual` — the very value the report then stores in
`ActualOnChain` and prints. The rule from M2 restated: mutate a receiver you own and are
still accumulating into; allocate fresh when the operands must survive unchanged. Here
`actual` and `expected` both need to survive into the `AssetReconciliation` struct, so the
subtraction can't clobber either. The `if ab.Actual != nil` guard matters because a
`*big.Int` is a pointer whose zero value is `nil`, and `nil.Add` would panic — a treasury
address the reader returned no balance for is skipped, not crashed on.

`discrepancy.Sign()` (returns -1/0/+1) is used instead of comparing against
`big.NewInt(0)` — it's the allocation-free way to ask "is this zero / negative / positive"
of a `*big.Int`, and it reads as intent.

### 3d. The narrow querier seam: structural satisfaction and injecting the real DB

```go
type reconcileDBQuerier interface {
	ListSettlementsForReconcileFirstPage(ctx context.Context, limit int32) ([]db.ListSettlementsForReconcileFirstPageRow, error)
	ListSettlementsForReconcileAfter(ctx context.Context, arg db.ListSettlementsForReconcileAfterParams) ([]db.ListSettlementsForReconcileAfterRow, error)
	SumNonHouseLiabilities(ctx context.Context, asset string) (int64, error)
}
```

`reconcileReport` accepts this interface, not `*db.Queries`. The concrete `*db.Queries`
(from `db.New(sqlDB)`) satisfies it **structurally** — Go interfaces are satisfied
implicitly, so sqlc's generated type never mentions `reconcileDBQuerier` and yet fits it
because it has all three methods. This is the "accept interfaces, return structs" idiom: the
consumer declares the *narrow* set of methods it actually calls (three, out of the dozens
`*db.Queries` exposes), which both documents the command's real DB surface and lets the
integration test inject a real querier while faking only the chain. Note the seam is
declared *twice* with the same shape — once here in `cmd`, once as `settlementQuerier`
inside `internal/reconcile` with just the two paging methods. That's not redundancy to
DRY away: each package declares exactly the methods *it* calls, so `internal/reconcile`
(which never sums liabilities) doesn't drag `SumNonHouseLiabilities` into its interface.
The interface belongs to the consumer, so each consumer gets its own.

The same seam is what makes the integration test cheap. sqlc generates against a `DBTX`
interface that *both* `*sql.DB` and `*sql.Tx` satisfy, so `db.New(tx)` — a real querier bound
to a transaction the test rolls back in `t.Cleanup` — fits `reconcileDBQuerier` exactly as
well as `db.New(sqlDB)` does. The test drives the *same* `reconcileReport` function with a
real database and a fake `BalanceReader`, and leaves the database byte-identical. (Two more
isolation details worth stealing: every seeded row is keyed on a `"RECON-"+uuid` asset, and
the assertions are per-asset rather than on `report.Clean`, so unrelated residue in a shared
dev database can't flip the result.)

### 3e. The cursor's zero values, and injecting the clock

Two small things in `AggregateSettlements` and `reconcileReport` are worth naming because
both are Go answers to problems other languages solve with a sentinel.

```go
var cursorAt time.Time
var cursorID uuid.UUID
```

These are declared with no initializer, so they hold `time.Time{}` and the nil UUID. They
are only ever *read* inside `for n == int(pageSize)`, and that loop runs only if the first
page came back full — which means the first-page `range` already executed `pageSize` times
and overwrote both. So the zero values are never actually used as a cursor; they exist to
give the variables a scope wide enough to survive the loop. Coming from a language with
`undefined`/`null`, the instinct is a `*time.Time` sentinel; in Go the typed zero value plus
a "the loop cannot run before the assignment" invariant is cleaner and allocation-free.
(`cursorAt, cursorID = r.CreatedAt, r.ID` is a tuple assignment: both right-hand sides are
evaluated before either assignment, the same mechanism that makes `a, b = b, a` a swap with
no temporary.)

And the clock is a **parameter**, not a call:

```go
func reconcileReport(ctx context.Context, q reconcileDBQuerier, br chain.BalanceReader,
	reg reconcile.Registry, pageSize int32, now time.Time) (reconcile.Report, error)
```

`runReconcile` — the thin shell that parses flags, dials Postgres and the chain node, and
owns the exit code — passes `time.Now()`; the test passes a fixed instant so the report
timestamp is deterministic. That test helper is a neat one-liner worth knowing: `func
fixedNow() (t time.Time) { return }` uses a **named return value**, which is zero-initialized,
so a bare `return` yields `time.Time{}` without naming the type twice. Injecting the clock is
the standard Go answer to "how do I test code that stamps the current time," and it is the
same pure-core / thin-shell split as Decision A's exit code: process, network, and clock live
in the shell; the core is a function you can drive deterministically.

One more deliberate line in that function: `if _, ok := actuals[e.Asset]; !ok` seeds an empty
`[]AddressBalance{}` for every registry asset *before* any balance read, so `BuildReport`'s
key union produces a row for an asset even when the manifest lists no address for it. The
report never silently drops a treasury asset.

### 3f. Decoding attacker-influenced bytes: four guards, and `%s` as a security decision

```go
func (r *BalanceReader) BalanceOf(ctx context.Context, token, holder string) (*big.Int, error) {
	if !common.IsHexAddress(token) {
		return nil, fmt.Errorf("evm: token is not a valid address: %w", chain.ErrInvalidIntent)
	}
	if !common.IsHexAddress(holder) { /* ... same ... */ }
	tokenAddr := common.HexToAddress(token)

	msg := ethereum.CallMsg{To: &tokenAddr, Data: packERC20BalanceOf(common.HexToAddress(holder))}
	out, err := r.rpc.CallContract(ctx, msg, nil)
	if err != nil {
		return nil, fmt.Errorf("evm: balanceOf call failed: %s", RedactRPCError(err))
	}
	if len(out) < erc20WordLen { // 32
		return nil, fmt.Errorf("evm: balanceOf returned %d bytes, want at least %d (not a token contract?)",
			len(out), erc20WordLen)
	}
	return new(big.Int).SetBytes(out[:erc20WordLen]), nil
}
```

**Validate the addresses before the RPC**, because `common.HexToAddress` does not fail on
garbage — it pads or truncates to 20 bytes and hands back a wrong-but-plausible address.
The test pins the ordering by setting the fake RPC to return "CallContract must not be
reached" and asserting the error is still `errors.Is(err, chain.ErrInvalidIntent)`.

**Guard `len(out) < 32` before `SetBytes`**, because `big.Int.SetBytes` interprets *whatever
bytes you give it* as a big-endian unsigned integer and never errors. A call to an EOA or a
contract without `balanceOf` returns empty or short data; decode that and "this address is
not a token" silently becomes "this treasury holds 7 wei," which reconcile then reports as a
gigantic discrepancy. **Slice to exactly 32** (`out[:erc20WordLen]`) because an ABI encoder
can return more than one word; the all-`0xff` test confirms the full `2²⁵⁶−1` decodes without
truncation, which is also the concrete reason `Actual` is a `*big.Int` and not an `int64`.

**And note `%s` where the rest of the codebase writes `%w`.** A managed-node RPC URL carries
an API key in its path, and go-ethereum's `*url.Error` embeds that URL in its message.
`RedactRPCError` produces a redacted *string* — but re-wrapping it with `%w` around the
original error would leave the raw, key-bearing error reachable via `errors.Unwrap`,
re-leaking the secret to anyone who walks the chain. `%s` flattens to the redacted text and
**severs the chain on purpose**. The `%w` on the address guards is equally deliberate, since
callers do match `chain.ErrInvalidIntent`. Which verb to use is a security decision here,
not a style one.

## 4. What would break

- **A monitor that can't tell insolvency from an outage.** Collapse the outcome into
  `err→exit(1)` and a DB ping failure and a real reserve shortfall page the same runbook.
  The tri-state code keeps 2 (operational) disjoint from 1 (money wrong) by construction.

- **Leaked DB/RPC connections on exit.** Call `os.Exit` inside `runReconcile` and the three
  deferred `Close`/`stop` calls never run — a connection leak on every scheduled run.
  Returning the code and exiting once in `main` lets the defers fire first.

- **A fabricated surplus from OFFSET pagination.** Under concurrent inserts, `OFFSET` shifts
  the window and double-counts or skips a settlement — a phantom discrepancy or a hidden
  one. The `(created_at, id)` keyset walk reads from an index position, so a mid-scan insert
  lands past the cursor and is counted exactly once.

- **The silent zero-sum map bug.** Writing `sums[asset].FinalizedMinor += amount` doesn't
  compile (map values aren't addressable); the tempting "fix" of mutating a local and
  forgetting `sums[asset] = s` compiles and silently discards every increment. The
  read-mutate-write-back idiom is mandatory, not stylistic.

- **A `nil` `*big.Int` panic.** An address with no returned balance has `Actual == nil`;
  `actual.Add(actual, nil)` panics. The `if ab.Actual != nil` guard skips it. Likewise
  `BuildReport` seeds `actual` with `big.NewInt(0)` so it's never a `nil` receiver.

- **In-flight settlements flagged as discrepancies.** Compare `actual` against only the
  *finalized* sum and every confirmed-but-not-final settlement looks like a surplus. Adding
  `ConfirmedMinor` as a bridge term on the expected side nets those to zero.

- **A clean reconciliation hiding insolvency.** One comparison (`actual == expected`) can't
  ask the solvency question. Computing the proof-of-reserves verdict independently, and
  AND-ing it into `Clean`, means an undercollateralized asset flips the run to exit 1 even
  when its reconciliation is perfect.

- **An empty registry reconciling against nothing.** If `LoadRegistry` returned an empty
  set on a bad manifest, the command would read zero balances, find zero discrepancies, and
  report CLEAN — a false all-clear. It fails closed: any read/parse/validate/duplicate error
  returns a zero `Registry` and a non-nil error, so a broken manifest is exit 2, not a lie.

- **A cursor that skips rows sharing a timestamp.** `created_at` alone is not unique: `>`
  drops the cursor row's siblings (undercount, and therefore a phantom deficit), `>=` loops
  forever on a tie. The `(created_at, id)` tuple makes the ordering total. Writing the
  comparison as `created_at > $1 AND id > $2` instead of a row-value comparison reintroduces
  the skip, because a same-timestamp row fails the first conjunct.

- **"Not a token contract" decoded as a tiny balance.** `big.Int.SetBytes` never errors and
  never validates length; four bytes of junk become a small number. The `len(out) < 32`
  guard is the difference between an honest error and a fabricated discrepancy, and the
  `IsHexAddress` guard stops a malformed treasury address from being coerced into a
  plausible one before the call is even made.

- **An API key re-leaked through `errors.Unwrap`.** Redacting an RPC error and then wrapping
  the *original* with `%w` leaves the key-bearing error reachable by anyone walking the
  chain. The `%s` on `RedactRPCError(err)` severs it deliberately.

- **The sign flip applied zero or two times.** Negate zero times and an undercollateralized
  treasury reports healthy; negate twice and a healthy one reports a breach. Both compile,
  both read plausibly, and neither is catchable by review — only a test asserting a concrete
  `LiabilitiesMinor` against a known-signed sum pins it.

## 5. Compared to what you know

- **The tri-state exit code is `System.exit(2)` semantics, done right.** Any language can
  return a numeric process code; the Go-specific lesson is the *interaction with `defer`*.
  In Java you'd register shutdown hooks or rely on try-with-resources and return a code from
  `main`; in Go the equivalent discipline is "compute the code in a helper that returns
  `int`, call `os.Exit` exactly once at the top, so deferred cleanup runs first." The bug
  class — exit skipping cleanup — is universal; the idiom that avoids it is Go-flavored.

- **The keyset walk is cursor-based pagination**, familiar from any GraphQL `after:` cursor
  or a Stripe `starting_after`. The twist for a summation (rather than a UI list) is that
  correctness under concurrent writes isn't a nicety — it's the difference between a true and
  a fabricated financial total. `OFFSET` is `LIMIT ... OFFSET` in SQL everywhere and is
  wrong here for the same reason it's wrong for infinite scroll, just with money at stake.

- **`*big.Int` is Java `BigInteger` — but mutable**, exactly as the M2 lesson stressed.
  `BigInteger.subtract` returns a new object and can never clobber an operand;
  `(*big.Int).Sub(x, y)` writes into the *receiver*, so `x.Sub(x, y)` destroys `x`. The
  `new(big.Int).Sub(actual, expected)` here is the Go way to say "give me a fresh result and
  leave the operands alone" — the thing `BigInteger` does for free.

- **The narrow querier interface is dependency inversion / a port**, familiar from
  hexagonal architecture. The Go difference is implicit satisfaction: sqlc's `*db.Queries`
  fits `reconcileDBQuerier` without declaring it, and the interface is declared on the
  *consumer* side ("accept interfaces, return structs") — the inverse of Java, where the
  interface usually ships with the provider and the implementer says `implements`.

## 6. Gotchas & idioms

- **Map values aren't addressable.** `m[k].Field = x` and `m[k].Field += x` don't compile
  when the value is a struct. Read into a local, mutate, assign back. Relied on in
  `accumulate`.

- **Map zero value, not `KeyError`.** `sums[asset]` on a miss returns `AssetSums{}`; the
  fold starts from zero with no initialization. Same fact powers `unionKeys` seeding an
  empty `struct{}` set.

- **`os.Exit` skips `defer`.** Never call it from a function holding deferred cleanup. The
  whole reason `runReconcile` returns `int` instead of exiting.

- **`(*big.Int).Sign()` over `Cmp(big.NewInt(0))`.** `Sign()` returns -1/0/+1 with no
  allocation and reads as intent; use it for zero/negative/positive tests.

- **`new(big.Int)` vs `big.NewInt(0)`.** Both give a usable zero. `big.NewInt(n)` is the
  int64 constructor (used to *lift* ledger sums); `new(big.Int)` is the bare fresh receiver
  (used for results that immediately get `.Sub`/`.Set`). Interchangeable for zero, but the
  choice signals intent.

- **A `nil` `*big.Int` panics on any method.** Its zero value is `nil`, not `0`. Guard
  pointer balances (`if ab.Actual != nil`) or seed with `big.NewInt(0)` before accumulating.

- **`SumNonHouseLiabilities` is a *signed* balance; the sign flip lives in Go.** The query
  returns Σ(credit−debit); the caller negates it to get positive owed value. Don't push the
  negation into SQL — it belongs next to the domain comment that explains why.

- **stdout is the artifact, stderr is the structured log, `$?` is the machine verdict.**
  Amounts go to stdout; the amount-free `logReconcileResult` summary goes to stderr; the
  exit code carries clean/discrepancy/error. Three channels, three audiences.

- **`SetBytes` never errors and never bounds-checks *meaning*.** It is a pure byte→bignum
  reinterpretation. Every piece of validation — length, address format, "is this even a
  token?" — is your job, up front, before the call.

- **`%w` vs `%s` can be a security decision.** `%w` keeps the wrapped error reachable
  (right for sentinels callers match on); `%s` flattens it and severs the chain (right when
  the wrapped error's *text* is the thing you are protecting against). Pick by what the
  caller needs to recover, not by habit.

- **Named-return bare-return for "the zero of T".** `func fixedNow() (t time.Time) { return }`
  yields `time.Time{}` in one line without repeating the type. Easy to over-use; reserve it
  for exactly this case.

- **Inject the clock as a parameter.** `now time.Time` beats calling `time.Now()` inside,
  for the same reason the exit code is returned rather than exited: the deterministic core
  should not reach out to the process.

- **Roll back the test transaction *and* namespace the seeded rows.** `BeginTx` +
  `t.Cleanup(Rollback)` keeps the shared dev database byte-identical; keying every seeded
  row on a `"RECON-"+uuid` asset and asserting per-asset (not on `report.Clean`) keeps the
  case robust against residue other tests leave behind.

## 7. Check yourself

1. `runReconcile` returns an `int` and `main` does `os.Exit(runReconcile(...))`. Name the
   three deferred calls that would leak if `runReconcile` called `os.Exit` itself, and
   explain why returning the code fixes it.
2. A treasury's on-chain balance exactly equals its finalized settlement sum, but users are
   owed *more* than that. What are `Discrepancy` and `Verdict`, what is `Clean`, and which
   exit code results? Now flip it: an unexplained on-chain surplus, but liabilities well
   covered — same questions.
3. Why is `ConfirmedMinor` added to `expected` rather than treated as a discrepancy? What
   would the report show for every in-flight settlement if it weren't?
4. Rewrite `accumulate`'s body as `sums[asset].FinalizedMinor += amount`. Does it compile?
   If not, why, and what's the idiomatic fix?
5. The keyset loop is `for n == int(pageSize)`. Walk through the round-trips when the table
   has exactly `2 * pageSize` matching rows. How many queries run, and what does the last
   one return?
6. `discrepancy := new(big.Int).Sub(actual, expected)` allocates a fresh receiver. What
   exactly breaks in the printed report if you write `actual.Sub(actual, expected)` instead?
7. The cursor compares `(created_at, id) > ($1, $2)` rather than `created_at > $1 AND id >
   $2`. Construct a concrete two-row example where the `AND` form silently drops a row, and
   say what the dropped row does to the reported discrepancy.
8. `BalanceOf` wraps its address-validation errors with `%w` but its RPC error with `%s`.
   Explain why the `%s` is a security requirement rather than a stylistic choice, and what
   an attacker (or an over-helpful log aggregator) recovers if you "fix" it to `%w`.
9. Reconcile could compute `expected` with one `GROUP BY SUM` query instead of a keyset
   walk. Argue honestly for the aggregate, then name the concrete requirement that would
   flip the decision back to the cursor.

<details>
<summary>Answers</summary>

1. `stop()` (unregisters the signal handler / cancels the context), `sqlDB.Close()` (returns
   the DB pool), and `closeChain()` (closes the ethclient connection). `os.Exit` terminates
   the process without running deferred functions, so all three leak on every run. Returning
   the `int` lets the normal return unwind fire all three defers *before* `main` calls
   `os.Exit` once.
2. Case one: `Discrepancy == 0` (reconciled) but `Verdict == UNDERCOLLATERALIZED` (actual <
   liabilities), so `Clean == false` → exit 1. Case two: `Discrepancy != 0` (surplus) but
   `Verdict == OK`, so `Clean == false` → exit 1. Both prove the two checks are orthogonal:
   either one failing flips `Clean`.
3. Confirmed-but-not-final funds are already on-chain (they're in `actual`) but not yet
   finalized in the ledger. Adding them to `expected` as a bridge term nets a healthy
   in-flight treasury to `Discrepancy == 0`. Without it, `actual` would exceed the finalized
   sum by every confirmed-pending amount, so every in-flight settlement would show as a
   spurious positive discrepancy and flip the run to not-clean.
4. It does **not** compile: map index expressions yield non-addressable values, so you can't
   take the address implied by `+=` on `sums[asset].FinalizedMinor`. Idiomatic fix: `s :=
   sums[asset]; s.FinalizedMinor += amount; sums[asset] = s` — read, mutate the local, write
   back.
5. Query 1 = first page (`pageSize` rows, `n == pageSize`, loop entered). Query 2 = second
   page (`pageSize` rows, `n == pageSize`, loop continues). Query 3 = returns **0** rows
   (`n == 0`), loop condition fails, walk ends. Three queries; the last returns an empty
   page. The extra empty round-trip is the cost of using a short page as the terminator.
6. `actual.Sub(actual, expected)` overwrites `actual` in place with the difference, but
   `actual` is stored in `ActualOnChain` and printed as "actual on-chain." The report would
   show the *discrepancy* value in the on-chain-balance field — corrupting the very number
   the reader trusts. Allocating a fresh receiver leaves `actual` and `expected` intact for
   the struct.
7. Cursor at `(10:00:00, id=5)`; the next row is `(10:00:00, id=9)` — same millisecond,
   higher id. The row-value form takes the tie branch (`created_at = x AND id > y`) and
   returns it. The `AND` form evaluates `created_at > 10:00:00` as **false** and drops it
   entirely, along with every other row sharing that timestamp. Those settlements are never
   summed into `expected`, so `expected` comes out too small and the report shows a
   fabricated *surplus* discrepancy — exactly the failure mode a reconciliation job exists to
   rule out.
8. A managed-node RPC URL carries the API key in its path, and go-ethereum's `*url.Error`
   embeds the full URL in its message. `RedactRPCError` reduces it to `scheme://host`, but
   `%w` around the *original* error would leave that original reachable through
   `errors.Unwrap` — so any later `%v`, any `errors.As` walk, or any log aggregator that
   renders the cause chain prints the key in full. `%s` flattens the redacted text and
   severs the chain, which is the whole point. The address guards keep `%w` because callers
   legitimately match `chain.ErrInvalidIntent` and the sentinel carries no secret.
9. For the current requirement — a scalar per-(asset, status) sum — the aggregate wins on
   every axis: one round-trip instead of N, no cursor to get wrong (no tuple comparison, no
   tiebreaker, no termination condition), and the correctness-under-concurrent-inserts
   argument becomes moot because the whole sum is one snapshot. The keyset walk is chosen
   here as the exercise, and that should be said out loud rather than rationalized. It flips
   back the moment the job must *stream*: emitting a per-row discrepancy line, checkpointing
   progress so a long run can resume, or bounding memory over a table that will not fit —
   none of which a single `SUM` can do.

</details>

## 8. Further reading

- [Go blog — Defer, Panic, and Recover](https://go.dev/blog/defer-panic-and-recover) — the
  defer semantics that make "return the code, `os.Exit` once in `main`" the correct shape.
- [`os.Exit` docs](https://pkg.go.dev/os#Exit) — note explicitly: "the program terminates
  immediately; deferred functions are not run."
- [`math/big` package docs](https://pkg.go.dev/math/big#Int) — receiver-mutating methods;
  read `Sub`, `Add`, `Sign`, and the `new(big.Int)` vs `big.NewInt` constructors.
- [Use the Index, Luke — No Offset](https://use-the-index-luke.com/no-offset) — why keyset
  pagination beats `OFFSET` for both performance and correctness under concurrent writes.
- [Effective Go — Interfaces and methods](https://go.dev/doc/effective_go#interfaces_and_methods) —
  implicit satisfaction and the "accept interfaces" idiom behind the narrow querier seam.
- [PostgreSQL — Row and Array Comparisons](https://www.postgresql.org/docs/current/functions-comparisons.html#ROW-WISE-COMPARISON)
  — the `ROW(a,b) > ROW(x,y)` lexicographic semantics that make the keyset tiebreaker work,
  and why the `AND` form is not the same thing.
- [Go blog — Working with Errors in Go 1.13](https://go.dev/blog/go1.13-errors) — `%w` versus
  `%s`, and the "when to deliberately break the chain" judgment behind the redacted RPC error.
