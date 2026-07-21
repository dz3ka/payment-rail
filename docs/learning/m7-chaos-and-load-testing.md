# M7 — Reliability evidence: injecting faults into the *real* money path in-process, and a stdlib-only load harness that measures the one number that matters

> Scope: this lesson is about *producing evidence that the system is correct under
> failure and characterized under load* — without a distributed test rig. Two headline
> achievements. (1) An **in-process chaos suite** (`internal/chaos`, `//go:build chaos`)
> that drives the *real* ledger/settlement code through injected mid-transaction faults —
> crash-before-commit, a server-side connection kill, a broken RPC broadcast, and a
> scripted reorg — and gates every scenario on one invariant: **the ledger converges**
> (closed double-entry sum + a clean reconcile). The faults are injected through the same
> `ledger.Store`/`ethRPC` seams M1 and M2 built, so a `faultStore` is a drop-in that fails
> exactly at the commit boundary and nowhere else, and every scenario is *synchronous* —
> no goroutines, no sleeps — so it's deterministic under `-race`. (2) A **stdlib-only load
> harness** (`cmd/paymentrailctl/loadtest.go`) whose `runLoad` fans out to N workers that
> each own a private latency slice (no lock on the hot path), then merges and reads
> nearest-rank percentiles — and a published benchmark that is honest about measuring
> **serialized double-entry commit throughput on one untuned Postgres**, not a headline
> number. This builds on M1's transactor seam (`ExecTx`/`PostWithin`) and M2's `ethRPC`
> port; if the `{check → act → commit}` critical-section shape and go-ethereum's
> `simulated` backend aren't reflexive yet, skim `m1-double-entry-ledger.md` and
> `m2-evm-chain-adapter.md` first.

## 1. What we built

M7 is the reliability-evidence milestone: it adds no product feature, it *proves things
about the features already shipped*. The chaos suite lives in `internal/chaos`, behind a
`//go:build chaos` tag and a `PAYMENT_RAIL_TEST_DSN` gate, so it never runs in the normal
`go test ./...` pass — it's opt-in, and it needs a real Postgres because the whole point
is to assert the *database's* atomicity, not a mock's. Five files carry it: `harness_test.go`
(the shared DB gate, asset-isolated seeders, and the convergence assertions), `faults_test.go`
(the `faultStore` fault injector), and four scenario files — `crash_test.go`,
`dbfailover_test.go`, `brokenrpc_test.go`, `reorg_test.go` — each of which drives a real
code path (`ledger.PostWithin`, `settlement.Sink.OnStatus`, `evm.Adapter.Submit`) straight
into a failure and then asserts recovery converges.

The design spine is that **faults are injected at existing seams, not bolted on**. M1 made
the ledger depend on a `ledger.Store` transactor interface (`ExecTx(ctx, fn)`); M2 made the
EVM adapter depend on an `ethRPC` interface. Because those seams exist, the chaos suite
supplies alternate implementations — a `faultStore` that runs the real transaction body and
then rolls back *instead of committing*, and a `brokenSendRPC` that shadows only
`SendTransaction` — and drives the *unmodified* production logic through them. No source
code changed to make it testable; the ports were already there. The one fault the suite
*couldn't* fake through a seam — a genuine connection death — it drives for real, with
`pg_terminate_backend` against a throwaway pool, because a pool `Close()` does not abort an
already-checked-out transaction (a subtle, empirically-verified trap documented in a code
comment).

The load harness is a separate deliverable: a `paymentrailctl loadtest` subcommand,
stdlib-only, that seeds funded accounts over the DSN, drives `POST /v1/payments` under
sustained concurrency, and prints a throughput + latency report. `docs/benchmark.md`
publishes a real run — ~568 req/s, plateauing as concurrency rises from 32 to 50 — and is
scrupulous about what that means: it's a single-host, untuned-Postgres, create-only number
whose bottleneck is *commit throughput* (each create is one fsync-bounded transaction that
also writes an outbox and an audit row). The part worth studying is `runLoad`: how it
measures latency without the measurement apparatus distorting the measurement.

## 2. The design decision

### Decision A: assert *convergence*, not *steps* — the invariant is the test oracle

**The problem.** A fault-injection test needs an oracle: after chaos, what do you assert?
The naive answer is to assert the specific intermediate states ("after the crash, row X is
absent; after retry, column Y equals Z"). That works but it's brittle and, worse, it doesn't
actually test the *property you care about* — that the books balance. A reorg scenario that
bounces a settlement in and out of the canonical chain three times produces a thicket of
journal entries (three settles, two reversals); enumerating each one is a transcription
exercise, and getting the enumeration wrong gives you a test that passes for the wrong reason.

**The chosen approach — a single `assertConverged` gate built from the system's own
closed-world invariants.** The suite defines convergence as two independent checks
(`harness_test.go`):

```go
func assertConverged(ctx context.Context, t *testing.T, dbh *sql.DB, asset string, actualOnChain int64) {
	t.Helper()
	assertLedgerClosed(ctx, t, dbh, asset)                 // Σ(credit − debit) over real entries == 0
	assertReconcileClean(ctx, t, dbh, asset, actualOnChain) // proof-of-reserves discrepancy == 0
}
```

`assertLedgerClosed` runs `SELECT SUM(CASE WHEN direction='credit' THEN amount ELSE -amount END)`
over every *non-opening* entry line for the asset and asserts it nets to zero. That's the
double-entry invariant itself: if any fault left a half-posted entry (a debit without its
credit, or a settle without its counterparty release), the sum is nonzero and the test fails
— no matter *which* entry broke. `assertReconcileClean` goes further and drives the *real*
reconcile core (`AggregateSettlements` + `SumNonHouseLiabilities` → `BuildReport`, the exact
path M6's `runReconcile` uses) and asserts the proof-of-reserves discrepancy is zero. Two
orthogonal oracles: one internal (the books balance), one external (settled liabilities match
the on-chain amount).

The payoff is that the *same* three-line gate validates a crash, a connection death, a broken
broadcast, and a triple-reorg. The reorg test can bounce the tx through Confirmed→Reorged→
Confirmed(B)→Reorged→Confirmed(C) — accumulating three settle entries and two reversals — and
still assert convergence with one call, because the invariant doesn't care about the *count*
of entries, only that they *net* correctly. It does *also* spot-check the counts
(`assertSettleEntryCount`, `settlementReversalCount`) to catch a "converged by luck" false
pass, but the load-bearing oracle is the sum.

**Alternative 1: assert exact intermediate states everywhere.** Brittle and blind. It couples
the test to the current entry-posting scheme (rename a `kind`, break fifty assertions) and, by
enumerating expected rows, it can't catch a *novel* imbalance the author didn't think to
enumerate. The invariant catches any imbalance by construction.

**Alternative 2: assert only balances of the specific accounts touched.** Better, but it
misses entries that balance *pairwise* while corrupting a third account, and it doesn't test
the external reconcile view at all. The closed-ledger sum is account-agnostic; the reconcile
check is the external cross-check. Together they're the full oracle.

### Decision B: inject the fault at the transactor seam, and fail *only* the commit

**The problem.** To test "crash after writes, before commit," you need to run the *real*
transaction body — the same `PostWithin` + `InsertPayment` the production `payments.Service`
runs — and then die at exactly the commit point, not before (or you've tested nothing) and not
after (or you've tested a no-op). And you must not fork the production code to do it.

**The chosen approach — `faultStore`, a `ledger.Store` that is byte-identical to the real
`SQLStore` up to the commit, then diverges.** `faults_test.go`:

```go
var _ ledger.Store = (*faultStore)(nil) // compile-time drop-in proof

func (s *faultStore) ExecTx(ctx context.Context, fn func(q db.Querier) error) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("chaos: begin tx: %w", err)
	}
	if err := fn(db.New(tx)); err != nil {          // real body; real rollback-on-error
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return errors.Join(err, fmt.Errorf("chaos: rollback: %w", rbErr))
		}
		return err
	}
	switch s.mode {                                  // fn SUCCEEDED — diverge here
	case faultCrashBeforeCommit:
		_ = tx.Rollback()                            // discard the applied work
		return errInjectedCrash
	}
	panic("...")
}
```

The critical property: a fault **only ever changes the success path**. Up to the point `fn`
returns nil, this is a line-for-line copy of production `SQLStore.ExecTx` — same isolation
level, same `errors.Join` on a bad rollback, same `sql.ErrTxDone` tolerance. Only when the
body *succeeds* — the moment production would `COMMIT` — does the fault fire: it `Rollback()`s
the applied-but-uncommitted work and returns `errInjectedCrash`. That models process death in
the window between "all writes applied in the transaction" and "commit acknowledged," which is
the single most dangerous window in any transactional system. The scenario then asserts on the
*real pool* that no row survived (`paymentCount == 0`, balances untouched) and that re-running
the identical `txBody` through a *real* store converges. Because the fault store and the real
store share the seam, "retry after crash" is literally the same closure passed to a different
`Store`.

**Alternative 1: a mock DB that pretends to fail.** Then you're testing your mock's fidelity,
not Postgres's atomicity. The whole value of this suite is that the rollback is *real* — it's
Postgres actually discarding the work — so the mock is disqualified.

**Alternative 2: add a `crashHere bool` to production `ExecTx`.** Poisons production code with
test scaffolding, and every branch is a place a real bug can hide. The seam already lets you
substitute behavior from outside; use it.

**The honest shortcut, stated.** `payments.Service.Create` builds its own `SQLStore` internally
and is *not* injectable, so the crash and failover scenarios can't drive `Create` directly —
they reconstruct its transaction body inline (`PostWithin` + `InsertPayment`, "faithful to
payments.go:103-140" per the comment) and run *that* through the `faultStore`. This is a real
fidelity gap: if `Create`'s body drifts, the chaos copy won't. The production-grade fix is to
make `Create` accept an injected `ledger.Store` (constructor injection), which would let the
scenario drive the *actual* method. The suite documents the gap rather than hiding it, and it
does exercise the real `settlement.Sink` and real `evm.Adapter` directly (both *are* injectable),
so only the payment-create body is reconstructed.

### Decision C: a pool `Close()` is not a connection death — kill the backend server-side

**The problem.** The DB-failover scenario wants a *genuine* mid-transaction connection death,
not a simulated one. The obvious move — open a pool, begin a tx, `sql.DB.Close()` the pool
before commit — is *wrong*, and wrong in a way that would give a false green.

**The chosen approach — pin one connection, read its backend PID, and terminate it from a
*different* pool with `pg_terminate_backend`.** `dbfailover_test.go`:

```go
conn, _ := doomed.Conn(ctx)                                   // pin ONE physical connection
var backendPID int
conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&backendPID)

tx, _ := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
txBody(db.New(tx))                                            // apply writes, do NOT commit

dbh.ExecContext(ctx, `SELECT pg_terminate_backend($1)`, backendPID) // kill it from the surviving pool
if err := tx.Commit(); err == nil {
	t.Fatal("COMMIT over a terminated connection = nil error, want it to fail")
}
```

The comment in `faults_test.go` records *why* the seemingly simpler pool-close approach was
removed: "closing the `*sql.DB` pool does NOT abort the transaction's already-checked-out
connection — the COMMIT still SUCCEEDS and the write PERSISTS (verified empirically)." A
`database/sql` pool is a manager of connections; `Close()` marks it closed and stops handing
out *new* connections, but a connection already checked out for an open `tx` keeps its socket
and commits fine. To actually sever an in-flight transaction you have to kill the *server-side
backend process* serving it — `pg_terminate_backend` does that — after which the doomed
`COMMIT` fails and Postgres rolls the transaction back. The scenario then proves on the
*surviving* pool that nothing persisted.

The structural care here is worth naming: the doomed transaction runs on its **own throwaway
pool** (a second `sql.Open` on the same DSN), and every assertion reads through the *surviving*
harness pool. Killing a backend on the doomed pool can't disturb the pool the test observes
through — the two pools share a database, not a connection.

### Decision D: shadow-embed the RPC so only `SendTransaction` breaks, and count real broadcasts

**The problem.** The broken-RPC scenario needs the *entire* Submit path to run for real —
build calldata, price gas, allocate a nonce, sign — but with a broadcast that fails on demand
and heals on demand, so it can prove a failed broadcast doesn't burn a nonce. Faking the whole
`ethRPC` interface would mean re-implementing six methods; faking none means you can't inject
the failure.

**The chosen approach — struct embedding to inherit the real backend, then override one
method.** `brokenrpc_test.go`:

```go
type brokenSendRPC struct {
	simulated.Client // embedded: promotes all six ethRPC methods for free
	mu       sync.Mutex
	failSend bool
	sendErr  error
	sent     int
}

func (r *brokenSendRPC) SendTransaction(_ context.Context, _ *types.Transaction) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failSend {
		return r.sendErr
	}
	r.sent++      // count a "successful" send WITHOUT touching the real chain
	return nil
}
```

Embedding `simulated.Client` means `brokenSendRPC` automatically satisfies `ethRPC` — the
`ChainID`, `PendingNonceAt`, `EstimateGas`, `SuggestGasTipCap`, `HeaderByNumber`, `CallContract`
calls are *promoted* from the embedded field and hit the real in-memory backend. Only
`SendTransaction` is *shadowed* by the outer method, and it deliberately does *not* delegate to
the embedded backend even on success — it just increments `sent`. That's a double win: the
counter is the "broadcast exactly once" probe, and *not* forwarding keeps the arbitrary
test-signed transaction off the simulated chain (which would reject it for unrelated reasons).
The scenario toggles `failSend`, submits (expecting `chain.ErrBroadcast`), heals, resubmits,
and asserts the `recordingSigner` was asked to sign the *same nonce both times* — the concrete
proof of M2's commit-on-success nonce discipline under a real broadcast failure.

**Alternative: hand-roll a full `ethRPC` fake.** Six methods of boilerplate, each a chance to
diverge from real backend semantics (gas estimation especially). Embedding gets five-and-a-half
methods of real behavior for free and lets the test override exactly the one it cares about.
This is the idiomatic Go answer to "I want *this* type's behavior except for one method."

### Decision E: stdlib-only load harness, per-worker slices, nearest-rank percentiles

**The problem.** Measure the latency distribution of `POST /v1/payments` under concurrency
without the measurement machinery becoming the bottleneck — and without adding a load-testing
dependency (the repo is deliberately stdlib-first).

**The chosen approach — N workers, each owning a private latency slice and outcome tally,
merged once at the end.** `loadtest.go`:

```go
results := make([]workerResult, cfg.Concurrency)
var wg sync.WaitGroup
for i := range results {
	wg.Add(1)
	go func(wr *workerResult) {
		defer wg.Done()
		for {
			if runCtx.Err() != nil { return }
			if cfg.Requests > 0 {
				if atomic.AddInt64(&remaining, -1) < 0 { return } // claim one slot
			}
			t0 := time.Now()
			oc := op(runCtx)
			if runCtx.Err() != nil { return }          // discard the deadline-truncated sample
			wr.lats = append(wr.lats, time.Since(t0))
			wr.tally[oc]++
		}
	}(&results[i])
}
wg.Wait()
```

The central decision is that **nothing on the hot path takes a shared lock**. Each worker
appends to *its own* `wr.lats` and increments *its own* `wr.tally`; the slices are disjoint, so
there's no contention and no false sharing of a mutex. A shared histogram guarded by a `Mutex`
would serialize every request behind the very lock you're trying to measure around —
Heisenberg-ing the latency. Only after `wg.Wait()` are the per-worker slices concatenated,
sorted *once*, and percentiles read by nearest-rank. Two subtler decisions: the request budget
is a single `atomic.AddInt64` (a lock-free claim, so "exactly N requests" holds regardless of
concurrency), and the *deadline-truncated in-flight sample is discarded* — when the run ends
mid-request, that sample's latency is a cancellation artifact (a truncated duration plus a
spurious `TransportError` from the aborted `client.Do`), so recording it would pollute both the
tail latency and the error tally.

Nearest-rank (not interpolation) is deliberate: "every reported latency is a value that
actually occurred," which is what a load report should claim.

**Alternative 1: a third-party load tool (k6, vegeta, wrk).** More features, but a dependency
and an external binary in the reproduce steps — and it wouldn't seed the ledger accounts the
create path needs. A ~150-line stdlib harness that also owns seeding is more self-contained for
a portfolio repo.

**Alternative 2: HDR histogram / t-digest for percentiles.** Right answer at scale (bounded
memory, mergeable), but for a run of tens of thousands of samples, "collect all, sort once,
index" is simpler and exact. The harness picks exact-and-simple over approximate-and-scalable,
which is the correct trade at this size.

### Decision F: publish an *honest* benchmark — name the bottleneck, not the headline

`docs/benchmark.md` reports ~568 req/s and then spends most of its words on *what that number
is not*. It states plainly that `POST /v1/payments` is a pure-Postgres path (no chain/signer/
Kafka on the request path), so the benchmark measures **serialized double-entry commit
throughput** with two extra row writes (outbox + audit) riding each commit. It shows the number
plateauing (567.9 → 568.0 req/s) as concurrency rises 32 → 50 while latency climbs across the
board — the textbook signature of a saturated bottleneck (past the knee, concurrency buys
latency, not throughput) — and cross-checks it against Little's Law. And it enumerates the
limitations (single-host loopback, untuned Postgres, create-only, `amount=1`). This is the
teaching point: a benchmark's *value* is in its stated methodology and honest bottleneck
attribution, not its top-line figure. A number without "here's exactly what saturated" is
marketing.

## 3. Language deep-dive

### 3a. Build tags and the `_test.go` + tag combination — a whole opt-in package

Every chaos file opens with:

```go
//go:build chaos

package chaos
```

The `//go:build chaos` line (with a mandatory blank line after it, before `package`) is a
*build constraint*: the file compiles only when `go test -tags=chaos` is invoked. Combined with
the `_test.go` suffix — which already restricts a file to test builds — these files are
double-gated: they're test-only *and* tag-only. So `go test ./...` (the normal CI pass) sees an
effectively empty `chaos` package and skips it entirely; you opt in with `-tags=chaos`, and even
then `requireChaosDB` skips at runtime unless `PAYMENT_RAIL_TEST_DSN` is set. Three gates,
escalating in cost: compile-time tag, test-only suffix, runtime DSN. This is Go's idiomatic way
to keep an expensive, infrastructure-dependent suite in the tree without slowing the fast path.
For a TypeScript/Java engineer: it's like a test tagged `@Tag("chaos")` and excluded from the
default surefire/jest run, except the exclusion happens at *compilation*, so the heavy imports
(`simulated`, `lib/pq`) don't even get built in the normal pass.

### 3b. Struct embedding is composition-with-promotion, not inheritance

`brokenSendRPC` embeds `simulated.Client` as an *anonymous field*:

```go
type brokenSendRPC struct {
	simulated.Client        // no field name — this is embedding
	mu sync.Mutex
	// ...
}
```

Because the field is anonymous, all of `simulated.Client`'s methods are *promoted* to
`brokenSendRPC`: calling `r.PendingNonceAt(...)` dispatches to `r.Client.PendingNonceAt(...)`
transparently. When `brokenSendRPC` also declares its *own* `SendTransaction`, that method
**shadows** the promoted one — Go's method resolution prefers the outer type's own method over a
promoted one at the same depth. The net effect: `brokenSendRPC` satisfies the six-method
`ethRPC` interface with five methods it inherited and one it overrode. This looks like
inheritance but it is *not*: there's no `super`, no vtable relationship, no "is-a." The embedded
`Client` is just a field with a blank name, and promotion is pure syntactic sugar for
`r.Client.Method()`. The gotcha this avoids: the overriding `SendTransaction` cannot accidentally
call "the parent" — if it wanted to delegate it would write `r.Client.SendTransaction(...)`
explicitly, and here it deliberately *doesn't*, keeping the test tx off the real chain. This is
the "accept interfaces, embed for reuse" idiom: composition that reads like inheritance but
keeps the seams explicit.

### 3c. The transactor closure: `ExecTx(ctx, fn)` and rollback-on-panic-vs-error

The `faultStore.ExecTx` and its production twin both take `fn func(q db.Querier) error` and run
it *inside* a transaction — the same "caller-supplied critical section" shape M2's `withNonce`
used, applied to a DB transaction:

```go
if err := fn(db.New(tx)); err != nil {
	if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
		return errors.Join(err, fmt.Errorf("chaos: rollback: %w", rbErr))
	}
	return err
}
```

Line by line: `db.New(tx)` wraps the `*sql.Tx` in the generated `Querier` so `fn` speaks the same
query API whether it's handed a pool, a tx, or a fault store — that's what makes the *same*
`txBody` closure runnable through both `faultStore` and the real `SQLStore`. On error, it rolls
back and returns the *original* error, but note the `sql.ErrTxDone` tolerance: if the transaction
is already finished (e.g. the context was canceled and the driver auto-rolled-back), `Rollback()`
returns `sql.ErrTxDone`, which is *expected*, not a new failure — so it's filtered with
`errors.Is`. Any *other* rollback failure is a genuine second problem, so it's `errors.Join`ed
onto the first — Go 1.20+'s way of returning *two* errors as one, both discoverable later via
`errors.Is`. This is the multi-error idiom: don't drop the rollback failure, don't let it mask the
original, carry both. For a Java engineer: `errors.Join` is roughly `addSuppressed`, but it's a
first-class value you build explicitly rather than a side channel on a thrown exception.

### 3d. `sql.NullString` and Go's answer to nullable columns

The reorg scenario reads a column that is `NULL` while a settlement is pending or reorged, and a
block hash once settled:

```go
var bh sql.NullString
if err := dbh.QueryRowContext(ctx,
	`SELECT settled_block_hash FROM settlements WHERE tx_hash = $1`, txHash,
).Scan(&bh); err != nil { /* ... */ }
return bh.String, bh.Valid
```

Go has no nullable primitives — a `string` can't be `nil`, its zero value is `""`, and `""` is a
*legitimate value*, so you can't use it to mean "absent." `sql.NullString` is the stdlib's
discriminated-union-by-struct: `{String string; Valid bool}`. After `Scan`, `Valid` is `false`
for a SQL `NULL` and `true` otherwise; `String` is only meaningful when `Valid`. The test returns
`(bh.String, bh.Valid)` and the caller checks the bool — so `!ok` distinguishes "reorged, anchor
NULLed" from "settled to the empty string," a distinction a bare `string` would erase. This is Go
declining `Option<String>` (Rust) or `String?` (Kotlin) as a language feature and providing it as
a library struct per scalar type (`NullInt64`, `NullTime`, ...). It's more verbose than a real
sum type, but it's explicit at the exact boundary where SQL nullability leaks into Go.

### 3e. `atomic.AddInt64` as a lock-free work-claim

The load loop's request budget is claimed without a mutex:

```go
remaining := int64(cfg.Requests)
// ...inside each worker...
if atomic.AddInt64(&remaining, -1) < 0 {
	return // budget spent
}
```

`atomic.AddInt64(&remaining, -1)` atomically decrements the shared counter and returns the *new*
value in one indivisible hardware operation (a `LOCK XADD` on x86). Because the decrement and the
read are atomic, no two workers can both see the "last" slot: exactly one worker gets the
transition to `0`, and every worker that pushes it negative returns without doing work. That's how
"exactly `cfg.Requests` ops in total across all workers" holds under any concurrency, with zero
locking. Contrast a `mu.Lock(); remaining--; got := remaining; mu.Unlock()` — correct but it
serializes the claim, and on a hot loop that's exactly the contention the per-worker-slice design
was avoiding elsewhere. The subtle correctness point: the counter is allowed to go *negative*
(each over-budget worker decrements once more before checking), which is fine — the sign is the
signal, and the magnitude of the overshoot is bounded by the worker count. This is the canonical
Go idiom for "distribute a fixed amount of work across goroutines without a coordinator."

## 4. What would break

- **A false green from a pool-close "fault."** `sql.DB.Close()` before COMMIT does *not* abort the
  in-flight transaction — the commit succeeds and the write persists — so a failover test built on
  it would assert "no partial state" against a DB that *did* persist the write, and pass anyway
  (testing nothing). Avoided by `pg_terminate_backend` against the real backend PID; the comment in
  `faults_test.go` forbids reintroducing the pool-close mode.

- **A burned nonce wedging a sender.** If a failed broadcast advanced the nonce high-water, every
  later tx for that sender would stall behind the hole. The broken-RPC scenario proves it doesn't:
  the `recordingSigner` records `{before, before}` — the retry reuses the exact nonce — and the
  chain's pending nonce never advances. This is M2's commit-on-success discipline, now proven under
  a *real* injected broadcast failure rather than a unit mock.

- **A double-settle on redelivery.** At-least-once delivery (a watcher that log-and-continues on a
  transient RPC error, then re-emits the same Confirmed status) would double-credit the house
  account if settle weren't idempotent. The broken-RPC and crash scenarios deliver the *same*
  status twice and assert exactly one `settlement.settle` entry (the `settle:<pid>:<blockHash>`
  external-ref guard holds) and no second debit.

- **A reorg netting to the wrong balance.** Bouncing a tx Confirmed→Reorged→Confirmed accumulates
  multiple settle *and* reversal entries; if any didn't net, the provisional credit would leak or
  double. `assertConverged` catches any net imbalance regardless of how many entries exist, and the
  restart subtest proves a *fresh* Sink resolves the persisted row by `tx_hash` alone (no in-memory
  carryover) — the process-death recovery path.

- **Measurement distortion in the load harness.** A shared mutex on the latency histogram would
  serialize the hot path and inflate the very latencies being measured; recording the
  deadline-truncated in-flight sample would inject a spurious `TransportError` and a truncated tail
  reading. Avoided by per-worker slices merged once, and by discarding the sample when `runCtx.Err()`
  is non-nil after the op returns.

- **A non-deterministic chaos suite under `-race`.** If faults were driven by timers/goroutines/
  sleeps racing the assertions, the suite would flake. Every scenario drives statuses *synchronously*
  via direct `Sink.OnStatus`/`Submit` calls with explicit block hashes — "no watcher Run loop, no
  timers, no goroutines" — so it's deterministic. The reorg *detection* is unit-tested elsewhere; the
  chaos suite tests only the *ledger-side* convergence, hand-feeding the statuses the watcher would emit.

## 5. Compared to what you know

- **The chaos suite is Jepsen-in-miniature, in-process.** Jepsen (Clojure) injects partitions/pauses
  into a real distributed system and checks a consistency model with a linearizability checker. This
  is the same *idea* — inject a fault into the real path, check an invariant — scoped down to a single
  process against a real Postgres, with the invariant being double-entry closure rather than
  linearizability. The oracle-not-steps philosophy is identical.

- **`assertConverged` is a property-based oracle, not an example-based assertion.** If you've used
  jqwik/QuickCheck/fast-check, the mindset transfers: instead of "given this input, expect this exact
  output," you assert a *property that must always hold* ("the books balance"). Here the "inputs" are
  fault sequences and the property is convergence. The difference from classic PBT is the inputs are
  hand-scripted scenarios, not generated — but the assertion is a property, which is why one gate
  covers wildly different fault paths.

- **`faultStore` is a test double at a port — the Ports & Adapters payoff, again.** It's the same move
  as substituting a fake repository in a hexagonal Java/TS app, except Go's *implicit* interface
  satisfaction means `faultStore` never declares `implements ledger.Store` — it just has the method,
  and `var _ ledger.Store = (*faultStore)(nil)` is a *voluntary* compile assertion. The interface was
  declared on the *consumer* side (M1's ledger), so a test in a *different package* can implement it
  without touching the original.

- **Struct embedding vs. inheritance.** Closest mainstream analogy is TypeScript's mixins or Kotlin's
  class delegation (`class Foo(b: Bar) : Bar by b`) — you get the delegatee's methods and can override
  one. It is *not* Java `extends`: there's no protected members, no constructor chaining, no `super`.
  The delegation is to a plain field, and "override" is just method shadowing. The analogy to `extends`
  breaks down the moment you reach for `super` — Go makes you name the field (`r.Client.Method()`).

- **`errors.Join` is `Throwable.addSuppressed`, as a value.** Java attaches suppressed exceptions as a
  side channel on the primary throwable; Go builds a multi-error *value* you return normally, and both
  members are later matchable with `errors.Is`. No stack unwinding involved — it's just data.

## 6. Gotchas & idioms

- **The blank line after `//go:build` is mandatory.** `//go:build chaos` must be followed by a blank
  line before the `package` clause, or the toolchain treats it as an ordinary comment and silently
  compiles the file unconditionally. A silent gate failure is the worst kind.

- **Embedding promotes methods *and* fields — and name collisions resolve by depth.** An outer method
  at depth 0 shadows a promoted one at depth 1; two promoted methods at the *same* depth are an
  ambiguity *compile error* unless you disambiguate. Here there's one embed and one override, so it's
  unambiguous — but embedding two types with a same-named method would not compile until you spell out
  which you mean.

- **`sql.DB.Close()` is not transactional cancellation.** The single most load-bearing gotcha in this
  milestone: closing the pool leaves checked-out connections alive to commit. Cancel via `context`, or
  kill the backend; never assume `Close()` aborts work in flight.

- **`atomic.AddInt64` on a value must not have a lock-free copy.** The counter is addressed by pointer
  (`&remaining`) and never copied; copying a value under concurrent atomic access would give each copy
  its own counter. (In newer code `atomic.Int64` makes this misuse un-representable; the harness uses
  the function form on a plain `int64`.)

- **Nearest-rank vs. interpolated percentiles report different numbers.** `percentile(sorted, 95)`
  here returns an *observed* sample (`sorted[ceil(0.95·n)-1]`); an interpolating implementation would
  return a value *between* two samples. For a "worst latency we actually saw at P95" report, observed
  is the honest choice — but don't compare these figures against a tool that interpolates and expect
  them to match.

- **Asset isolation via UUID keying is what makes a shared dev DB safe.** Every scenario keys its rows
  on `"CHAOS-"+uuid`, so concurrent runs (and best-effort cleanup) never collide, and a leftover row is
  inert. `assertLedgerClosed` scopes its sum to *one asset* precisely so another asset's rows on the
  shared DB can't dirty the verdict.

## 7. Check yourself

1. `faultStore.ExecTx` is byte-identical to production `SQLStore.ExecTx` up to the point `fn` returns
   nil. Why is that identity *essential* to the test's validity, and what would a divergence *before*
   the commit point silently break?
2. The DB-failover scenario runs the doomed transaction on a *second* `sql.Open` pool and kills the
   backend from the *harness* pool. What specifically would go wrong if it used a single shared pool
   for both the doomed tx and the surviving assertions?
3. Walk through why `atomic.AddInt64(&remaining, -1) < 0` yields exactly `cfg.Requests` ops even when
   32 workers race on it. Where does the counter end up, and why is a negative value not a bug?
4. `brokenSendRPC.SendTransaction` deliberately does *not* delegate to the embedded
   `simulated.Client` even on the success path. Give the two distinct reasons, and describe what would
   break if it forwarded the call.
5. `assertConverged` gates a triple-reorg scenario that produces three settle entries and two
   reversals. Explain how one closed-ledger `SUM` check validates that thicket without enumerating any
   individual entry — and construct a bug it would catch that `assertSettleEntryCount` alone would not.

<details>
<summary>Answers</summary>

1. The test's claim is "the *real* commit boundary is atomic." If the fault store diverged from
   production before the commit — a different isolation level, a skipped rollback-on-error branch, a
   different `Querier` wrapping — then the transaction it aborts isn't the one production runs, and the
   "no partial state" assertion proves atomicity of a *different* code path. The identity up to
   `fn`-returns-nil is what lets you conclude anything about production; the fault must change *only*
   the success/commit step.
2. A single pool shares physical connections between the doomed tx and the assertion queries.
   `pg_terminate_backend` kills a specific backend PID; if the surviving assertions might be routed to
   that same connection (or the pool's health is disturbed by the kill), the assertions could fail for
   infrastructure reasons unrelated to the invariant. Separate pools guarantee the kill touches only
   the doomed connection, and the assertions read through an untouched one — they share a *database*,
   not a *connection*.
3. Each worker, before doing work, does one atomic decrement and checks the *returned* new value.
   Exactly one worker observes the decrement that produces `0` (the last valid slot); every worker that
   observes a negative return did an over-decrement and returns without working. With W workers the
   counter ends at roughly `-W+1` (each idle worker overshoots once), which is fine — only the *sign*
   gates work, and the count of successful claims is exactly `cfg.Requests` because the atomic makes the
   "claim the last slot" transition happen for one and only one worker.
4. (a) The counter `sent` is the "broadcast exactly once" probe — forwarding would conflate the probe
   with real backend behavior. (b) The tx is signed with an arbitrary test key and never mined-legally
   on the simulated chain; forwarding it to `simulated.Client.SendTransaction` would have the real
   backend *reject* it (bad nonce/unknown token/etc.), turning a should-succeed retry into a spurious
   failure. Forwarding breaks both the probe's meaning and the test's control over success.
5. The closed-ledger check sums `credit − amount` over *all* non-opening entry lines and asserts zero.
   Three settles each move `amount` one way and two reversals move it back; if and only if they net
   perfectly does the sum reach zero — the check validates the *aggregate* without knowing the count.
   A bug it catches that a count check misses: a reversal that debits the *wrong* account (right count,
   wrong target) leaves the settle-entry count correct but the sum non-zero. `assertSettleEntryCount`
   would pass; `assertLedgerClosed` would fail. (That's exactly why the suite runs both.)

</details>

## 8. Further reading

- [Go command — build constraints (`//go:build`)](https://pkg.go.dev/cmd/go#hdr-Build_constraints) —
  the tag syntax, the mandatory blank line, and how `-tags` selects files.
- [Effective Go — Embedding](https://go.dev/doc/effective_go#embedding) — struct/interface embedding,
  method promotion, and how shadowing resolves.
- [`sync/atomic` package docs](https://pkg.go.dev/sync/atomic) — the atomic add/load primitives and the
  newer typed `atomic.Int64` that makes the copy hazard un-representable.
- [`errors.Join` (Go 1.20 release notes)](https://go.dev/doc/go1.20#errors) — returning multiple errors
  as one value, and how `errors.Is`/`As` traverse a joined error.
- [PostgreSQL — `pg_terminate_backend`](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADMIN-SIGNAL) —
  server-side backend termination, the only faithful in-process way to abort an in-flight transaction.
</content>
</invoke>
