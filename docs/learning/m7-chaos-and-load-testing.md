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

**The `je.kind <> 'opening'` exclusion in that sum is load-bearing, not tidiness.** The
seeders inject starting balances with an `'opening'` journal entry that has a **lone credit
line and no counterparty** — the "money enters the system" shortcut, where a real deposit
would carry an external funding counter-entry. That entry is *deliberately unbalanced*.
Without the exclusion, a funded source would always show a nonzero sum equal to the injected
amount, so no scenario could ever converge and the suite would be permanently red. With it,
the invariant says exactly what it should: **every entry the system itself posted balances.**
Get that clause wrong in either direction and the oracle is either always-red or silently
blind to a real imbalance — which is why a one-clause `WHERE` earns this much prose.

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

**Alternative 3: external chaos tooling — toxiproxy, pumba, a real `docker kill`.** This is
the industry-standard way to do the network-partition and process-kill classes, and it was
rejected for three reasons. It is *non-deterministic* (timing-dependent, and therefore flaky
in CI); it is *heavyweight* (a proxy or a container-runtime dependency inside the test path,
against a repo that has no such precedent); and — the decisive one — it tests **the
orchestration around the process, not the transactional discipline inside it**, which is the
thing this milestone set out to prove. In-process injection is deterministic, dependency-free,
and runs clean under `-race`. State the limit honestly: it *cannot* exercise a real OS-level
`SIGKILL` landing between two syscalls. For a single-Postgres-transaction design, though,
"crash before COMMIT" is faithfully modelled by rolling back in place of committing, and
"connection death" is faithfully modelled server-side (Decision C) — so the two fault classes
that matter here are covered without the rig.

**And convergence must be asserted on *persisted* state, read through a *surviving*
connection.** Asserting on in-memory counters or maps would defeat the point: a restarted
process has no in-memory state, so the only meaningful claim is about the durable record.
Reading it back through the connection the fault killed would either error or serve stale
data. Every assertion in the suite goes through `dbh`, the harness pool that was never in the
blast radius.

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

**Two small shapes inside `faultStore` are worth copying.** First, there is **deliberately no
"no-fault" mode** — a `faultStore` *always* fails, and non-fault work runs through the real
`ledger.NewSQLStore`. Keeping the two behaviors in two different types makes it impossible to
accidentally get a real commit out of the saboteur. Second, the unknown-mode branch is
`default: panic(...)`, not a returned error: an unrecognized fault mode is a *programmer*
error in the test, and Go has no exhaustiveness check on a `switch` over an int-typed enum, so
the panic is the runtime backstop for "this switch is meant to be exhaustive."

**And the fault returns a sentinel, which the scenario matches.** `errInjectedCrash` plus
`errors.Is` at the call site is how a scenario proves the transaction died *exactly where it
was aimed* — rather than from some incidental constraint violation or bad connection that
would make the whole test a false positive. Matching on an error string would be both brittle
and capable of matching an unrelated failure.

**The honest shortcut, stated.** `payments.Service.Create` builds its own `SQLStore` internally
and is *not* injectable, so the crash and failover scenarios can't drive `Create` directly —
they reconstruct its transaction body inline (`PostWithin` + `InsertPayment`, "faithful to
payments.go:103-140" per the comment) and run *that* through the `faultStore`. This is a real
fidelity gap: if `Create`'s body drifts, the chaos copy won't. The production-grade fix is to
make `Create` accept an injected `ledger.Store` (constructor injection), which would let the
scenario drive the *actual* method. The suite documents the gap rather than hiding it, and it
does exercise the real `settlement.Sink` and real `evm.Adapter` directly (both *are* injectable),
so only the payment-create body is reconstructed.

One nuance keeps that compromise from being a hole rather than a gap: the reconstruction is
**conservative**. It *omits* the outbox and audit writes, and both of those would only ever
*add rows to the same transaction*. Extra writes inside one transaction can make atomicity
easier to satisfy, never harder — so their absence cannot manufacture a false pass of the
atomicity claim. The reconstruction could still drift in a way that matters (if `Create` ever
grew a *second, independent* transaction, a single-tx model would stop representing it), which
is exactly why the comment pins the production line range it mirrors, and why the named fix is
constructor injection rather than a comment refresh.

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

**Why `Close()` is a no-op here comes straight out of `database/sql`'s three layers,** and
this is the most portable paragraph in the milestone:

1. **`*sql.DB` is a *pool*, not a connection.** It is a concurrency-safe handle that lazily
   manages a set of driver connections — `sql.Open` does not even dial, which is why the
   harness calls `PingContext` to force one open.
2. **`BeginTx` checks *one* connection out of the pool and pins it** to the `*sql.Tx` for the
   transaction's entire life. That connection is no longer "in" the pool.
3. **`Close()` closes the *pool*** — it marks the pool closed and closes the *idle*
   connections. The transaction's connection is not idle; it belongs to the tx. So it stays
   open, the COMMIT flushes over that still-live socket, and Postgres commits. Durable write,
   no error, and a "fault" that was a no-op.

Note the reassuring flip side of the same fact: a graceful `db.Close()` during shutdown will
**not** rip the rug out from under an in-flight commit — the transaction's connection is
protected until the transaction ends.

**And the pinning in the fix is not incidental.** `pg_backend_pid()` is *per connection*: run
it on a random pooled connection and you learn the PID of a backend you are not about to use,
then terminate the wrong process and prove nothing. `doomed.Conn(ctx)` reserves one physical
connection so that the PID read and the transaction started share one backend. (`defer
conn.Close()` on the now-dead connection is still correct — closing an already-terminated
connection is a harmless no-op whose error is ignored.)

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

**Alternative 3: a channel of latency samples fanned into one collector goroutine.** This
looks *more* idiomatic ("share memory by communicating") and it does keep the workers
lock-free — so it deserves a real answer rather than a dismissal. Every sample now crosses a
channel: a send, a receive, and a scheduler hop, which is *more* per-sample overhead than an
uncontended lock, not less. And it adds a second moving part (the collector, its buffer size,
its drain-on-shutdown) for zero benefit. A channel earns its keep when you need to **stream**
or apply **backpressure**; here the job is to tally disjoint data and combine it once, which
is what sharded slices do with no machinery at all. Contrast M4's webhook fan-out, where the
consumers genuinely stream work and the plumbing is justified. "Share memory by
communicating" is a default, not a law.

### Decision G: the traffic pattern is derived from the system under test, not from the tool

**The problem.** A load generator that ignores the SUT's idempotency and locking semantics
measures an *artifact of those mechanisms* rather than the path you care about. Two lines in
`httpOp` exist only because of how this specific system behaves, and each is the difference
between a real benchmark and a fake one:

```go
req.Header.Set("Idempotency-Key", uuid.NewString()) // fresh per request
src := sources[rand.IntN(len(sources))]             // spread across many funded sources
dst := dests[rand.IntN(len(dests))]
```

**A fresh `Idempotency-Key` per request.** The API stores a keyed response and *replays* it
for a repeated key (M1's idempotency middleware). Reuse one key and every request after the
first returns a cached response without touching the ledger at all — you would be publishing
the latency of a keyed lookup, which is fast, flat, and meaningless as a create-path number.

**Traffic spread across many funded source accounts.** A payment create does
`SELECT … FOR UPDATE` on the source account row inside its transaction, so concurrent creates
against the *same* source serialize on that row lock — 32 workers collapse to an effective
concurrency of 1 for that account, and the "concurrency" knob stops meaning anything.
`seedAccounts` funds `--accounts` sources (default 100) with a high opening balance (`2^40`)
via the same derived-balance opening-entry shortcut the integration tests use, so every source
stays solvent for the whole run at `amount=1` and latency reflects the create path rather than
a wall of `insufficient_funds` rejections. Picking a random source per request keeps row-lock
contention low enough that concurrency actually converts into throughput.

The lesson generalizes past this repo: **design the traffic pattern backward from the system's
concurrency model.** Ask what the SUT caches, what it locks, and what it rejects, and make
sure your generator is not accidentally exercising one of those instead of the work.

### Decision H: `--migrate` is a deliberately minimal bootstrap, and says so

```go
func applyMigrations(ctx context.Context, sqlDB *sql.DB, dir string) error {
	entries, err := os.ReadDir(dir)
	// ... collect *.up.sql, sort lexically (filenames are zero-padded, so lexical IS apply order) ...
	for _, f := range files {
		stmt, _ := os.ReadFile(filepath.Join(dir, f))
		if _, err := sqlDB.ExecContext(ctx, string(stmt)); err != nil {
			return fmt.Errorf("apply migration %s: %w", f, err)
		}
	}
	return nil
}
```

**No version table, no down migrations, fail-fast on the first error.** The repo's hermetic
tooling rules (ADR-0015, ADR-0025) rule out shelling out to a host `psql` or adding a
migration-library dependency for a bench tool, and the load harness runs this exactly once
against an empty database. Run it against an already-migrated one and it fails on the first
`CREATE TABLE` with `relation … already exists` — which is fine, because `make down` (drop the
volume) is the reset.

Be honest about what it is not. No version tracking means it cannot apply a subset, cannot
detect a partial application, and is **not idempotent**; a production migrator (golang-migrate,
goose, Atlas) records applied versions and can roll forward from any state. For a run-once
fresh-database bootstrap in a benchmark harness that machinery is pure overhead — and reusing
the repo's own `*.up.sql` files means the benchmark schema can never drift from what the
services actually run. The shortcut is correct *for its scope*, and naming the scope is what
keeps it from becoming a landmine.

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

**Learn to read the table, because the reading is the deliverable.**

| Run | Concurrency | Throughput | P50 | P99 |
|---|---|---|---|---|
| 1 | 32 | 567.93 req/s | 45.7 ms | 146.5 ms |
| 2 | 50 | 568.03 req/s | 72.4 ms | 214.5 ms |

Little's Law makes the plateau quantitative. For a stable system `L = λ · W`, i.e.
`concurrency ≈ throughput · mean-latency`, so `throughput ≈ concurrency / mean-latency`.
Check it: `32 / 0.0457 s ≈ 700` and `50 / 0.0724 s ≈ 690` — the same neighborhood, converging
on the observed ~568 once the tail is folded in. When throughput is pinned while
`concurrency / latency` stays roughly constant, latency *must* be rising in lock-step with
concurrency, which is exactly what the table shows: the system is not getting more work done,
the extra workers are standing in line.

*Where* is the line? The create path is pure Postgres — one `ExecTx` that locks source and
destination, runs the double-entry balance check, inserts the journal entry, its lines and the
payment row, and in the same transaction appends an outbox event and an audit record. Every
commit is one synchronous `fsync`-bounded transaction on a single untuned dev Postgres, now
carrying two extra row writes per commit. Commit throughput is the wall. The Go client, the
HTTP framing, and the harness are nowhere near saturation — which is *precisely why* the
harness had to be lock-free and artifact-free. If the instrument were the bottleneck you would
misread the plateau as the server's limit.

The operational read, which is the actual output of a benchmark: **right-size concurrency near
run 1 (~32 workers)** — it delivers the same throughput as 50 at roughly half the P99. That
recommendation is only trustworthy because the measurement path is clean.

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

Reason about the boundary once and it stays reasoned: the first `Requests` claims see post-values
`Requests-1, …, 1, 0` — all `>= 0`, so all proceed. The next sees `-1` and returns, as does
every claim after it. Exactly `Requests` ops. Writing `<= 0` instead of `< 0` would turn away
the claim that produced `0` — the *last valid slot* — and run `Requests-1` ops. (Why not a
buffered channel of N tokens? It also gives exactly-N, at the cost of allocating and filling an
N-length channel and paying a channel receive per request. For a pure counter with no payload
to carry, one atomic word is the smaller and clearer answer; channels earn their place when
tokens carry data or need buffering.)

### 3f. The worker pool: `Add` before `go`, `Done` in a `defer`, and passing the pointer

```go
results := make([]workerResult, cfg.Concurrency)
var wg sync.WaitGroup
for i := range results {
	wg.Add(1)
	go func(wr *workerResult) {
		defer wg.Done()
		// ... hot loop writes only through wr ...
	}(&results[i])
}
wg.Wait()
```

Four Go facts are doing real work here.

`make([]workerResult, cfg.Concurrency)` puts every worker's state in one backing array, but
each worker writes to a **distinct address**. A data race requires two goroutines touching the
*same* address with at least one write; here every address has exactly one writer, so the whole
hot path is race-free with no lock and `-race` confirms it. `wr.lats = append(...)` grows a
slice the worker exclusively owns, so even the reallocation is private.

`wg.Add(1)` runs on the **parent** goroutine, before `go`. Move it inside the child and
`wg.Wait()` can run before that child is scheduled, observe a zero counter, and return early.
The rule is "Add before you go, Done inside via defer" — and `defer wg.Done()` as the first
statement means every exit path (deadline, budget spent, cancellation) decrements exactly once,
which is why no explicit `Done` is needed at any of the returns.

`go func(wr *workerResult) { … }(&results[i])` **passes** the address rather than capturing the
loop variable. This is the classic Go concurrency bug in older code: a closure over `i` reads
whatever `i` *is when the goroutine runs*, so every worker stampedes the final slot. `&results[i]`
is evaluated in the parent at the moment `go` executes, once per iteration, binding each worker
to its own slot at launch. Go 1.22's per-iteration loop scoping would also make a captured `i`
safe here, but the explicit pointer says "this worker owns this slot" at the call site and is
version-proof.

Finally, `wg.Wait()` returning after every `Done` is a **happens-before edge** in the Go memory
model, so the single-threaded merge afterward sees all the workers' writes with no lock at all —
concatenate, sort once, index. Shard on the hot path, reconcile at the join.

### 3g. `context.WithTimeout` and the cancelled-tail trap

The run's stop condition is expressed as a *derived context*, not a hand-rolled `time.After` in
the loop:

```go
runCtx := ctx
if cfg.Duration > 0 {
	var cancel context.CancelFunc
	runCtx, cancel = context.WithTimeout(ctx, cfg.Duration)
	defer cancel()
}
```

`WithTimeout` returns a child that is `Done()` after the duration *or* when the parent (the
SIGINT-cancellable context) is cancelled, whichever comes first — so a worker's stop check is
the single predicate `runCtx.Err() != nil` and it covers both. `defer cancel()` is mandatory,
not hygiene: skip it and the timer's resources leak until the deadline fires, which vet and
lint will flag.

The trap is that the check at the *top* of the loop is not enough:

```go
for {
	if runCtx.Err() != nil { return }
	// ... budget claim ...
	t0 := time.Now()
	oc := op(runCtx)
	if runCtx.Err() != nil { return } // <-- discard the cancellation artifact
	wr.lats = append(wr.lats, time.Since(t0))
	wr.tally[oc]++
}
```

Picture the exact instant the deadline fires: a worker has already passed the top check and is
*inside* `op(runCtx)` with a request in flight. The timeout cancels `runCtx`, which aborts the
in-flight `client.Do`, so `httpOp` classifies it as a `TransportError` with a **truncated**
latency — the request was killed early, not completed. Without the second check that artifact is
recorded, and across C workers you get up to C phantom transport errors at the tail of *every
clean run* plus a near-zero sample that corrupts `Min`.

This is the shape of the hazard worth internalizing, well beyond this file: **Go cancellation is
cooperative, so it surfaces as an aborted operation returning an error — not as a thread kill.**
Anything that classifies errors must therefore ask "did the run end while I was working?" before
believing the classification. `TestRunLoadDurationModeDiscardsCancelledSample` pins it with a
fake op that returns `TransportError` on `ctx.Done()`, asserting a clean duration run reports
zero transport errors *and* `Min > 0`. The cost of the fix is at most one dropped sample per
worker at the very tail; the cost of omitting it is a benchmark that quietly lies.

### 3h. `Op func(ctx) Outcome`: when a function type beats an interface

```go
type Op func(ctx context.Context) Outcome

func runLoad(ctx context.Context, cfg loadConfig, op Op) loadResult { ... }
```

`Op` is the harness's entire dependency-inversion boundary, and it is a *function type* rather
than an `interface{ Do(ctx) Outcome }`. In Go those are near-equivalent when the seam has
exactly one method and no state the caller needs to inspect — and the function is lighter: no
struct to declare, no method set, the closure captures whatever it needs. This is the same
instinct as `http.HandlerFunc` in the standard library. Contrast M5's `Screener`, which stayed
an interface because a documented second *implementation* was coming; here there is one real op
and a family of test ops, so a `func` is the right razor.

The property that makes the seam pay: **the load loop times the op; the op never times itself.**
`t0 := time.Now()` and `time.Since(t0)` bracket the call, so "how the request is issued" — HTTP
today, gRPC tomorrow, an in-memory fake in a test — is orthogonal to "what latency is." The op
returns only a classification; the number under test belongs to the harness. That is exactly how
the duration-mode tests exercise the timing loop with no server and no database.

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

- **An always-red convergence oracle.** Drop the `je.kind <> 'opening'` filter and every seeded
  source's lone, deliberately-unbalanced opening credit lands in the sum, so
  `assertLedgerClosed` reports the seeded amount instead of zero on *every* scenario. Widen the
  filter the other way and the oracle stops seeing real imbalances. The clause has to be exactly
  "entries the system itself posted."

- **A convergence assertion on the wrong number.** Asserting `settle entry count == 1` looks
  right and is wrong under reorg: each `Confirmed(block)` posts a fresh block-hash-scoped settle
  entry and each `Reorged` posts a reversal, so a correct system ends a triple-reorg with three
  settles and two reversals. The raw count grows monotonically with churn; only the *net*
  (balances back to zero, reconcile discrepancy zero) is stable across any number of cycles. The
  alternative — a fragile `count == 2·reorgs + 1` formula — encodes the mechanism into the test.

- **The loop-variable capture stampede.** Capturing `i` instead of passing `&results[i]` sends
  every worker to the same slot. Pre-Go-1.22 this is a guaranteed bug; the explicit pointer
  argument is immune either way.

- **A lost increment from a shared tally.** One `[4]int` shared across workers makes
  `tally[oc]++` a racing read-modify-write that silently drops counts (and trips `-race`).
  Per-worker arrays merged after `Wait()` are race-free by construction.

- **An unbounded run from a flag typo.** `--duration=0` with no `--requests` leaves neither a
  deadline nor a budget, so the loop hammers the API until SIGINT. `runLoadtest` rejects that
  combination *before* any database dial, so an operator typo fails fast.

- **A leaked timer goroutine.** Omitting `defer cancel()` on the `WithTimeout` child leaks the
  timer until the deadline elapses.

- **A benchmark that measures the idempotency cache.** Reusing one `Idempotency-Key` makes every
  request after the first a keyed replay that never touches the ledger — high, flat throughput
  and a low P99 that describe a lookup, not a create.

- **A benchmark that measures one row lock.** Driving every create from a single funded source
  serializes them all on that row's `SELECT … FOR UPDATE`, collapsing effective concurrency to 1
  and making the concurrency sweep meaningless.

- **Connection churn masquerading as the concurrency limit.** Failing to drain and close the
  response body pins each connection and defeats keep-alive, so the effective concurrency
  becomes the dial rate rather than `--concurrency`.

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

- **The pool / connection / transaction model maps one-for-one onto JDBC, gotcha included.**
  `*sql.DB` ≈ a HikariCP `DataSource` pool, `*sql.Conn` ≈ a checked-out
  `java.sql.Connection`, `*sql.Tx` ≈ that connection with `autoCommit=false`. And the analogy
  holds *all the way to the trap*: `dataSource.close()` in Java likewise shuts the pool without
  aborting a connection a thread is mid-transaction on, and killing the backend with
  `pg_terminate_backend` is the same trick a Java integration test reaches for to simulate a
  failover. This one transfers cleanly — which is exactly why it is worth remembering.

- **The worker pool is a fixed thread pool joined by a `CountDownLatch`.** `sync.WaitGroup` is
  the latch and `defer wg.Done()` is the `finally { latch.countDown() }`. The Go difference is
  that goroutines are cheap enough that "one goroutine per worker" is the *literal*
  implementation, not a pool abstraction over OS threads — and per-worker slices merged at the
  end are the same sharding a Java `Collector` with a per-thread accumulator performs.

- **`context.WithTimeout` is a `CancellationTokenSource(timeout)` / an `AbortController` wired
  to a `setTimeout`.** Where it breaks: Go's cancellation is *cooperative* — nothing is
  preempted, and the operation must actually observe the context (the HTTP client does, via
  `NewRequestWithContext`) for cancellation to take effect. That cooperation is precisely why
  the cancelled-tail discard exists: cancellation arrives as an aborted call returning an error,
  not as a killed thread.

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

- **`Outcome`'s zero value is `OK` on purpose.** `const ( OK Outcome = iota; … )` makes `OK == 0`,
  so a freshly made `[4]int` tally reads as "no successes recorded here" rather than as some
  garbage class. Zero-value-as-sensible-default, chosen deliberately and commented.

- **`[4]int` is an array, not a slice — and that matters twice.** Arrays are *values*: assigning
  the tally copies all four ints, and arrays are **comparable**, so the tests can write
  `if r.ByOutcome != want`. A slice would share backing storage and would not be `==`
  comparable; both properties are relied on.

- **Drain *and* close the response body.** `io.Copy(io.Discard, resp.Body)` followed by
  `defer func() { _ = resp.Body.Close() }()` is what lets the transport reuse the connection. A
  body left unread pins its connection and defeats keep-alive, silently capping effective
  concurrency at the dial rate — so the `--concurrency` knob would stop being the real limiter.

- **`math/rand/v2`'s top-level functions are goroutine-safe without a global mutex.**
  `rand.IntN(len(sources))` is safe to call from every worker; the older `math/rand` top-level
  functions were also safe, but via a shared lock. v2 is the current idiom for exactly this
  hot-path use.

- **`fs.Visit` is how you express "exactly one of these flags".** `flag` cannot say XOR, but
  `fs.Visit` iterates only the flags the user *actually set*, so `set["duration"] &&
  set["requests"]` distinguishes "both explicitly given" from "both at default." A separate
  lower-bound guard (`Duration <= 0 && Requests <= 0`) closes the case the XOR check misses.

- **Guard shared test state even in a single-threaded scenario.** `brokenSendRPC`'s counters are
  `mu`-guarded and `recordingSigner` hands out a copy via `signedNonces()` — not because the
  scenario is concurrent, but because the *EVM adapter* is entitled to touch them from multiple
  goroutines. Hygiene at the seam, not at the test.

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
6. `assertLedgerClosed` filters `je.kind <> 'opening'`. For a scenario that seeds a source with an
   opening balance of 1000 and then moves 400 to a destination, describe exactly what the assertion
   reports if that clause is removed — and why the failure would appear in *every* scenario rather
   than just this one.
7. Delete the *second* `if runCtx.Err() != nil { return }` (the one after `op`). Which reported
   fields become wrong, why does a quick manual duration run still look completely plausible, and
   which single assertion in the suite catches it?
8. Rewrite the budget claim as `if atomic.AddInt64(&remaining, -1) <= 0 { return }`. For
   `Requests = 500`, exactly how many ops run, and which specific slot did you lose?
9. Throughput is reported as `tally[OK] / elapsed.Seconds()` — successful requests only. Under a
   server saturated into 5xx, describe how throughput and the outcome tally each move, and argue
   why an OK-only numerator is the right choice for a *create-path* benchmark.

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
6. Without the clause, the sum includes the `'opening'` entry's lone 1000 credit, which has no
   debit counterparty by design. The 400 payment posts a *balanced* entry that nets to zero, so
   `Σ(credit − debit)` comes out at **+1000**, not 0, and `assertLedgerClosed` fails. It would
   fail in every scenario, because every scenario must seed a funded source to have anything to
   move — removing the filter redefines "converged" as "no money was ever injected into the
   system," which no payment test can satisfy.
7. `ByOutcome[TransportError]` gains up to `Concurrency` phantom errors on every clean run (one
   per worker whose in-flight request the deadline aborted), and `Min` collapses toward zero from
   the truncated sample. A manual run still shows thousands of OKs and sane P50/P99 figures, so a
   handful of tail artifacts hides in the noise unless you specifically read `Min` or the error
   tally — which is what makes it dangerous. `TestRunLoadDurationModeDiscardsCancelledSample`
   fails: it asserts `TransportError == 0` *and* `Min > 0`.
8. 499. `atomic.AddInt64` returns the value *after* the decrement, so the claim that yields `0`
   is the 500th and last valid slot — under `<= 0` that worker returns instead of running its
   op. You lose exactly the final slot, which is also the boundary a naive test with a small
   budget is least likely to notice.
9. `ByOutcome[ServerError]` climbs while `tally[OK]` — and therefore throughput — *falls*, which
   is the honest reading: throughput should reflect *useful work completed*, not requests
   attempted. Counting every response would flatter a failing server that returns errors quickly,
   and could show throughput *rising* as the system degrades. For a create benchmark the number
   that matters is creates that actually committed; the tally keeps the failures visible on a
   separate line rather than folding them into the headline.

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
- [`database/sql` package docs](https://pkg.go.dev/database/sql) — the `DB` (pool) / `Conn`
  (checked-out) / `Tx` (pinned) lifecycle behind Decision C's three-layer explanation.
- [The Go Memory Model](https://go.dev/ref/mem) — the happens-before guarantees that make
  "disjoint per-worker writes plus `WaitGroup.Wait()`" correct with no locks at all.
- [`context.WithTimeout`](https://pkg.go.dev/context#WithTimeout) and
  [Go blog — Go Concurrency Patterns: Context](https://go.dev/blog/context) — derived deadlines,
  cooperative cancellation, and why `defer cancel()` is mandatory rather than tidy.
