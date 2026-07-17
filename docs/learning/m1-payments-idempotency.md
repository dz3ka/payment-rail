# M1 — Payments & the idempotent API: transaction composition, a buffering ResponseWriter, and keyset paging

> Scope: this lesson is about *composing a new service on top of an existing
> domain seam* and *building an at-most-once HTTP layer* in Go. The headline
> achievements are (1) a payments service that commits its own row and a ledger
> journal entry in **one** Postgres transaction by sharing the ledger's
> `Querier`, without payments ever learning how transactions begin or commit;
> (2) an idempotency middleware that captures a handler's exact bytes and status
> through a home-grown `http.ResponseWriter` and replays them verbatim; and (3)
> keyset pagination that pages a live, concurrently-growing table with no `OFFSET`
> and no count query. Each is an idiom you will reuse in every service that
> follows. This builds directly on the ledger lesson — read that first if the
> `Store`/`Querier` seam is not yet second nature.

## 1. What we built

M1 put an external REST surface on the ledger: `POST /v1/payments`,
`GET /v1/payments/{id}`, `GET /v1/payments`, and `POST /v1/payments/{id}/cancel`.
The new domain package `internal/payments` turns "move Amount from source to
dest" into an atomic pair of writes — one balanced journal entry *and* one
`payments` row — and undoes a payment the same way, by appending a *reversing*
entry rather than mutating history. Crucially, payments never stores a balance
and never bypasses double-entry: it reuses the ledger's rules wholesale. The
seam that makes this possible is a small extraction in the ledger package,
`ledger.PostWithin(ctx, q, e)`, which posts an entry using a `Querier` the caller
already holds — so payments can write its own row on the *same* transaction
(ADR-0007).

On top of the service sits the HTTP layer in `cmd/api`. The load-bearing piece
is the idempotency middleware (`withIdempotency`, ADR-0005): every create must
carry an `Idempotency-Key`, the middleware hashes the canonical request, claims
the key with `INSERT ... ON CONFLICT DO NOTHING`, runs the handler exactly once
into a **buffering recorder**, then caches the recorded status + bytes so any
same-key retry within 24 hours replays the original response instead of moving
money twice. A background sweeper reaps expired keys so the table stays bounded.
The list endpoint pages with a **keyset cursor** — an opaque base64url token — so
pages stay stable while new payments are being inserted.

The part to study hardest is *how three separate concerns compose without leaking
into each other*: the ledger owns accounting invariants, payments owns the
payment/ledger atomicity, and the HTTP layer owns idempotency and wire shapes —
and the transaction boundary is drawn deliberately *around the payment* but
deliberately *outside the idempotency bookkeeping*, because those two must fail
independently. The milestone's gate, `TestConcurrentCreatesNeverOverdrawSource`,
proves the whole stack is concurrency-safe under `-race`: 30 racing debits against
a balance that funds only 10, yielding exactly 10 clean `201`s and 20 clean `422`s,
final balance 0, never negative, never a `500`.

## 2. The design decision

### Decision A: compose the payment and the journal entry in ONE transaction via a shared `Querier`

**The problem.** Creating a payment is two writes that must be all-or-nothing: the
`journal_entries`/`entry_lines` rows that actually move the money, and the
`payments` row that records it. If the journal entry commits but the payment row
does not (or vice-versa), you have money moved with no record, or a record with no
money — both corruptions. They must land in the same Postgres transaction.

**The chosen approach — expose the ledger's work as a transaction-agnostic
primitive.** The ledger already had `Service.PostEntry`, but that method *owns* its
transaction: it calls `store.ExecTx`, opens a tx, does the work, commits. Payments
can't nest inside that. So M1 extracted the body into `PostWithin(ctx, q, e)`,
which performs the lock → check → apply → insert sequence against a `Querier` the
*caller* supplies and does **not** open a transaction of its own. `PostEntry` now
just wraps `PostWithin` in an `ExecTx`; payments calls `PostWithin` *inside its own*
`ExecTx` closure and writes the `payments` row on the same `q`:

```go
err := s.tx.ExecTx(ctx, func(q db.Querier) error {
    je, err := ledger.PostWithin(ctx, q, entry) // journal entry, same tx
    if err != nil {
        return err
    }
    payment, err = q.InsertPayment(ctx, db.InsertPaymentParams{ /* ... */ JournalEntryID: je.ID})
    return err
})
```

One `ExecTx`, one Postgres transaction, both writes or neither. The payment's `id`
doubles as the entry's `ExternalRef`, so a payment and its ledger footprint are
one-to-one and mutually idempotent at the database level (the ledger's
`UNIQUE(kind, external_ref)` still applies). This is the **unit-of-work** pattern
again, but the lesson here is *seam reuse*: the same `ExecTx` closure that the
ledger tests fake is now the composition point for a second package.

**Alternative 1: nested transactions / savepoints.** Rejected. Postgres has no true
nested transactions; the closest thing is savepoints (`SAVEPOINT`/`RELEASE`), which
add rollback-to-point machinery for no benefit here — payments needs *all-or-nothing
with the entry*, not partial rollback of a sub-step. Savepoints solve a problem we
don't have.

**Alternative 2: two independent transactions (post the entry, then insert the
payment).** Rejected outright: a crash between them is exactly the money-with-no-record
corruption above. Distributed-transaction patterns (a saga with a compensating
"reverse the entry" step) exist for when the two writes *can't* share a transaction —
e.g. across service/database boundaries — but here they share one Postgres
connection, so a single local transaction is strictly simpler and strictly safer.
Keep the saga in your pocket for the day payments and the ledger live in different
databases.

**Alternative 3: store a balance on the account row and skip the ledger.** Rejected —
it violates ADR-0004's derived-balances invariant (see the ledger lesson). Payments
is a *client* of the ledger, not a replacement for it.

### Decision B: the idempotency lifecycle lives OUTSIDE the payment transaction — on purpose

**The problem.** At-most-once create: a client retrying after a timeout must not
double-charge. The standard shape is a state machine keyed by a client token:
claim → run once → cache the response → replay on retry.

**The chosen approach — a separate single-statement lifecycle, failing safe.** The
`IdempotencyStore` operates on the pool directly (each method is one statement),
*deliberately not* inside the payment's transaction. The reasoning is the inverse of
Decision A: a payment must **not** roll back just because *caching its response*
failed. The states are `in_flight` (claimed) → `completed` (response snapshotted),
with a 24h TTL. `Begin` uses `INSERT ... ON CONFLICT DO NOTHING RETURNING *`: a
returned row means the claim is ours (fresh); zero rows (`sql.ErrNoRows`) means the
key is taken, so we fetch the existing row and let the caller resolve the retry —
replay verbatim if completed and the request hash matches, `409` while `in_flight`,
`422` on a hash mismatch (same key, different body = client misuse).

**The honest limitation, stated in the code.** If `Complete` fails *after* the
payment has already committed, the key stays `in_flight`, and same-key retries get
`409` until the sweeper reaps it after the TTL. That is a *safe* failure — no
in-window double charge — but it is a real rough edge: the client is told "still in
progress" for a request that actually succeeded. The fully-robust fix is to fold the
idempotency completion into the payment's own transaction (so the response snapshot
commits atomically with the payment). That was deferred (ADR-0005) precisely because
it re-couples the two concerns Decision B just separated, and the safe-failure mode
buys us the time to design it properly. Naming the trade-off in a comment at the
failure site is the point — the next engineer sees the sharp edge without reading the
ADR.

**Alternative: only cache on success, delete the claim on any failure.** That's what
the code does for non-2xx (`s.idem.Delete`), but note it can't cover the
"payment committed, cache write failed" window — the payment is *already* a success,
so deleting the claim would *re-enable* a double charge on retry. Failing safe as
`in_flight` is the lesser evil until the transactional fix lands.

### Decision C: keyset (cursor) pagination with a `limit+1` look-ahead

**The problem.** Page a table that is being written to concurrently, cheaply and
without skipping or repeating rows.

**The chosen approach.** Order by `(created_at DESC, id DESC)` and continue with a
**row-value comparison**: `WHERE (created_at, id) < ($1, $2)`. That is a single index
range scan over `idx_payments_keyset`, O(page size), and stable under inserts — a new
payment landing between requests never shifts a page you already fetched. To know
whether a *next* page exists without a second `COUNT(*)` round trip, fetch `limit+1`
rows: if more than `limit` come back, there's another page and the extra row's
predecessor supplies `next_cursor`; otherwise it's the last page. The cursor is an
opaque base64url token clients must not parse.

**Alternative: `OFFSET`/`LIMIT`.** Rejected. `OFFSET n` makes the database *scan and
discard* n rows (O(n) per page, quadratic over a full walk), and it *drifts* under
concurrent inserts: a row inserted before your offset shifts everything down, so page
2 repeats a row from page 1. Keyset paging has neither problem. The cost is that you
can't jump to "page 47" — but a payments API doesn't need random page access, it
needs a stable forward scan.

### Decision D: cancel is an append-only reversing entry, guarded by the UPDATE itself

Cancel posts a *mirror* journal entry (debit dest, credit source) and flips the
payment's status — in one transaction, via the same `PostWithin` seam. It never
mutates or deletes the original entry, honoring ADR-0004: the ledger is append-only,
so a "cancel" is just more history. The concurrency guard is subtle and worth
stealing: the `CancelPayment` SQL is `UPDATE ... WHERE id = $1 AND status =
'completed'`. A second concurrent cancel matches no row, sqlc returns
`sql.ErrNoRows`, and the service maps that to `ErrPaymentNotCancelable` — so the
"who won the cancel race" decision is made *by the database's row lock on the UPDATE*,
not by a read-then-write in Go that would be a TOCTOU race. If the destination has
since spent the returned funds, the reversal legitimately can't balance and
`ErrInsufficientFunds` propagates — the correct outcome, not something to paper over.

## 3. Language deep-dive

### 3a. Implementing `http.ResponseWriter` to buffer a handler's output

The middleware can't let the handler write straight to the socket, because it must
decide *after the handler runs* whether to cache or discard the response. So it hands
the handler a fake `http.ResponseWriter` that records everything:

```go
type responseRecorder struct {
    status      int
    body        *bytes.Buffer
    header      http.Header
    wroteHeader bool
}

func (r *responseRecorder) Header() http.Header { return r.header }

func (r *responseRecorder) WriteHeader(code int) {
    if r.wroteHeader {
        return
    }
    r.status = code
    r.wroteHeader = true
}

func (r *responseRecorder) Write(b []byte) (int, error) {
    if !r.wroteHeader {
        r.WriteHeader(http.StatusOK)
    }
    return r.body.Write(b)
}
```

`http.ResponseWriter` is a three-method interface (`Header() http.Header`,
`Write([]byte) (int, error)`, `WriteHeader(int)`). Because Go interface satisfaction
is **structural** — a type satisfies an interface by having the methods, with no
`implements` keyword — `responseRecorder` *is* an `http.ResponseWriter` simply by
defining those three. The middleware then calls `next(rec, r)` and the handler,
which only knows it holds an `http.ResponseWriter`, writes into our buffer none the
wiser. This is the **decorator/adapter** pattern, and it's the same trick as the
ledger's `fakeQuerier`, applied to a *standard-library* interface instead of a
generated one.

Two details are load-bearing. First, `WriteHeader` guards on `wroteHeader` and is a
no-op on a second call — this mirrors the real `http.ResponseWriter`, where a second
`WriteHeader` is an error and the first status wins; faithfully reproducing that
contract means a handler that mistakenly writes the header twice behaves identically
against the recorder and the wire. Second, `Write` implicitly calls
`WriteHeader(200)` if the handler wrote a body without an explicit status — again
matching net/http's real behavior, so the recorded `status` is never a meaningless
zero. The `flushTo` method later copies the buffered header, status, and body onto
the *real* writer, but only after the middleware has cached them. Note the standard
library ships `httptest.ResponseRecorder` for exactly this; rolling a minimal one
here keeps the dependency surface small and makes the buffering explicit for the
lesson — in production either is fine.

### 3b. `defer` + a `completed` flag to release the claim on a panic, and `context.WithoutCancel`

A claimed key must never get stuck `in_flight` because the handler *panicked*. The
middleware defends the unwind path with a deferred release:

```go
completed := false
defer func() {
    if !completed {
        if delErr := s.idem.Delete(context.WithoutCancel(ctx), key); delErr != nil {
            s.log.ErrorContext(ctx, "idempotency release after panic failed", "err", delErr, "key", key)
        }
    }
}()
next(rec, r)
completed = true
```

`defer` schedules the closure to run when the function returns *by any path,
including a panic* — this is Go's `finally`, except it's attached to a specific
statement, not a block, and it runs LIFO. The `completed` boolean is the idiom for
"did we reach the normal path?": if `next` panics, the assignment `completed = true`
never runs, so the deferred closure sees `false` and releases the claim as it
unwinds; the panic then continues up to the server's own recovery, which turns it
into a `500`. On the normal path `completed` is `true` and the defer does nothing —
the explicit `Complete`/`Delete` logic below handles those cases. Without the flag,
the defer couldn't tell a panic from a clean return.

`context.WithoutCancel(ctx)` is the other subtlety. By the time the panic unwinds,
the request's `ctx` may already be canceled (client gone, or the panic is *because*
of a timeout). If we passed that canceled `ctx` to `Delete`, the delete would fail
immediately with `context.Canceled` and the key would stay stuck — the exact bug we're
trying to prevent. `WithoutCancel` (Go 1.21) returns a context that keeps the parent's
*values* (trace IDs, deadlines-as-values) but is **detached from its cancellation**, so
the cleanup query runs even though the request is dead. The same pattern appears in
`main.go`'s graceful shutdown, which builds a *fresh* `context.WithTimeout` for
`srv.Shutdown` precisely because the run context is already canceled.

### 3c. Closures capturing outer variables to smuggle results out of `ExecTx`

`ExecTx` takes a `func(q db.Querier) error` — the closure can only return an `error`,
so how does `Create` get the `payment` row back out? By **closing over** a variable
declared in the enclosing scope:

```go
var payment db.Payment
err := s.tx.ExecTx(ctx, func(q db.Querier) error {
    je, err := ledger.PostWithin(ctx, q, entry)
    if err != nil {
        return err
    }
    payment, err = q.InsertPayment(ctx, /* ... */)  // assigns the OUTER payment
    return err
})
if err != nil {
    return db.Payment{}, err
}
return payment, nil
```

`payment` is declared *outside* the closure; the closure captures it **by reference**
(Go closures capture variables, not values), so the assignment inside the transaction
is visible to `Create` after `ExecTx` returns. This is the standard Go workaround for
the fact that the unit-of-work callback has a fixed signature: results ride out on
captured variables, control-flow rides out on the returned `error`. Note the careful
ordering — `payment` is only *read* after checking `err != nil`; on the error path the
captured value may be a half-written zero, so the code returns `db.Payment{}` explicitly
rather than trusting the closure's partial write. `Cancel` uses the identical shape with
`var canceled db.Payment`. A newcomer coming from a language with multi-value lambda
returns often reaches for a channel or a pointer parameter here; the captured local is
the idiomatic Go answer.

### 3d. `limit+1` look-ahead and reslicing to compute the next cursor without a count

```go
if after == nil {
    rows, err = q.ListPaymentsFirstPage(ctx, limit+1)   // ask for one extra
} else {
    rows, err = q.ListPaymentsAfter(ctx, db.ListPaymentsAfterParams{
        AfterCreatedAt: after.CreatedAt, AfterID: after.ID, PageLimit: limit + 1,
    })
}
// ...
if int32(len(rows)) <= limit {
    return rows, nil, nil                                // no extra row => last page
}
rows = rows[:limit]                                      // drop the peeked row
last := rows[len(rows)-1]
return rows, &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}, nil
```

The trick is entirely in the arithmetic. Asking the database for `limit+1` rows and
checking whether that many came back answers "is there a next page?" for free — no
`SELECT COUNT(*)`. If `len(rows) <= limit`, the extra row didn't exist, so this is the
last page and `next_cursor` is `nil`. Otherwise `rows = rows[:limit]` **reslices** to
drop the peeked row. Reslicing is O(1) and allocation-free: a Go slice is a
`{pointer, len, cap}` header, and `rows[:limit]` returns a new header pointing at the
same backing array with a smaller `len` — no copy. The kept last row supplies the
cursor. The corresponding SQL is where the paging actually happens:

```sql
WHERE (created_at, id) < (sqlc.arg(after_created_at)::timestamptz, sqlc.arg(after_id)::uuid)
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(page_limit);
```

The `(created_at, id) < ($1, $2)` is a **row-value (tuple) comparison**, not two
`AND`ed predicates — Postgres compares lexicographically, so it means "created earlier,
or same instant but a smaller id," which is exactly "strictly before this cursor in the
sort order." Including `id` as a tiebreaker is what makes the order a *total* order even
when two payments share a `created_at`, so no row is ever skipped or repeated at a page
boundary. The `::timestamptz`/`::uuid` casts exist so sqlc infers concrete Go parameter
types (`time.Time`, `uuid.UUID`) for the `sqlc.arg()` placeholders instead of `interface{}`.

### 3e. `select` over `ctx.Done()` and a ticker: a self-terminating background goroutine

The sweeper is a long-lived goroutine that must die cleanly on shutdown:

```go
func (s *IdempotencyStore) RunSweeper(ctx context.Context, interval, ttl time.Duration, log *slog.Logger) {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            n, err := s.SweepExpired(ctx, ttl)
            if err != nil {
                log.ErrorContext(ctx, "idempotency sweep failed", "err", err)
                continue  // don't die on a transient error; next tick retries
            }
            // ...
        }
    }
}
```

`select` blocks until one of its cases is ready. Here that's either the ticker firing
(`<-ticker.C` yields a time value every `interval`) or the context being canceled
(`<-ctx.Done()` unblocks once, permanently, when shutdown begins). Because `ctx.Done()`
is checked *on every loop iteration*, the goroutine exits promptly on shutdown rather
than sleeping out the rest of its interval. `defer ticker.Stop()` releases the ticker's
underlying timer when the loop returns — a `time.Ticker` that isn't stopped leaks its
runtime timer. The `continue` on a sweep error is the "a failed background job must not
kill the worker" idiom: log it and let the next tick retry, so a momentary DB blip
doesn't silently disable TTL cleanup. `main.go` launches this with a bare
`go idem.RunSweeper(ctx, ...)`, and the shared `ctx` cancellation is the *only* thing
needed to shut it down — no separate stop channel, no `sync.WaitGroup`, because the run
context already models "the process is stopping."

## 4. What would break

- **Double charge on retry.** The whole point of the idempotency machine. A naive
  create endpoint re-runs the payment on every retry; here a repeat key replays the
  cached bytes (`replay`), and the `in_flight` state returns `409` while the first is
  still running, so two concurrent identical requests can't both post.
- **Money moved with no record (or vice-versa).** The single `ExecTx` around
  `PostWithin` + `InsertPayment` makes a partial commit impossible — a newcomer using
  two separate `db.New(pool)` calls would ship exactly this corruption.
- **Response cached but request differs.** Same key, different body is client misuse;
  the SHA-256 request hash comparison (`bytes.Equal(storedHash, hash)`) catches it and
  returns `422` instead of silently replaying an unrelated response.
- **Stuck `in_flight` key on a handler panic.** The `defer` + `completed` flag +
  `context.WithoutCancel` release the claim during unwind. Without the detached context
  the cleanup delete would itself fail on the already-canceled request context — a
  subtle bug that only bites under load/timeout.
- **Pagination drift and O(n) skips.** Keyset paging with the `(created_at, id)` tuple
  is stable under concurrent inserts and index-scan cheap; `OFFSET` would repeat rows and
  scan-discard.
- **Unbounded idempotency table.** The sweeper caps it at a 24h window; without it the
  table grows forever.
- **Overdraw under concurrency.** Proven absent by `TestConcurrentCreatesNeverOverdraw
  Source` under `-race`: the ledger's ordered `SELECT ... FOR UPDATE` serializes the
  balance holds, so 30 racing debits against a 10-payment balance yield exactly 10
  successes and 20 clean `422`s, final balance 0, never a `500`. A newcomer would likely
  read the balance *before* locking (TOCTOU) and ship an intermittent overdraw.
- **Cancel race.** The `UPDATE ... WHERE status = 'completed'` guard lets the database
  arbitrate concurrent cancels; a read-modify-write in Go would double-reverse.
- **OOM from a hostile body.** `http.MaxBytesReader` caps the create body at 64 KiB
  *before* `io.ReadAll` buffers it, and the `*http.MaxBytesError` is caught via
  `errors.As` to return `413` rather than a generic `400`.

## 5. Compared to what you know

- **`PostWithin(ctx, q, e)` vs `PostEntry`** is the difference between a repository
  method that *joins* an ambient transaction and one that *starts* its own — like
  Spring's `@Transactional(propagation = MANDATORY)` (must run in a caller's tx) vs
  `REQUIRES_NEW`. Except there's no annotation or thread-local `TransactionSynchronization
  Manager`: the transaction handle is the explicit `q` argument you thread through.
- **The idempotency middleware** is Stripe's `Idempotency-Key` header, which is the
  reference implementation of this exact pattern. The state machine (claim → run →
  cache → replay) is identical.
- **`responseRecorder`** is a servlet `HttpServletResponseWrapper`, or Express
  monkey-patching `res.write`/`res.end` to buffer output — a decorator over the response
  object. Go's version is a plain struct satisfying a 3-method interface, no inheritance.
- **Keyset pagination** is the "seek method" every serious pagination guide recommends
  over `OFFSET`; it's cursor-based paging as GraphQL Relay connections specify it
  (`after: cursor`), and the opaque base64 token is the same idea as a Relay cursor.
- **Closure-captured result variables** are like a Java lambda writing to an effectively-
  final array element (`result[0] = ...`) to escape the lambda's single return — Go just
  lets you assign the captured local directly.
- **`select { case <-ctx.Done() ... case <-ticker.C ... }`** is a cancellable
  `ScheduledExecutorService` task, or `setInterval` with an `AbortController`. The context
  is the abort signal, threaded explicitly.

## 6. Gotchas & idioms

- **Nullable columns become `*T` with `omitempty`, not `{Valid, ...}` wrappers.**
  `toPaymentResponse` maps sqlc's `uuid.NullUUID`/`sql.NullTime` to `*uuid.UUID`/`*time.Time`
  so the JSON omits absent fields entirely. Watch the aliasing guard: `id := p.ReversalEntryID.UUID;
  resp.ReversalEntryID = &id` copies into a *fresh* local before taking its address —
  taking `&p.ReversalEntryID.UUID` directly would be fine here, but the copy-then-address
  habit is what keeps you safe when the same pattern appears inside a `range` loop over a
  reused loop variable.
- **A request body is a one-shot reader.** `withIdempotency` needs the body twice (to hash
  it *and* to hand to the handler), so it `io.ReadAll`s once and re-wraps with
  `io.NopCloser(bytes.NewReader(body))`. Reading `r.Body` a second time without this yields
  zero bytes — a classic HTTP-in-Go trap.
- **`errors.As` for the typed error, `errors.Is` for the sentinel.** `errors.As(err,
  &tooLarge)` pulls a concrete `*http.MaxBytesError` out of the chain to distinguish
  "too large" (413) from a generic read failure (400) — same idiom as the ledger's
  `*pq.Error` extraction.
- **`sql.ErrNoRows` is a *signal*, not always an error.** `Begin` treats it as "key already
  claimed" and `CancelPayment` treats it as "lost the cancel race" — both `errors.Is`-check
  for it and branch, rather than propagating it as a 500. Meanwhile `Get` maps the *same*
  `sql.ErrNoRows` to `ErrPaymentNotFound` (404). The row is the same; the meaning is
  context-dependent.
- **`INSERT ... ON CONFLICT DO NOTHING RETURNING *` returns zero rows on conflict**, which
  sqlc surfaces as `sql.ErrNoRows` for a `:one` query. That's the entire fresh-vs-existing
  discriminator in `Begin` — no separate existence check needed.
- **`:execrows` vs `:exec`.** `DeleteExpiredIdempotencyKeys` is `:execrows` so sqlc returns
  the affected-row count (`int64`), letting the sweeper log how many keys it reaped;
  `CompleteIdempotencyKey` is plain `:exec` because nobody needs the count.
- **The generic error envelope withholds detail on purpose.** Every `writeError` message is
  generic ("insufficient funds for this payment") — amounts, balances, and account internals
  go to the *log*, never the wire, mirroring the ledger's `logResult` discipline. `422` was
  chosen over `409` for insufficient funds deliberately: the request is well-formed and
  understood, it just can't be satisfied, and retrying it unchanged won't help (which is what
  `409 Conflict` would imply).
- **Reads run on the pool, writes run through the transactor.** `Get`/`List` use
  `db.New(s.db)` (implicit single-statement transactions — no explicit tx needed for one
  `SELECT`); only `Create`/`Cancel` open an `ExecTx`. Don't reflexively wrap every read in a
  transaction.

## 7. Check yourself

1. `PostWithin` was *extracted* from `PostEntry` rather than payments calling
   `PostEntry` directly. Why can't payments just call `PostEntry` and then insert its
   row? What exactly would be non-atomic?
2. The idempotency `Complete` runs on the pool, *not* inside the payment's `ExecTx`.
   Give the concrete failure sequence that leaves a key stuck `in_flight`, explain why
   it's nonetheless "safe," and describe the transactional fix and the property it
   restores.
3. In `withIdempotency`, delete the line `completed = true` (leave everything else).
   What now happens to the idempotency key on a *successful* create, and what does the
   client observe on the next retry?
4. `List` asks the database for `limit+1` rows. Walk through what `rows = rows[:limit]`
   does to the slice header and backing array, and explain why the row you dropped is
   the right source for `next_cursor`.
5. The list SQL uses `(created_at, id) < ($1, $2)` instead of `created_at < $1 AND id <
   $2`. Construct two payments and a cursor where the second, wrong form skips or
   duplicates a row.
6. `RunSweeper` passes the request-scoped... no — it passes the *run* `ctx` to
   `SweepExpired`, but `withIdempotency`'s panic cleanup uses `context.WithoutCancel(ctx)`.
   Why the difference? What breaks if you swap them?

<details>
<summary>Answers</summary>

1. `PostEntry` opens and *commits* its own transaction. If payments called it and then
   inserted the `payments` row, the journal entry would already be committed by the time
   the payment insert runs — so a crash (or a failed insert) between them leaves money
   moved with no payment record. `PostWithin` doesn't commit; it runs on the caller's `q`,
   so both writes commit together in the caller's single `ExecTx`.
2. Sequence: `ExecTx` commits the payment successfully, then the separate
   `Complete` UPDATE fails (DB blip, timeout). The key is still `in_flight`, so same-key
   retries get `409` until the sweeper reaps it after 24h. It's safe because the payment
   is recorded and its ledger `UNIQUE(kind, external_ref)` would reject a re-post anyway —
   no double charge in-window. The fix: write the response snapshot (status + body) inside
   the payment's `ExecTx`, so "payment committed" and "response cached" are one atomic
   commit; that restores the invariant *committed payment ⇒ replayable completed key*.
3. On a successful create, `completed` stays `false`, so the deferred cleanup runs and
   `Delete`s the key it just completed — releasing the claim. The next retry finds no key,
   treats itself as fresh, and *runs the payment again* — a double charge. The flag is the
   only thing distinguishing "panicked, release it" from "succeeded, keep it."
4. `rows[:limit]` returns a new slice header with `len = limit`, same `cap` and same
   backing-array pointer — an O(1), zero-copy narrowing; the `limit+1`th element is still
   in the array, just no longer addressable through this slice. The dropped row is the
   *first row of the next page*, so the last *kept* row (`rows[limit-1]`) is the correct
   cursor: the next `ListPaymentsAfter` will return everything strictly older than it,
   starting with exactly the row we dropped.
5. Say two rows: A = (created_at=10:00, id=ffff) and B = (created_at=09:00, id=0001), and a
   cursor at A. Correct form `(created_at,id) < (10:00, ffff)` returns B (good). The wrong
   form `created_at < 10:00 AND id < ffff` also returns B here — but flip it: cursor at
   (10:00, 0001) with a row C = (09:00, ffff). Tuple form returns C (09:00 < 10:00, correct).
   Wrong form requires `id < 0001`, which C (id=ffff) fails, so C is *skipped* even though
   it sorts after the cursor. The per-column `AND` conflates the tiebreaker with the primary
   key.
6. The run `ctx` is the process lifetime; the sweeper *should* stop when it's canceled
   (that's shutdown), so passing it is correct. The panic cleanup must run *even though* the
   request context is (or is about to be) canceled — using the live request ctx there would
   make `Delete` fail with `context.Canceled` and leave the key stuck, the exact bug the
   cleanup exists to prevent. Swap them and: the sweeper would ignore shutdown (leaking a
   goroutine and a ticker), and the panic cleanup would silently fail to release claims
   under timeout.

</details>

## 8. Further reading

- [Stripe — Designing robust and predictable APIs with idempotency](https://stripe.com/blog/idempotency)
  — the canonical treatment of the claim/complete/replay state machine this middleware implements.
- [`net/http` `ResponseWriter` docs](https://pkg.go.dev/net/http#ResponseWriter) and
  [`httptest.ResponseRecorder`](https://pkg.go.dev/net/http/httptest#ResponseRecorder) — the
  contract `responseRecorder` faithfully reproduces.
- [`context.WithoutCancel`](https://pkg.go.dev/context#WithoutCancel) — detaching cleanup work
  from a canceled request.
- [Use the Index, Luke — No Offset (keyset pagination)](https://use-the-index-luke.com/no-offset)
  — why row-value seek beats `OFFSET`, with the index-scan reasoning.
