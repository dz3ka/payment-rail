# M4 — The domain-event backbone: an atomic outbox drained at-least-once, and a signed webhook fan-out that dead-letters instead of dropping

> Scope: this lesson is about *reliably turning an in-transaction state change into
> an out-of-process notification* without ever losing an event and without ever
> needing a distributed transaction. The headline achievements are (1) the
> **transactional-outbox pattern** — `outbox.Emit` appends an event row through the
> *same* `db.Querier` that mutated the aggregate, so the payment/settlement write and
> its intent-to-publish either both commit or both roll back; (2) an **at-least-once
> relay** whose `claim → publish → mark-sent` runs inside one Postgres tx with
> `FOR UPDATE SKIP LOCKED`, so a publish failure leaves the row unsent and a crash
> re-drives it, never double-marking; (3) a **webhook consumer that classifies poison
> vs transient** so a malformed message is skipped-and-committed while a DB blip is
> left uncommitted and redelivered, leaning on a `UNIQUE(event_id, subscription_id)`
> fan-out to make redelivery idempotent; and (4) a **delivery worker** that signs
> each POST with HMAC-SHA256, retries with clamped exponential backoff, and
> **dead-letters** after a threshold instead of retrying forever — with an operator
> **replay** path to re-drive a fixed endpoint. Everything transport (Kafka, net/http)
> lives behind narrow ports in `cmd/`, exactly the seam discipline the M2 adapter
> established; if `evm.Signer`/`ethRPC` as consumer-owned interfaces isn't reflexive
> yet, skim `m2-evm-chain-adapter.md` first.

## 1. What we built

M4 is the event spine that lets the rest of the system react to payments without
polling the database. It has three moving parts. The **outbox** (`internal/outbox`)
is a tiny, transport-free library: a producer inside a write transaction hands
`Emit` an `Event`, `Emit` wraps it in a versioned `Envelope` and appends one row to
the `outbox` table via the *same* `db.Querier`, and that's it — the package imports
only stdlib, `uuid`, and `internal/db`. The **relay** (`cmd/outboxrelay` +
`outbox.Relay`) polls that table, claims a batch of unsent rows, publishes each
envelope verbatim to a Kafka topic keyed by aggregate id, and stamps the batch
`sent_at`. The **webhook service** (`cmd/webhookd` + `internal/webhook`) consumes
that topic, fans each event out to one `webhook_delivery` row per matching
subscription, then a worker claims due rows, signs and POSTs them, and records
success / a scheduled retry / a dead-letter.

The layering is strict in a way worth noticing before the details: `internal/outbox`
imports only stdlib, `uuid`, and `internal/db`, and the Kafka client (`segmentio/kafka-go`)
lives in exactly two files — `cmd/outboxrelay/kafka.go` and `cmd/webhookd/kafka.go` — behind
one-method ports. `net/http` is similarly confined to `cmd/webhookd/httpsender.go`. That is
why every test in this milestone runs with in-memory fakes: no broker, no database, no HTTP
server, anywhere.

The reason this shape exists at all is a failure that a newcomer to distributed
systems reliably re-invents: *write the row, then publish the event.* Those are two
systems (Postgres and Kafka) and there is no atomic commit across them. Crash between
the two and you have either a state change nobody was told about (write succeeded,
publish didn't) or a lie on the wire (publish succeeded, the tx rolled back). The
transactional outbox collapses the two writes into one — the event row lives in the
same database as the aggregate, so one `COMMIT` covers both — and defers the
cross-system hop to a separate process that can be retried safely because it's
idempotent on the receiving side.

The part to study hardest is the **at-least-once seam repeated at three boundaries**:
DB→relay (`drainBatch`), relay→Kafka (the producer), and Kafka→delivery-rows (the
consumer). At each hop the rule is identical — *do the visible work, and only then
record that you did it; if anything fails in between, leave the marker unset so the
next attempt redoes it.* At-least-once plus an idempotent consumer is the whole
contract; the alternative (exactly-once) would need distributed transactions the code
deliberately refuses. The gate tests pin every branch of it without a database, a
broker, or an HTTP server, because each seam is a pure function behind a narrow
interface.

## 2. The design decision

### Decision A: the transactional outbox — one `db.Querier`, one commit, event and state together

**The problem.** `payments.Create` inserts a payment row and a ledger journal entry
in one transaction, and it wants to publish a `payment.created` event. If it published
to Kafka *inside* the handler, there is no way to make "commit the DB tx" and "the
broker accepted the message" atomic. The two orderings both lose:

- *Write, then publish.* Commit succeeds, the process dies before the Kafka send —
  the payment exists and no consumer will ever hear about it. Silent event loss.
- *Publish, then write.* The broker accepts the event, the DB tx then rolls back
  (constraint violation, deadlock, crash) — consumers act on a payment that does not
  exist. A phantom event.

**The chosen approach — append the event to an `outbox` table through the same
`db.Querier` the aggregate write used, so both are in one transaction.** `Emit` never
opens its own transaction; it takes a `q db.Querier` and inserts through it:

```go
func Emit(ctx context.Context, q db.Querier, e Event) error {
	id := uuid.New()
	env := Envelope{
		ID:            id.String(),
		Type:          e.Type,
		AggregateType: strings.SplitN(e.Type, ".", 2)[0],
		OccurredAt:    time.Now().UTC(),
		SchemaVersion: schemaVersion,
		Data:          e.Data,
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("outbox: emit %s: %w", e.Type, err)
	}
	if err := q.InsertOutboxEvent(ctx, db.InsertOutboxEventParams{
		ID: id, EventType: e.Type, AggregateID: e.AggregateID, Payload: payload,
	}); err != nil {
		return fmt.Errorf("outbox: emit %s: %w", e.Type, err)
	}
	return nil
}
```

The call site is the load-bearing part. In `payments.Create` the emit is the *last*
statement inside the existing `ExecTx` closure:

```go
err := s.store.ExecTx(ctx, func(q db.Querier) error {
	// ... insert payment + journal entry through q ...
	return outbox.Emit(ctx, q, outbox.Event{
		Type: "payment.created", AggregateID: pid.String(), Data: paymentEvent{...},
	})
})
```

Because `Emit` writes through the same `q`, the outbox row is part of the same
transaction. Either the payment, the journal entry, *and* the event row all commit,
or none do. There is no window. This is the textbook **transactional outbox**, and
the `db.Querier` seam from M1 is exactly what makes it composable — `Emit` doesn't
know or care whether `q` is a `*sql.Tx`-backed `*db.Queries` or a fake; it just
appends.

Two subtle correctness points in the emit *placement*, visible in the settlement
diff. First, every emit sits *after* the state-transition query and *inside the
branch that actually flipped the row*: `settle` emits only after `MarkSettlementSettled`
succeeded, and the comment notes that a redelivered confirm short-circuits at the
status guard above and never reaches the emit. That's what makes emission
**exactly-once per real transition** even though the settlement pipeline itself is
at-least-once (chainwatcher can redeliver a confirmation). Second, `payments.Create`
can emit *unconditionally* because `pid` is freshly generated each call, so a fresh
insert is always a real transition — there is no idempotent-replay path to guard. The
rule generalizes: *emit inside the tx, in the branch that observed the transition.*

`payments.Cancel` is the sharpest illustration of the failure the rule prevents. Hoist its
`Emit` to the *top* of the `ExecTx` closure, before the guarded `CancelPayment`, and it still
compiles and still passes the happy path — but a redelivered cancel, which matches no row and
returns `sql.ErrNoRows`, now publishes a phantom `payment.canceled` for a payment that did
not transition. `TestCancel_AlreadyCanceledEmitsNothing` locks the correct behavior: the
second cancel returns `ErrPaymentNotCancelable` and adds exactly zero outbox rows.

**Alternative 1: dual-write with a best-effort publish and a `defer`/retry.** Publish
to Kafka right after commit, and on failure log-and-retry in the background. This is
the most common naive design and it's simply lossy: the retry queue is in memory, so a
crash between commit and a successful publish drops the event. You've reintroduced the
"write, then publish" gap with extra machinery.

**Alternative 2: change-data-capture (Debezium tailing the WAL).** Instead of an
application-level outbox, stream the aggregate table's binlog/WAL to Kafka. This is a
legitimate production pattern and *does* give you atomicity for free (the row is the
event). It was rejected here for cost and coupling: it binds the event schema to the
physical table layout (every column rename is a breaking wire change), needs a
Debezium/connect cluster to operate, needs replication slots managed (a stuck slot fills the
disk), and makes the "what exactly is the public event contract" question implicit. The
application outbox keeps the envelope an explicit, versioned artifact (`schema_version`, a
curated `paymentEvent` body that deliberately omits `journal_entry_id`) that evolves
independently of the tables. Worth noting the upgrade path stays open: if throughput ever
demands it, **CDC *on the outbox table*** is the natural next step — the same rows,
WAL-tailed instead of polled, with the envelope shape unchanged.

**The cost, stated honestly.** The outbox needs a relay process and a polling loop,
and it is at-least-once, not exactly-once — a consumer *will* occasionally see a
duplicate. That cost is paid down by consumer idempotency (Decision C), which is
cheaper and more robust than chasing exactly-once delivery.

### Decision B: the relay drains inside one tx — `claim → publish → mark`, publish held *inside* the transaction

**The problem.** The relay reads unsent rows and publishes them. When does it stamp a
row `sent_at`? Stamp before the Kafka ack and a publish failure loses the event
(marked sent, never delivered). Stamp after, but in a *separate* transaction, and a
crash between publish and mark re-publishes on restart — which is fine (at-least-once)
but you also want two relay replicas to not both grab the same rows.

**The chosen approach — `drainBatch` runs `claim → publish → mark` and returns before
`MarkOutboxSent` on any publish error, and the whole thing runs inside one `ExecTx`.**

```go
func drainBatch(ctx context.Context, q outboxQuerier, p Producer, batch int32) (published int, err error) {
	rows, err := q.ClaimUnsentOutbox(ctx, batch)
	if err != nil { return 0, err }
	if len(rows) == 0 { return 0, nil } // clean no-op: no broker round-trip

	msgs := make([]Message, len(rows))
	ids := make([]uuid.UUID, len(rows))
	for i, row := range rows {
		msgs[i] = Message{Key: []byte(row.AggregateID), Value: []byte(row.Payload)}
		ids[i] = row.ID
	}
	if err := p.Publish(ctx, msgs); err != nil {
		return 0, err // publish failed: return BEFORE Mark, tx rolls back, rows stay unsent
	}
	_, markErr := q.MarkOutboxSent(ctx, ids)
	return len(rows), markErr
}
```

The claim query is where the concurrency safety lives:

```sql
-- ClaimUnsentOutbox
SELECT * FROM outbox WHERE sent_at IS NULL
ORDER BY created_at LIMIT $1
FOR UPDATE SKIP LOCKED;
```

`FOR UPDATE` row-locks the claimed rows for the duration of the transaction;
`SKIP LOCKED` tells Postgres *"don't wait on rows another transaction already
locked — skip them and take the next."* Together they let N relay replicas each grab
a **disjoint** batch with no coordination and no blocking: replica A locks rows 1–100,
replica B's identical query skips those locked rows and takes 101–200. Without
`SKIP LOCKED` the second relay would *block* on A's lock and then re-select the same
rows once A commits, doing redundant work; without `FOR UPDATE` both would select the
same batch and double-publish every event.

Holding `Publish` *inside* the transaction is the deliberate move. Because the tx is
still open when we publish, a publish error returns before `MarkOutboxSent`, so
`ExecTx` rolls the whole tick back — the `sent_at` stamp never happens and the rows
are re-claimed next tick. This is at-least-once *by construction*: the marker is only
committed on the same commit that the publish preceded. Note the honest imperfection:
the Kafka write is not itself transactional with Postgres, so a crash *after* the
broker acks but *before* the DB commit will re-publish those rows on restart. That's a
duplicate, not a loss — exactly the trade the design accepts, and exactly what the
consumer's idempotency (Decision C) absorbs.

The relay's `Run` loop closes the reliability story: a drain error that isn't context
cancellation is *logged and swallowed*, and the loop continues — the rows stayed
unsent, so a transient broker or DB blip self-heals next tick rather than killing the
process:

```go
if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
	r.log.Error("outbox drain failed", "err", err)
}
```

**Alternative: publish first, then a separate mark transaction.** Splitting publish
and mark into two txs means a crash in between re-publishes (same at-least-once
guarantee) — but you lose `SKIP LOCKED`'s clean multi-replica story unless you add a
`claimed_by`/lease column, and you open a wider duplicate window. Keeping mark in the
same tx as the claim, with publish sandwiched between, is the minimal correct shape.

**Alternative: exactly-once producer semantics (Kafka transactions / the idempotent
producer).** It would shrink the duplicate window, and it was rejected as *redundant* rather
than as too hard. The only duplicate window here is "publish succeeded, then the mark or the
commit failed → republish next tick," and the consumer already dedupes on the envelope `id`.
Paying for a transactional producer to remove duplicates that the consumer filters anyway
buys nothing.

**The latent constraint this design carries — write it on the wall.** Per-aggregate ordering
holds **for a single relay instance only.** The message key is `aggregate_id` with a `Hash`
balancer, so all events for one payment land on one partition, and the claim's
`ORDER BY created_at` publishes them in order — *as long as one drainer is doing the
claiming.* Run two relay replicas and `FOR UPDATE SKIP LOCKED`, the very clause that makes
multi-replica claiming safe for *throughput*, hands each replica an interleaved batch: two
events for the same aggregate can be published, and therefore land on the partition, out of
order. A settle → reorg → re-settle sequence could arrive as reorg → settle. `SKIP LOCKED`
makes replicas *worse* here, not better, and that is counterintuitive enough to be worth
stating explicitly. Today the relay is a deliberate singleton; scaling it out first requires
per-aggregate claim affinity (hash the aggregate to a worker) or a decision to accept only
partition-level ordering. This is a documented constraint, not a bug — but it is the kind of
constraint that gets violated by a routine "let's bump replicas to 3" change.

There is also a smaller ordering nuance in `drainBatch` itself: the `for` loop that builds
`msgs` preserves the claim order into the published batch, so the `ORDER BY created_at`
guarantee survives the Go side. And `Value` is `row.Payload` forwarded **verbatim** — the
relay never unmarshals, inspects, or logs the envelope, which keeps amounts out of the logs
and makes the relay a dumb, generic pipe.

### Decision C: the consumer classifies *poison vs transient*, and the fan-out is idempotent

**The problem.** The webhook consumer reads a Kafka message and fans it out to
delivery rows. Two failure modes need opposite handling. A **malformed** message
(garbage JSON, a non-UUID event id) will *never* succeed no matter how many times you
retry — committing its offset and moving on is correct; blocking the partition on it
forever is a self-inflicted outage. A **transient** failure (the database is down
during the fan-out insert) *will* succeed later — you must *not* commit the offset, so
the message is redelivered.

**The chosen approach — a sentinel `ErrPoisonMessage` the loop matches with
`errors.Is`, and a fan-out insert that is idempotent on redelivery.** `Handle` wraps
parse failures in the sentinel and returns DB errors raw:

```go
func Handle(ctx context.Context, q fanOutQuerier, value []byte, log *slog.Logger) error {
	env, err := outbox.ParseEnvelope(value)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPoisonMessage, err) // poison: loop skips + commits
	}
	eventID, err := uuid.Parse(env.ID)
	if err != nil {
		return fmt.Errorf("%w: bad event id %q: %w", ErrPoisonMessage, env.ID, err)
	}
	n, err := q.FanOutDelivery(ctx, db.FanOutDeliveryParams{
		EventID: eventID, EventType: env.Type, Payload: json.RawMessage(value),
	})
	if err != nil {
		return fmt.Errorf("webhook: fan out %s: %w", eventID, err) // transient: NOT poison
	}
	return nil
}
```

The consume loop reads the classification and decides whether to commit the offset:

```go
herr := webhook.Handle(ctx, q, m.Value, log)
if herr != nil && !errors.Is(herr, webhook.ErrPoisonMessage) {
	return fmt.Errorf("webhookd: fan-out failed at offset %d, redelivering: %w", m.Offset, herr)
}
if err := c.reader.CommitMessages(ctx, m); err != nil { ... } // commit on success OR poison
```

The idempotency that makes redelivery safe is in the SQL, not the Go:

```sql
-- FanOutDelivery
INSERT INTO webhook_deliveries (event_id, subscription_id, event_type, payload)
SELECT $1, s.id, $2, $3
FROM webhook_subscriptions s
WHERE s.active AND $2 = ANY(s.event_types)
ON CONFLICT (event_id, subscription_id) DO NOTHING;
```

The `UNIQUE (event_id, subscription_id)` constraint (from migration 0006) plus
`ON CONFLICT DO NOTHING` means that if the same event is redelivered — because the
relay double-published it, or because a crash left the offset uncommitted — the second
fan-out **collapses onto the existing rows and inserts nothing**. This is the
idempotent consumer that pays for at-least-once: the whole pipeline can deliver a
message twice and the delivery table is unchanged the second time. The single
`INSERT ... SELECT` also does the fan-out in one statement — one event to N matching
subscriptions — with no read-modify-write race.

Note the transient-error handling deliberately *returns the error up* rather than
`continue`-ing. Returning cancels the errgroup, `service.Run` exits non-zero, and the
supervisor restarts the reader, which resumes from the last committed offset and
re-processes the uncommitted message. The comment in `kafka.go` is explicit that this
is chosen over sleep-and-retry-same-message because it's "the simplest strictly-correct
at-least-once behaviour" — a bare `continue` would advance past an uncommitted message
and lose it.

**Alternative: a single "retry everything" policy.** Treating malformed messages the
same as transient ones (never commit, always redeliver) wedges the partition on the
first bad message forever — every later event queues behind a message that can never
succeed. Treating transient the same as poison (always commit) silently drops events
when the DB hiccups. The two-way classification is the minimum needed to be both
non-blocking and non-lossy.

### Decision D: HMAC-SHA256 over `"<t>.<body>"`, written as raw bytes not `%s`

**The problem.** A subscriber receiving a POST needs to verify (a) it really came from
us and (b) the body wasn't tampered with in flight. And we must not mangle the signed
bytes: the payload is arbitrary JSON that could contain non-UTF-8 or be
byte-for-byte significant.

**The chosen approach — a Stripe-style `t=<unix>,v1=<hex(HMAC_SHA256(secret,
"<t>.<body>"))>` header, with the body `Write`n as raw bytes.**

```go
func Sign(secret []byte, t int64, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "%d.", t)  // the timestamp prefix and literal "."
	mac.Write(body)             // raw bytes — NOT %s, never coerced/re-encoded
	return fmt.Sprintf("t=%d,v1=%s", t, hex.EncodeToString(mac.Sum(nil)))
}
```

Three deliberate choices. First, **HMAC, not a bare hash.** `sha256(secret+body)` is
vulnerable to length-extension and doesn't cleanly bind the secret; HMAC is the
construction designed for "authenticate a message with a shared key." Go's
`crypto/hmac` is a `hash.Hash`, so you `Write` into it like any hash and `Sum(nil)`
finalizes. Second, **the timestamp is inside the signed content** (`"<t>.<body>"`),
not just alongside it — that lets a subscriber reject stale/replayed requests by
checking `t` is recent *and* covered by the MAC, so an attacker can't replay an old
signed body with a fresh timestamp. Third, **the body goes through `mac.Write(body)`,
not `fmt.Fprintf(mac, "%s", body)`** — `%s` on a `[]byte` is mostly a passthrough but
invites someone to "clean up" the code into a format string that coerces or escapes;
writing raw bytes guarantees the MAC covers exactly the bytes on the wire. The signing
secret is per-subscription (`webhook_subscriptions.signing_secret`), so compromise of
one subscriber's secret doesn't forge messages to another.

The known-answer test pins the construction against an independently computed vector
(`python hmac`), so any accidental change to the signed layout breaks CI:

```go
want = "t=1700000000,v1=c89214b5b5da833daed6f0b8c5bb6bd58cea9022bd80ccc78230f3942d632925"
```

### Decision E: clamped exponential backoff, a bounded retry count, and a dead-letter with replay

**The problem.** A subscriber endpoint can be down for a second (deploy) or for a week
(decommissioned). Retrying a dead endpoint forever burns the worker and never drains
the queue; giving up after one failure drops events on a transient blip. And a naive
`base << (attempts-1)` overflows `int64` once `attempts` gets large.

**The chosen approach — exponential backoff clamped to a cap, a `maxAttempts`
dead-letter threshold, and an operator replay query.** The backoff doubles in a loop
that clamps the instant it reaches the cap, so the running value can never overflow:

```go
func backoff(attempts int32) time.Duration {
	if attempts < 1 { attempts = 1 }
	d := backoffBase // 2s
	for i := int32(1); i < attempts; i++ {
		d *= 2
		if d >= backoffCap { return backoffCap } // 1h; return before doubling past int64
	}
	return d
}
```

`classify` turns a send result into one of three fates, and it's a pure function so
every branch is unit-tested without a worker:

```go
func classify(newAttempts int32, res SendResult, err error) outcome {
	if err == nil && res.StatusCode >= 200 && res.StatusCode < 300 {
		return outcomeSucceeded
	}
	if newAttempts >= maxAttempts { // 8
		return outcomeDeadLetter
	}
	return outcomeRetry
}
```

Below the threshold a failure schedules the next attempt via `MarkDeliveryRetry` with
`next_attempt_at = now + backoff(newAttempts)`; at the threshold it moves the row to
`status='dead_letter'` via `MarkDeliveryDeadLettered`, which **stops** retrying and
parks the row for a human. The claim query only ever selects `status='pending' AND
next_attempt_at <= now()`, so a dead-lettered row is simply invisible to the worker
until an operator resurrects it. `ReplayDeadLettered` is that resurrection — a
one-shot `paymentrailctl replay-webhook --subscription-id` after a broken endpoint is
fixed:

```sql
UPDATE webhook_deliveries
SET status = 'pending', attempts = 0, next_attempt_at = now(), last_error = NULL, updated_at = now()
WHERE status = 'dead_letter' AND subscription_id = $1;
```

**Alternative: unbounded retry, or a fixed retry-N-times-then-drop.** Unbounded retry
never lets go of a permanently dead endpoint and the pending queue grows without
bound. Retry-then-drop loses the events silently, which for a payments notification is
unacceptable — you want the events *parked and recoverable*, not gone. Dead-lettering
is the pattern that turns "give up" into "hand to an operator," and the replay query is
what closes the loop. The honest shortcut here: replay resets `attempts = 0`, so a
still-broken endpoint will re-dead-letter after another `maxAttempts` failures — replay
is an operator's explicit "I fixed it, try again," not an automatic recovery.

### Decision F: the delivery worker leases claimed rows for crash recovery

**The problem.** A worker claims a due row, POSTs it (up to a 10s HTTP timeout), then
marks the outcome. If the worker crashes *after* the POST but *before* the mark — or if
two worker replicas run — a row could be delivered twice in a tight window, or a
claimed row could be orphaned forever.

**The chosen approach — `ClaimDueDeliveries` pushes each claimed row's
`next_attempt_at` forward by a lease (`leaseSeconds = 60`) in the same UPDATE that
claims it, again under `FOR UPDATE SKIP LOCKED`.**

```sql
UPDATE webhook_deliveries d
SET next_attempt_at = now() + make_interval(secs => sqlc.arg(lease_seconds)::int), updated_at = now()
FROM webhook_subscriptions s
WHERE d.subscription_id = s.id
  AND d.id IN (
      SELECT id FROM webhook_deliveries
      WHERE status = 'pending' AND next_attempt_at <= now()
      ORDER BY next_attempt_at LIMIT sqlc.arg(claim_limit)::int
      FOR UPDATE SKIP LOCKED
  )
RETURNING d.id, d.event_id, d.attempts, d.payload, s.url, s.signing_secret;
```

Claiming *is* leasing: the moment a row is claimed its `next_attempt_at` jumps 60s into
the future, so no other worker (and no later tick of the same worker) will re-claim it
for 60s. If the worker finishes and marks the outcome, the row leaves the `pending`
window entirely. If the worker *crashes* mid-delivery, the lease expires after 60s and
the row becomes due again — self-healing crash recovery with no separate reaper. The
`SKIP LOCKED` inner select means two worker replicas grab disjoint batches, and the
`JOIN` to `webhook_subscriptions` in the same statement returns the `url` and
`signing_secret` alongside the delivery, so the worker needs exactly one round-trip to
get everything it needs to sign and send.

This is at-least-once again (a crash after POST re-delivers after the lease) — the same
duplicate-tolerant trade as everywhere else, and the subscriber is expected to dedupe
on the `X-Payment-Rail-Event-Id` header the sender sets.

**Why it has to be *one* statement.** The select and the lease-bump look separable, and
splitting them is the natural first draft: `SELECT` the due rows, then `UPDATE` their
`next_attempt_at`. The window between those two statements is a race. Worker A runs its
`SELECT` and gets row R. Before A's `UPDATE` lands, worker B runs the identical `SELECT` —
R is still due, so B gets R too. Both POST R, and the subscriber sees the delivery twice on
*every* tick, not just after a crash. Fusing them into one `UPDATE … WHERE id IN (SELECT …
FOR UPDATE SKIP LOCKED) RETURNING` closes it from both sides: B's inner select skips the
rows A's statement has locked, *and* R's `next_attempt_at` has already moved forward by the
time any lock is released. **Claim and schedule must be the same atomic act, because the
schedule *is* the claim.**

### Decision G: the consume loop and the delivery worker are two stages joined by a *table*, not a function call

**The problem.** The obvious implementation of "consume an event and notify subscribers" is
one loop: fetch a message, POST it, retry with backoff until it succeeds, commit the offset.
Everything is in one place and there is no queue table to maintain. It is also wrong, and
the reason is the most transferable thing in this milestone.

**Kafka consumption is partition-ordered with a single commit pointer.** Block that loop on
a backoff — and this design backs off up to **one hour** — and three things follow at once.
Every *later* event on that partition is stuck behind one slow subscriber, including events
for entirely unrelated payments and subscriptions that merely share a partition. The offset
cannot advance, so a restart re-reads everything since the last commit, and the broker may
decide the consumer is dead (session timeout) and rebalance the partition away mid-backoff.
And fan-out is one-to-many: a single event can map to fifty subscriptions, so inline
delivery serializes fifty independent POSTs, each with its own retry schedule, behind one
offset.

**The chosen approach — the consume loop does the minimum durable thing and hands off.**
Parse the envelope, `FanOutDelivery` one row per matching active subscription, commit the
offset. Ownership of the slow, unreliable part moves to a separate poll-loop worker reading a
database queue, where **the row itself is the retry state**: `attempts`, `next_attempt_at`,
`status`, `last_error`. The two stages never call each other; they communicate only through
`webhook_deliveries`. They happen to run in one process under one `errgroup`, but nothing
would change if they were separate deployments. This is the classic store-and-forward shape,
and it is the same reason the relay is a separate process from the domain writers.

**Alternative: one goroutine per in-flight delivery.** Spawn a goroutine per due row and let
the scheduler juggle them. That trades a bounded, *observable* DB queue for an unbounded,
*invisible* in-memory one: a burst of events or a wall of slow endpoints spawns thousands of
goroutines and open sockets with no backpressure, and a crash loses every in-flight attempt
because the only record of it lived in a goroutine stack. Claiming a bounded batch per tick
(`claimBatch = 100`) gives natural backpressure and survives restarts, because all the state
is in Postgres.

**And note that this deliberately inverts Decision B.** The relay holds its Kafka publish
*inside* the DB transaction and the row lock. The worker does the opposite: claim, release
the lock, POST outside any transaction, then a second short statement records the outcome.
That is not inconsistency — it is the *same* rule applied to two different callees. The
relay's callee is a Kafka broker: trusted infrastructure you operate, with predictable
latency, so holding a transaction across it buys at-least-once for free. The worker's callee
is an arbitrary third-party HTTP endpoint that may hang for the full 10-second timeout;
holding a Postgres row lock and connection across *that* would pin one of each per in-flight
delivery, and a wall of slow subscribers would exhaust the pool. **Whether you may hold a
lock across a call is a property of the callee's trust and latency, not a house style.** The
lease is what makes releasing the lock early safe.

## 3. Language deep-dive

### 3a. `db.Querier` as the composition seam — the same interface threads through emit, relay, and consumer

The single most important Go idea in M4 is that *every* boundary is a small,
consumer-defined interface, and the concrete `*db.Queries` satisfies all of them
implicitly. `Emit` takes the broad `db.Querier` (it's called inside an existing tx and
may need any query). But the relay and consumer each define a *narrow* slice naming
only the methods they call:

```go
// relay.go
type outboxQuerier interface {
	ClaimUnsentOutbox(ctx context.Context, limit int32) ([]db.Outbox, error)
	MarkOutboxSent(ctx context.Context, ids []uuid.UUID) (int64, error)
}
type transactor interface {
	ExecTx(ctx context.Context, fn func(q db.Querier) error) error
}

// consumer.go
type fanOutQuerier interface {
	FanOutDelivery(ctx context.Context, arg db.FanOutDeliveryParams) (int64, error)
}
```

This is "accept interfaces, return structs" taken seriously. Nothing declares "I
implement `outboxQuerier`" — `*db.Queries` just *has* those methods, so it satisfies
the interface structurally (Go's implicit interface satisfaction, the same mechanism
M2's `signerClient`/`evm.Signer` relied on). The payoff is the test files: `drainBatch`
is exercised with a spy that implements two methods, `Handle` with a `fanOutSpy`, and
the worker with a `markSpy` — no database, no `sql.DB`, no migrations. The interface is
declared *next to the consumer that needs it*, which is the inverse of Java/C# where
the interface usually ships with the provider. Note `transactor` exists purely so
`Relay.Run` can be tested with a fake `ExecTx` that just invokes `fn(fakeQuerier)` — the
transaction boundary itself is mockable because it's an interface, not a naked
`*sql.DB`.

There's a lovely detail in `cmd/webhookd/kafka.go`: it declares its *own*
`webhookFanOut` interface, structurally identical to the package-internal
`fanOutQuerier`, and passes `*db.Queries` typed as it into `webhook.Handle`. Because Go
matches interfaces structurally, a value typed as `webhookFanOut` is accepted anywhere a
`fanOutQuerier` is wanted — two interfaces with the same method set are interchangeable
at the call boundary, no shared declaration needed. That's how the command wiring
stays decoupled from the domain package's unexported interface.

**One level down is the seam that made the outbox nearly free: sqlc's `DBTX`.** sqlc
generates the `Querier` interface and a concrete `*db.Queries` constructed over a tiny
`DBTX` interface — roughly `ExecContext`, `QueryContext`, `QueryRowContext`,
`PrepareContext`. Both `*sql.DB` **and** `*sql.Tx` satisfy `DBTX`, because both expose
exactly those four methods. So the *same* generated query code runs against a pooled
connection or against an open transaction, decided entirely by what `Queries` was
constructed over. `ledger.SQLStore.ExecTx` opens a `*sql.Tx`, builds a `*db.Queries` bound
to it, and hands the closure that as a `db.Querier` — which means the `q` you hold inside
any `ExecTx` closure is *already* transaction-scoped, and emitting an event is one more
method call on it. `Emit` has **no idea** whether it is inside a transaction, and that
ignorance is exactly what makes it correct: there was no transaction manager to thread, no
new interface, no second connection. The atomicity in Decision A is a *free consequence* of
a design choice sqlc and M1's `ExecTx` had already made. If you come from Spring this is the
moment `@Transactional` "just works" because the `EntityManager` is bound to the current
transaction — except here it is explicit and visible: the transaction *is* the `q` you were
handed, and you can read off exactly which writes share it. No thread-local, no proxy.

### 3b. `errors.Is` with a wrapped sentinel to *route* control flow, and `%w: %w` double-wrapping

The consumer's poison-vs-transient decision is pure `errors.Is` plumbing, but note the
*double* `%w`:

```go
return fmt.Errorf("%w: %w", ErrPoisonMessage, err)
```

Go 1.20+ lets `fmt.Errorf` wrap *multiple* errors — this error `Is` both
`ErrPoisonMessage` *and* the underlying `json` error. The loop matches the sentinel to
decide offset behavior:

```go
if herr != nil && !errors.Is(herr, webhook.ErrPoisonMessage) {
	return ... // transient → don't commit → redeliver
}
// fallthrough: nil OR poison → commit offset
```

`errors.Is` walks the wrap chain (now a *tree*, since one error can wrap two) looking
for `ErrPoisonMessage`. The `TestHandleDBErrorIsTransient` test pins the critical
inverse: a DB error must *not* be `errors.Is(err, ErrPoisonMessage)`, because if it were,
the loop would wrongly commit the offset and lose the event. This is the M2 pattern
("wrap a neutral sentinel with `%w`, read it back with `errors.Is`") used not for
logging levels but to *drive at-least-once offset semantics* — the identity of the error
literally decides whether Kafka advances.

### 3c. `sql.NullInt32` / `sql.NullString` — Go's answer to "a column that might be NULL"

The delivery worker records `last_status_code` and `last_error`, both nullable — a
transport failure has no HTTP status, a success has no error. Go models SQL NULL with
the `sql.Null*` wrapper structs, and the worker sets `.Valid` deliberately:

```go
statusCode := sql.NullInt32{}                                   // Valid=false → SQL NULL
if sendErr == nil {                                             // a response arrived
	statusCode = sql.NullInt32{Int32: int32(res.StatusCode), Valid: true}
}
```

`sql.NullInt32` is a two-field struct `{Int32 int32; Valid bool}`. When `Valid` is
false the driver writes SQL `NULL`; when true it writes the `Int32`. This is the
idiomatic Go way to represent "optional column" — there is no `*int32` nullable-pointer
convention in the standard `database/sql` (you *can* use pointers, but the `Null*`
types are the explicit, self-documenting choice, and sqlc generates them). The
semantics are load-bearing: a transport error (`sendErr != nil`) means no response was
received, so `last_status_code` *must* be NULL, not `0` — `0` would be a lie
(a real "status 0"). `TestDrainOnceDeadLetterOnTransportErrorAtThreshold` asserts
exactly `q.dead.LastStatusCode.Valid == false` on a transport error. For a Java/C#
engineer this is `Optional<Integer>` / `int?` — except Go makes the null-ness an
explicit field you set, not an ambient property of the reference.

### 3d. Two loops under one `errgroup`, and context cancellation as clean shutdown

`webhookd` runs the Kafka consume loop and the delivery worker concurrently, and needs
"if either fails for real, stop both; on shutdown, exit cleanly":

```go
g, gctx := errgroup.WithContext(ctx)
g.Go(func() error { return consumer.run(gctx, q, log) })
g.Go(func() error { return worker.Run(gctx) })
if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
	return err
}
return nil
```

`errgroup.WithContext` derives `gctx` that is cancelled the moment *any* `g.Go`
function returns a non-nil error — so if the consumer hits a transient DB error and
returns, `gctx` cancels and the worker's `Run` sees `<-ctx.Done()` and returns nil. This
is Go's structured-concurrency answer to "N goroutines, fail-together": you don't
hand-roll a `chan error` and a `sync.WaitGroup`, you let `errgroup` fan-in the first
error and propagate cancellation. The `!errors.Is(err, context.Canceled)` filter at the
end is the shutdown discipline seen throughout M4: a cancelled context is a *clean stop*
(the operator sent SIGTERM), not a failure, so it's swallowed to a nil return and the
process exits zero. Both `Relay.Run` and `Worker.Run` follow the same `select { case
<-ctx.Done(): return nil; case <-t.C: ... }` ticker shape — the canonical Go
poll-until-cancelled loop, where `defer t.Stop()` releases the ticker on every exit
path.

There is a **deliberate asymmetry** between the two loops that is easy to read as
sloppiness. `consumer.run` *returns* a transient error and lets the errgroup tear the
process down; `Worker.Run` *swallows* its drain errors and keeps looping:

```go
case <-t.C:
    if err := w.drainOnce(ctx); err != nil &&
        !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
        w.log.Error("webhook: delivery drain failed", "err", err)
    }
```

Each stage gets the behavior its own recovery story justifies. The consumer's correctness
depends on *offset ordering*, and the only clean recovery from an unhandled message is to
restart and resume from the last committed offset — so it returns and lets the supervisor do
that. The worker's rows are already **leased**: a transient DB blip during a claim leaves
them safely invisible for 60 seconds and then due again, so escalating to a process restart
would only cancel the consumer for no benefit. **Whether a loop returns or swallows should
follow from what recovers its work, not from a per-repo convention.**

### 3e. `json.RawMessage` — carry bytes through verbatim, don't re-marshal

The consumer stores the *exact* Kafka value bytes as the delivery payload, and the
worker later POSTs those exact bytes and signs over them:

```go
q.FanOutDelivery(ctx, db.FanOutDeliveryParams{ ..., Payload: json.RawMessage(value) })
```

`json.RawMessage` is `type RawMessage []byte` with custom marshal methods that make it a
*passthrough* — when a struct containing it is marshaled, the raw bytes are spliced in
verbatim rather than re-encoded, and when it's a destination it captures the raw bytes
undecoded. Using it here is a *correctness* choice, not an optimization: if the consumer
decoded the envelope and re-marshaled it into the delivery row, the byte layout could
shift (key ordering, whitespace, number formatting) and the signature the worker later
computes over `row.Payload` would cover *different bytes* than a subscriber would need to
verify. By forwarding the verbatim value from Kafka → delivery row → HTTP body → HMAC,
the exact same bytes are signed and sent. The comments hammer this: "published
verbatim," "POSTed verbatim, so the signature the worker computes covers exactly what the
subscriber receives." This is the same "minimize degrees of freedom at a
precision-sensitive boundary" instinct as M2's big-endian-bytes-not-decimal-string
decision, applied to signed bytes.

### 3f. Embedding a nil interface: a strict mock built out of Go's embedding rules

`db.Querier` has roughly twenty-five methods. An outbox test cares about one. Spelling the
other twenty-four out as no-op stubs is pure noise, so the tests do this instead:

```go
type captureQuerier struct {
	db.Querier // embedded interface, deliberately left nil
	got        db.InsertOutboxEventParams
	called     int
	err        error
}

func (c *captureQuerier) InsertOutboxEvent(_ context.Context, arg db.InsertOutboxEventParams) error {
	c.called++
	c.got = arg
	return c.err
}
```

Embedding the `db.Querier` **interface** (not a struct) promotes its entire method set onto
`captureQuerier`, so the type satisfies `db.Querier` without a line of boilerplate. The
embedded field is an interface's zero value, which is `nil`. Then the one method the test
cares about is overridden with a real implementation. Any method the test did *not* override
dispatches to the nil embedded interface — and panics.

**That panic is the feature.** It means "some code path called a `Querier` method this test
did not anticipate," and it fails loudly at the exact offending call rather than returning a
plausible zero value that lets a bug through. This is a strict mock that throws on
unexpected interactions, assembled from the language's embedding rules with no mocking
framework in sight.

The ledger's `fakeQuerier` reaches the same goal from the other direction, and the contrast
is instructive:

```go
func (q *fakeQuerier) InsertOutboxEvent(context.Context, db.InsertOutboxEventParams) error {
	panic("fakeQuerier.InsertOutboxEvent: not used by the ledger domain")
}
```

That fake is domain-complete — it spells every method out — so the boundary it wants to
assert is a *named* one: outbox emission must never originate from inside the ledger domain.
Same defensive intent, two expressions of it: **embed-and-override for a thin capture,
spell-out-and-guard when the fake is a domain model in its own right and the panic is
documenting an architectural rule.**

### 3g. Which seams earn a port, and the compile-time assertion that pins them

M4 adds two real ports and deliberately declines a third. The relay gets one:

```go
// internal/outbox/producer.go
type Producer interface {
	Publish(ctx context.Context, msgs []Message) error
}

// cmd/outboxrelay/kafka.go — the only file that imports kafka-go
type kafkaProducer struct{ w *kafka.Writer }

var _ outbox.Producer = (*kafkaProducer)(nil) // compile-time proof
```

The worker gets another (`webhook.Sender`, with `var _ webhook.Sender = (*httpSender)(nil)`
in `httpsender.go`). But the *consume* side gets **no** `MessageStream` port: the
`*kafka.Reader` stays concrete inside `kafkaConsumer.run`, and only the pure part —
`webhook.Handle`, which takes an already-fetched `value []byte` — is extracted and
exhaustively tested. That is the judgment worth internalizing: **add a port where you have
something to fake or swap; keep it concrete where the abstraction would wrap stdlib with no
second implementation and no test seam it enables.** A `MessageStream` interface would have
exactly one implementation, and the fetch/commit loop would remain untested against it
either way. `Sender` and `Producer` earn their keep — each has a real fake in tests *and*
isolates a vendor client from the domain. `MessageStream` would be ceremony.

`var _ outbox.Producer = (*kafkaProducer)(nil)` is worth reading closely because it recurs
everywhere in this repo: a throwaway variable named `_` (blank, discarded), typed as the
interface, assigned a typed-nil pointer. Nothing is allocated, no method runs, and the
compiler elides it entirely. Its only job is to break the build **in this file** the moment
the adapter stops satisfying the port — rather than at some distant call site, or worse, at
a wiring point that only executes in production. Go has no `implements` keyword because
satisfaction is structural and implicit; this line is how you opt into the compiler check
where it matters.

## 4. What would break

- **Silent event loss / phantom events (the newcomer bug).** Publishing to Kafka
  inside the payment handler instead of appending to the outbox gives you no atomicity:
  a crash between commit and publish loses the event, or a rollback after publish emits
  a phantom. Avoided by emitting through the *same* `db.Querier` so state and event
  share one commit.

- **Double-marked, never-delivered rows.** Stamping `sent_at` before the Kafka ack (or
  in a separate committed tx before publish) loses events on a publish failure.
  `drainBatch` returns before `MarkOutboxSent` on any publish error, inside one tx, so
  the mark only lands on the commit that follows a successful publish.

- **Two relays double-publishing every event.** A plain `SELECT ... WHERE sent_at IS
  NULL` with two replicas selects the same rows. `FOR UPDATE SKIP LOCKED` gives each
  replica a disjoint batch.

- **A poison message wedging the partition forever.** Treating a malformed message as
  transient (never commit) blocks every later event behind one un-processable message.
  `ErrPoisonMessage` + skip-and-commit drains it; the inverse mistake (committing a DB
  error as if poison) is caught by `TestHandleDBErrorIsTransient`.

- **A duplicate delivery from at-least-once redelivery.** Without `UNIQUE(event_id,
  subscription_id)` + `ON CONFLICT DO NOTHING`, a redelivered event inserts a second
  delivery row and the subscriber is POSTed twice from *our* side. The constraint makes
  fan-out idempotent so redelivery is a no-op.

- **`int64` overflow in backoff.** A naive `base << (attempts-1)` wraps to a negative or
  tiny duration once `attempts` is large, scheduling an immediate or past retry.
  `backoff` clamps to the cap *before* doubling past `int64`; `TestBackoffNoOverflowAtLargeAttempts`
  pins `attempts = 1_000_000`.

- **Retrying a dead endpoint forever, or dropping events on a blip.** No dead-letter →
  the pending queue grows unbounded on a decommissioned endpoint; retry-then-drop →
  silent loss. The `maxAttempts` dead-letter parks the row recoverably, and
  `ReplayDeadLettered` re-drives it after a fix.

- **A leaked secret or payload in logs/`last_error`.** The signing secret and payload
  body carry sensitive data (amounts, and in the test a card number). Every log call and
  `last_error` write is secret-free; `deliveryError` renders only "status NNN" or the
  transport error string, and `TestDrainOnceNeverLogsSecretOrPayload` runs a real JSON
  handler and asserts neither leaks. `httpSender` even refuses to fold `url.Parse`'s
  error (which echoes embedded userinfo credentials) into the returned error.

- **An orphaned in-flight delivery on worker crash.** Without the claim-time lease, a
  worker that crashes after POST but before mark strands the row. `ClaimDueDeliveries`
  pushes `next_attempt_at` forward by the lease as it claims, so a crashed row becomes due
  again after 60s with no separate reaper.

- **Per-aggregate ordering silently lost to a replica-count bump.** Scaling `outboxrelay`
  past one instance lets `SKIP LOCKED` hand two events for the same aggregate to different
  drainers, so they can reach the partition out of order — a settle → reorg → re-settle
  sequence arriving as reorg → settle. Nothing errors; the guarantee just stops holding.
  Avoided today only by the relay being a deliberate singleton.

- **Duplicate delivery on every tick from a two-statement claim.** Selecting due rows and
  bumping `next_attempt_at` as two queries lets a second worker grab the same rows in the
  window between them — not a crash-window duplicate, a *steady-state* one. Avoided by
  fusing select and lease-bump into one `UPDATE … RETURNING`.

- **A partition wedged behind one slow subscriber.** POSTing (and backing off) inside the
  consume loop stalls every later event on the partition, blocks the offset, and can get the
  partition rebalanced away mid-backoff. Avoided by the fan-out-then-hand-off split.

- **An unbounded, invisible in-memory queue.** A goroutine per due delivery has no
  backpressure and no durability: a burst spawns thousands of goroutines and sockets, and a
  crash loses every in-flight attempt. Avoided by claiming a bounded batch per tick and
  keeping all delivery state in Postgres.

- **A partial index the planner silently stops using.** `CREATE INDEX … WHERE sent_at IS
  NULL` is only usable if the claim query's predicate matches it exactly. Change one
  predicate and not the other and nothing breaks — the query just quietly starts scanning
  the whole outbox history instead of the tiny unsent backlog.

## 5. Compared to what you know

- **The transactional outbox is "domain events in the aggregate's transaction"** —
  exactly the pattern behind Spring's `@TransactionalEventListener` /
  `ApplicationEventPublisher` when backed by an outbox table, or a `.NET` `IOutbox`. The
  twist Go adds is that there's no framework: the "same transaction" guarantee is just
  *passing the same `db.Querier` value* into `Emit`, and Go's implicit interfaces make
  that a one-argument seam rather than an ambient `TransactionScope`.

- **`FOR UPDATE SKIP LOCKED` is a database-native work queue** — the same primitive
  behind pg-boss, Que, and Rails' `solid_queue`. If you've reached for a Redis list or an
  SQS queue to hand work to N consumers, this is the "your existing Postgres already does
  this" answer: the lock *is* the claim, and `SKIP LOCKED` *is* the "invisible to other
  consumers" property, with no extra infrastructure.

- **At-least-once + idempotent consumer is the Kafka contract you already know.** If
  you've used Kafka in Java, "commit the offset only after you've durably handled the
  message, and make handling idempotent" is the same discipline; here the idempotency key
  is a unique constraint instead of an idempotency table. The Go-specific part is that the
  offset commit is an explicit `CommitMessages` call gated on an `errors.Is` check, not an
  auto-commit config flag.

- **`errgroup` is `CompletableFuture.allOf` with fail-fast cancellation** / structured
  concurrency (Java 21's `StructuredTaskScope`, Kotlin's `coroutineScope`). The
  derived-context-cancels-siblings behavior is exactly `StructuredTaskScope.ShutdownOnFailure`.
  Where the analogy breaks: Go's cancellation is *cooperative* — a goroutine only stops
  when it checks `ctx.Done()`, so `Run` loops must actively `select` on it; there's no
  thread interrupt.

- **HMAC webhook signing is Stripe's `Stripe-Signature` scheme**, deliberately — the
  `t=...,v1=...` layout and the `"<t>.<body>"` signed payload are copied from it because
  it's a well-analyzed design (timestamp-in-MAC defeats replay). If you've verified a
  Stripe webhook, you've already implemented the subscriber side of this.

- **The delivery worker is SQS reimplemented on Postgres.** The DB queue is the queue, the
  `next_attempt_at` lease is SQS's *visibility timeout*, and `status='dead_letter'` is the
  DLQ. If you have used Sidekiq, Celery, or SQS, the whole of Decisions E–G is a shape you
  already know; what's new is that it needs no infrastructure beyond the database you
  already run, and that the retry state is queryable by an operator with plain SQL.

- **sqlc's `DBTX` is "pass a `Connection` or a `Session`" without the base class.** Both
  `*sql.DB` and `*sql.Tx` satisfy it structurally, so the query code genuinely does not care
  which it got. The nearest TypeScript analogue is a function typed to accept
  `{ query(...): ... }` that both a pool and a client happen to fit; the nearest Java one is
  passing either a `Connection` or a transaction-bound `EntityManager`, except Go needs no
  common supertype for it to work.

- **`var _ Iface = (*Impl)(nil)` has no Java counterpart** — `implements` already forces the
  check there. It is Go's opt-in substitute, used where you want the compiler to enforce
  conformance at the definition site rather than discovering a drift at a call site.

## 6. Gotchas & idioms

- **`sql.Null*`, not a naked zero.** A NULL column is `sql.NullInt32{Valid:false}`, and
  a transport error must produce `Valid:false` — writing `0` would be a real value and a
  lie. Always set `.Valid` from the *presence* of the datum, not from whether the number
  is zero.

- **`json.RawMessage` is passthrough bytes.** Use it when you must forward JSON verbatim
  (here: so the signed bytes equal the sent bytes). Re-marshaling through a `struct` would
  silently change the byte layout and break the signature.

- **Multi-`%w` wrapping (Go 1.20+).** `fmt.Errorf("%w: %w", ErrPoison, cause)` produces
  an error that `Is` *both* — the wrap chain is a tree, and `errors.Is` walks all
  branches. Handy for "this is a poison message *and* here's the parse cause."

- **`FOR UPDATE SKIP LOCKED` needs an open transaction.** The lock lives for the tx's
  duration — that's why the relay's claim/mark and the worker's claim happen inside a
  transaction (or a single atomic UPDATE). Outside a tx the lock releases immediately and
  buys you nothing.

- **`mac.Write(body)` vs `fmt.Fprintf(mac, "%s", body)`.** For a MAC over arbitrary
  bytes, `Write` the raw slice — a `%s` format is an invitation for a later "cleanup" to
  introduce escaping or re-encoding that shifts the signed bytes.

- **Cancellation is cooperative and filtered.** `context.Canceled` bubbling out of `Run`
  or `g.Wait()` is a *clean shutdown*, swallowed with `!errors.Is(err, context.Canceled)`.
  A loop that doesn't `select` on `ctx.Done()` will never stop — the ticker `select` is
  load-bearing.

- **Emit placement is the idempotency contract.** `Emit` unconditionally in `Create`
  (fresh id = always a real transition) but only *inside the row-flipped branch* in
  settlement (a redelivered confirm short-circuits earlier). Move the emit above the status
  guard and a redelivered settlement would emit the event twice.

- **Struct field order is a wire contract.** `encoding/json` marshals fields in declaration
  order, so reordering the `Envelope` struct silently changes the bytes on the topic.
  `envelope_test.go` asserts the exact key sequence with a streaming `json.Decoder` —
  unmarshaling into a `map` would lose the ordering and the test would prove nothing.

- **`omitempty` cannot distinguish "absent" from "legitimately zero."** `settlementEvent`
  tags `Asset`/`Amount` `omitempty` because `finalize` resolves no payment and emits just
  `{payment_id, tx_hash}`, where `"amount": 0` would be a misleading claim rather than a
  missing field. It is safe *only* because a real amount is never 0 — the same tag on a field
  whose zero value is meaningful would silently drop real data.

- **`schema_version: 1` from day one costs one constant.** Retrofitting a version field
  *after* a consumer ships means every consumer special-cases version-less messages forever.
  Here the first consumer was literally the next slice.

- **`aggregate_type` is derived, never stored.** `strings.SplitN(e.Type, ".", 2)[0]` turns
  `"payment.created"` into `"payment"`, and there is deliberately no `aggregate_type` column
  — one source of truth. The `2` cap matters: a future `"payment.refund.partial"` still
  yields `"payment"` rather than splitting into three.

- **A partial index only helps if the query predicate matches it exactly.**
  `CREATE INDEX … WHERE sent_at IS NULL` and the claim's `WHERE sent_at IS NULL` are the same
  predicate on purpose, so the planner can use it *and* rows fall out of the index the
  instant they are stamped sent — the index stays proportional to the backlog, not to all
  history. Diverge the two and the planner silently stops using it.

- **Kafka clients dial lazily.** Neither `kafka.NewWriter` nor `kafka.NewReader` opens a
  connection; the first `WriteMessages`/`FetchMessage` does. So construction never blocks and
  needs no error return — but a bad broker address surfaces at the first publish or fetch,
  inside the loop, not at startup.

- **`hash.Hash` is an `io.Writer`.** You sign by *streaming* content into the MAC (`Fprintf`
  the prefix, `Write` the body) and then call `Sum(nil)`. `Sum` **appends** its digest to the
  argument you pass — `nil` is how you ask for just the digest.

- **Drain the response body, but bound it.** `io.Copy(io.Discard, io.LimitReader(resp.Body,
  RespBodyCap))` drains so the keep-alive connection can be reused, and caps at 4KB so a
  hostile subscriber cannot stream gigabytes into the process. Only `resp.StatusCode`
  matters; the body is never stored. Always `resp.Body.Close()` — a leaked body leaks a
  connection.

- **`CheckRedirect` returning an error *blocks* the redirect.** That is the mechanism behind
  the SSRF stance: a subscriber cannot bounce a *signed* POST to a host you never intended
  to talk to, and the refused 3xx surfaces to `classify` as an ordinary non-2xx retry rather
  than being silently followed.

- **`defer consumer.Close()` flushes pending offsets.** The deferred close is not just socket
  hygiene — it commits any offset the reader was holding, so its ordering relative to the
  errgroup's `Wait` is load-bearing.

## 7. Check yourself

1. `drainBatch` calls `Publish` *before* `MarkOutboxSent`, both inside one `ExecTx`.
   Walk through the exact state of the outbox rows if the process crashes (a) after
   `ClaimUnsentOutbox` but before `Publish`, (b) after `Publish` acks but before the DB
   commits, (c) after commit. Which case produces a duplicate on the topic, and what
   absorbs it?
2. Why does the consumer commit the Kafka offset on a *poison* message but not on a
   *transient DB* error? Construct the outage each opposite choice would cause.
3. The fan-out is `INSERT ... SELECT ... ON CONFLICT (event_id, subscription_id) DO
   NOTHING`. Two things make redelivery safe here. Name both, and explain what breaks if
   you drop the `UNIQUE` constraint but keep the `ON CONFLICT`.
4. `ClaimDueDeliveries` sets `next_attempt_at = now() + lease` *in the same UPDATE that
   claims the row*, not after the delivery. What failure does this specifically recover
   from, and why is a separate "reaper" process unnecessary?
5. `deliver` builds `statusCode` as an empty `sql.NullInt32{}` and only fills it when
   `sendErr == nil`. Construct the delivery scenario where writing `0` instead would
   corrupt an operator's later debugging.
6. `backoff` clamps inside the doubling loop rather than computing `base << (attempts-1)`
   and then `min`-ing with the cap. Why can't the naive expression be salvaged with a
   post-hoc `min`?
7. You scale `outboxrelay` to two replicas for throughput. Which guarantee from Decision B
   silently breaks, and why does `FOR UPDATE SKIP LOCKED` — the clause that makes
   multi-replica claiming *safe* — make this particular problem *worse*?
8. `Emit` takes a `db.Querier` and has no idea whether a transaction is open. Name the
   concrete mechanism that nevertheless guarantees the outbox row and the payment row commit
   together, and say why `Emit`'s ignorance is a feature rather than a gap.
9. `Worker.Run` logs and swallows a drain error; `consumer.run` returns one and lets the
   errgroup tear the process down. Justify each choice from the recovery story of its own
   stage, and say what would go wrong if you made them consistent in either direction.

<details>
<summary>Answers</summary>

1. (a) The rows were `FOR UPDATE`-locked but the tx never committed, so the locks
   release on crash and the rows are still `sent_at IS NULL` — re-claimed next tick, no
   publish happened, no duplicate. (b) The broker has the message but the DB rolled back,
   so the rows are *still unsent* and will be re-published next tick — **this is the
   duplicate case**, absorbed by the consumer's `ON CONFLICT DO NOTHING` fan-out (the
   second delivery inserts nothing). (c) Published and marked sent atomically — the happy
   path, never re-claimed. So the design is at-least-once, and duplicates only ever come
   from (b), handled downstream.
2. Poison → commit: a malformed message can never succeed, so *not* committing wedges the
   partition — every later event queues behind a message that fails forever (an outage).
   Transient → don't commit: the DB will recover, so committing now would advance past an
   event we never handled and lose it silently. Committing poison and not-committing
   transient is the only combination that is both non-blocking and non-lossy.
3. (a) The `UNIQUE(event_id, subscription_id)` constraint + `ON CONFLICT DO NOTHING`
   makes a redelivered event a no-op insert; (b) the payload is forwarded verbatim as
   `json.RawMessage`, so even the "same" row is byte-identical. Drop the `UNIQUE`
   constraint and `ON CONFLICT (event_id, subscription_id)` has no unique index to detect
   a conflict against — Postgres errors ("no unique or exclusion constraint matching the
   ON CONFLICT specification"), so the statement fails rather than dedupes; the fan-out is
   no longer idempotent and redelivery either errors or (without the clause) double-inserts.
4. It recovers from a worker crash (or a slow worker exceeding the lease) *after* claiming
   but *before* marking the outcome — the row's `next_attempt_at` was pushed 60s out at
   claim time, so it's invisible until the lease expires, then becomes due again
   automatically. No reaper is needed because "claimed" and "leased" are the same UPDATE:
   an unfinished claim self-expires; a finished one leaves the pending window via the mark.
5. A subscriber returns HTTP 500 (real response, code 500) on attempt 1, then the endpoint
   goes fully offline (transport error, no response) on attempts 2–8. If a transport
   failure wrote `last_status_code = 0` instead of NULL, the dead-letter row would read
   "last status 0," indistinguishable from a genuine "status 0" and hiding that the last
   several attempts never reached the server at all. NULL vs 0 is the difference between
   "no response" and "a response," which is the first thing an operator needs to know.
6. `base << (attempts-1)` overflows `int64` for large `attempts` *before* you get to the
   `min` — the shift wraps to a negative or tiny value, so `min(garbage, cap)` returns the
   garbage, not the cap. You'd have to detect the overflow to reject it, at which point the
   clamp-during-doubling loop is simpler and never lets the running value exceed the cap in
   the first place.
7. Per-aggregate ordering. Keying on `aggregate_id` with a `Hash` balancer pins one
   aggregate to one partition, but that only preserves *order* if a single drainer publishes
   the rows in `created_at` order. With two replicas, `SKIP LOCKED` is doing exactly its job
   — handing each drainer a disjoint batch it can work without blocking — and that means
   *interleaved* batches: two events for the same aggregate can be published by different
   replicas in either order. The clause that removes contention is precisely the clause that
   removes the single-drainer property the ordering guarantee rested on. Fixing it needs
   per-aggregate claim affinity, not a bigger replica count.
8. `ledger.SQLStore.ExecTx` opens a `*sql.Tx` and builds a `*db.Queries` over it; `*sql.Tx`
   satisfies sqlc's `DBTX`, so the `q db.Querier` handed to the closure is already
   transaction-scoped. `InsertOutboxEvent` therefore runs on the same transaction as
   `InsertPayment`, and Postgres' atomicity does the rest. `Emit`'s ignorance is what makes
   it reusable: it works unchanged inside any `ExecTx` in the codebase, and there is no
   transaction manager to thread or forget to thread. `TestCreate_OutboxErrorPropagates`
   pins the inverse direction — an outbox insert error fails `Create`, so the payment row
   rolls back with it.
9. The worker's failed rows are *leased*: whatever it did not finish stays invisible for the
   lease and then becomes due again, so a transient DB blip is genuinely self-healing and
   escalating it to a process restart would cancel the consumer for nothing. The consumer's
   correctness rests on offset ordering, and the only clean recovery from a message it could
   not handle is to resume from the last committed offset — which needs a restart. Make the
   consumer swallow-and-continue and it advances past an event it never fanned out (silent
   loss); make the worker return-and-die and every transient DB hiccup takes the consumer
   down with it, converting a self-healing condition into an outage.

</details>

## 8. Further reading

- [microservices.io — Transactional Outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html)
  — Chris Richardson's canonical write-up of the pattern, the polling-publisher relay,
  and the CDC alternative weighed in Decision A.
- [PostgreSQL — `SELECT ... FOR UPDATE ... SKIP LOCKED`](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE)
  — the locking clause behind the relay's disjoint-batch claim and the worker's lease.
- [Go blog — Working with Errors in Go 1.13](https://go.dev/blog/go1.13-errors) and the
  [`errors` package docs](https://pkg.go.dev/errors) — `%w`, `errors.Is`, and (1.20+)
  multi-error wrapping that the poison-vs-transient routing relies on.
- [`golang.org/x/sync/errgroup`](https://pkg.go.dev/golang.org/x/sync/errgroup) — the
  fail-fast, context-cancelling group running webhookd's consume loop and delivery worker.
- [Stripe — Verifying webhook signatures](https://docs.stripe.com/webhooks#verify-manually)
  — the `t=...,v1=...` HMAC scheme this webhook signer mirrors, and the replay-defense
  rationale for signing the timestamp.
- [Effective Go — Embedding](https://go.dev/doc/effective_go#embedding) and the
  [blank identifier](https://go.dev/doc/effective_go#blank) — the two rules behind the
  embed-a-nil-interface strict fake (§3f) and the `var _ Iface = (*Impl)(nil)` assertion
  (§3g).
