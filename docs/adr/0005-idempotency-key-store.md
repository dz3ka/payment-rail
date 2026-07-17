# ADR-0005: Idempotency-key store for the payments API

- **Status:** Accepted
- **Date:** 2026-07-16
- **Deciders:** Bogdan Dzekic

## Context

`POST /v1/payments` moves money, so a client that retries after a timeout must
never create a second payment. PRD-01 §5 F2 requires that a repeat of the *same*
request (same `Idempotency-Key`) return the *same* result, and that a repeat with
a *different* body under the same key be rejected rather than silently served the
old answer. This must hold under concurrency (two retries racing) and survive a
handler that crashes mid-flight.

## Decision

An `idempotency_keys` table keyed by the client key stores the request-body hash,
a status, and a snapshot of the response (status code + body). The `withIdempotency`
middleware runs a small state machine:

- **claim:** `INSERT … ON CONFLICT DO NOTHING`. Winner is `in_flight` and runs the
  handler; the response is captured via a buffering `ResponseWriter` recorder and
  written back on `Complete` (2xx) or released on `Delete` (non-2xx).
- **replay:** an existing `completed` row with a matching body hash returns the
  stored response verbatim; a `completed`/`in_flight` row with a *mismatched* hash
  returns **422**; an `in_flight` row with a matching hash returns **409** (a retry
  is still running). A missing header is **400**.
- **expiry:** rows live 24h (PRD dedupe window); a background sweeper
  (`RunSweeper`) deletes expired rows on a ticker.

## Alternatives considered

- **`SELECT`-then-`INSERT`:** loses the race — two concurrent retries both see no
  row and both create a payment. `ON CONFLICT DO NOTHING` makes the claim atomic.
- **No stored response (recompute on replay):** cannot guarantee byte-identical
  replay and re-runs side effects. Snapshotting the response is the only way to
  replay exactly.
- **Redis / external KV:** another datastore and a cross-store consistency problem
  with the payment write; Postgres keeps claim and payment in the same system.

## Consequences

- Easier: at-most-once payment creation per key; exact replay; races resolve to a
  single winner; a crashed handler releases its `in_flight` claim via a panic-recovery
  `defer` (using `context.WithoutCancel`) so the key is not wedged.
- Harder / known limitation: the idempotency completion is written **after** the
  payment commits, in a separate step. If `Complete` fails after the payment is
  committed, the key stays `in_flight` and same-key retries get **409** until the
  24h TTL sweep. This **fails safe** — no in-window double charge — but is not the
  full guarantee. The robust fix (fold the idempotency completion into the payment
  transaction so claim+payment+completion commit atomically) is a deferred follow-up.
- Every mutating public endpoint must go through `withIdempotency`; direct calls to
  the payments service bypass the dedupe.
