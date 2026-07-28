# ADR-0019: Per-signing-key sliding-window velocity limits

- **Status:** Accepted. Second slice of M5 (PRD F8 policy engine). Extends ADR-0018's `submit.go`
  policy seam with a second, independent control; does NOT supersede it. Four-eyes
  approval (F8c) and the audit log (F9) remain later slices at their own seams.
- **Date:** 2026-07-21
- **Deciders:** Bogdan Dzekic

## Context
PRD F8 requires "velocity limits (per account, sliding window)". Like the denylist
(0018), the only place the spend actually happens is `cmd/paymentrailctl submit`, and
the "account" analog at that seam is the **signer key-id** (the spend-authorizing
identity — there is no ledger-account in the on-chain submit path). Unlike the
denylist, velocity is **stateful**: it needs the recent spend history of a key. The
enforcement seam is a **one-shot CLI process**, so in-process state (as in the signer's
long-running `spendBucket` mutex, ADR-0009) resets every invocation and enforces
nothing — the window must be durable across invocations.

## Decision
- **Durable Postgres state (`velocity_events`), not in-process, not Redis.** A sliding
  window needs per-event timestamped rows to `SUM` over `[now−window, now]`; a
  monotonic counter cannot expire old contributions. New table `velocity_events
  (id, key_id, amount BIGINT, occurred_at TIMESTAMPTZ)` + index `(key_id, occurred_at)`,
  migration 0007. Redis (PRD §7 pre-authorizes it for velocity windows) rejected: Postgres
  is already wired, the CLI is low-throughput, no new infra earns its place. Trigger to
  revisit Redis: a high-QPS enforcement seam (the REST path), not this CLI.
- **Dimension = signer key-id.** The only spend-authorizing identity at the seam;
  caps sender velocity, matching F8b intent. (`--to` recipient rejected — that is the
  denylist's subject, not velocity's.)
- **Decision logic in db-free `internal/policy`, storage in the composition root, bridged
  by a `decide`-callback port.** `VelocityStore.Charge(ctx, keyID, amount, window, now,
  decide func(Usage) error)`: the store acquires the per-key lock, computes in-window
  `Usage`, calls `decide` (the caps comparison, which lives in `policy`), and inserts the
  event only if `decide` returns nil — all in ONE transaction. This is the only shape that
  keeps the cap decision inside `internal/policy` (which must not import `internal/db`, a
  standing dependency rule) AND inside the store's single tx. Mirrors the repo's existing
  `ledger.PostWithin` callback-in-tx seam. A two-call split (`WindowUsage` then `Insert`)
  was rejected: it forfeits atomicity.
- **Per-key concurrency via `pg_advisory_xact_lock`.** Append-only event rows have no
  single row to `SELECT … FOR UPDATE`, so a transaction-scoped advisory lock serializes
  the SUM-then-INSERT per key — the DB analogue of the signer's mutex-across-check-then-
  commit. SERIALIZABLE rejected (forces 40001 retry loops); plain READ COMMITTED rejected
  (two concurrent same-key charges both read room and both insert → over-cap). Lock key =
  fnv-64a of the key-id → int64; an explicit 64-bit hash is preferred over the undocumented
  int4 `hashtext()` builtin (razor CUT overridden — correctness-first repo, larger space,
  testable). Collisions only over-serialize two distinct keys; queries still filter on the
  real `key_id`, so a collision never mis-counts.
- **Record-on-attempt.** The event is inserted in the same locked tx that admits it,
  before broadcast. A failed broadcast then over-counts in the SAFE direction (denies
  more, never less); recording after a successful broadcast would reopen the concurrent-
  double-spend window the advisory lock exists to close, and under-count on a crash
  between broadcast and record. Fail-closed wins over precision here.
- **int64/BIGINT money with an overflow guard.** Amounts are `*big.Int` in the app but
  BIGINT/int64 in Postgres (repo convention — no `numeric` type anywhere). An
  `!amount.IsInt64()` guard fails closed before any write; `SUM(amount)::bigint` raises
  "bigint out of range" (verified against live PG) rather than wrapping, so an
  over-range window sum also fails closed.
- **Disabled-when-unset.** `VelocityCaps.Enabled()` (Window > 0 AND ≥1 cap set) gates
  the ONLY new Postgres dial; unset caps preserve submit's legacy no-DB print-only
  contract, exactly as the denylist's `""`=disabled. Breach returns a `%w`-wrapped
  `ErrVelocityExceeded`; `errors.Is` distinguishes it from an operational failure, both
  fail closed.
- **Log-hygiene:** a velocity deny logs `key_id` + reason at WARN (extends 0018's
  deny-only audit exception; a key-id is a signing handle, not a recipient/amount);
  backend failures log at ERROR. The amount and `--to` are never logged.

## Consequences
- New `internal/policy/velocity.go` (limiter + port + sentinel), a composition-root
  `pgVelocityStore` (tx + advisory lock), 3 config knobs
  (`PAYMENT_RAIL_POLICY_VELOCITY_{WINDOW_SECONDS,MAX_COUNT,MAX_AMOUNT}`), migration 0007
  + one sqlc query file. `internal/db`'s generated `Querier` grew 3 methods; the two
  in-memory `db.Querier` fakes gained matching stubs. No API/proto/binary change (still
  seven binaries).
- Enforcement verified end-to-end against live Postgres, including a 20-goroutine/K=5
  concurrency test proving the advisory lock admits exactly K and writes exactly K rows.
- A repeatedly-failing broadcast can transiently over-consume a key's window budget;
  events age out of the window naturally. Documented at the wiring site.
- The submit CLI is the only velocity seam today; when the REST create path gains an
  on-chain destination + higher throughput, both the limiter and the Redis-vs-Postgres
  store choice are revisited there.
