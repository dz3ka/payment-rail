# ADR-0021: Append-only hash-chained audit log

- **Status:** Accepted. Fourth and final slice of M5 (PRD F9); completes the milestone
  (F8a denylist / F8b velocity / F8c four-eyes + this). Reuses the transactional
  outbox seam (ADR-0011), the in-tx callback model (ADR-0004/0007), and the
  advisory-lock idiom introduced for velocity (ADR-0019); supersedes none of them.
- **Date:** 2026-07-21
- **Deciders:** Bogdan Dzekic

## Context
PRD F9 (P1): "append-only audit log (hash-chained records) covering every state
transition and operator action." Threat-model ties: Repudiation (immutable record
of every transition) and Tampering (hash-chained, deletion/reordering detectable).
The outbox (ADR-0011) already captures every domain state transition as a versioned
JSON envelope inside the caller's `ExecTx`, so event *capture* did not need
re-plumbing — the audit log parallels it at the same call sites. The open forces:
how atomic the audit record must be relative to the fact it records; how to serialize
an inherently-serial hash chain under concurrent writers; and how to keep the chain
re-verifiable across future code changes.

## Decision
- **Same-tx append, co-located with `outbox.Emit`, NOT an async outbox consumer.**
  `audit.Append(ctx, q, Entry)` runs as the last write in the SAME `ExecTx` as each
  state change (`internal/payments/payments.go`, `internal/settlement/settlement.go`)
  using the same tx-bound `db.Querier`; its error propagates so a failed append rolls
  the state change back (fail-closed — the audit row is as atomic as the ledger effect).
  The rejected async consumer (drain the outbox like webhookd) is at-least-once and
  leaves a window where a committed state change has no audit row yet — too weak for
  "immutable record of *every* transition". Cost, accepted: the audit write now sits on
  the state-change write path.
- **A single GLOBAL chain serialized by a fixed `pg_advisory_xact_lock` key, NOT
  per-entity chains.** A hash chain is serial (each row needs the prior row's
  `entry_hash`). `Append` acquires `chainAdvisoryKey` (a deliberately fixed well-known
  constant = `int64(fnv64a("payment-rail.audit.chain.v1"))`, unlike velocity's per-key
  hashed values), then reads the head (`GetAuditHead`), then inserts — **all three under
  the one lock in one tx**, so head-read→insert is atomic; without holding the lock
  across both, two writers read head N and both write seq N+1 (a fork). Xact-scoped lock
  auto-releases on commit/rollback. A global chain verifies as one linear walk from
  genesis; per-entity chains would scale better but make "covers everything" verification
  require enumerating every stream. Contention on every audited write is accepted for a
  low-volume testnet rail.
- **App-assigned gap-free `seq = head.Seq + 1` under the lock; PK is plain `BIGINT`, NOT
  serial/identity.** A serial/identity column consumes a value on a rolled-back tx,
  leaving gaps that would make Verify's contiguity check ambiguous ("is a missing seq a
  deletion or a rollback?"). App-assigning under the lock keeps the chain gap-free, so a
  gap is unambiguously tampering.
- **Canonical hash preimage: length-prefixed byte framing + the raw payload stored as
  BYTEA; NOT hashing JSON.** `canonical()` frames every variable-length field
  (`actor`/`action`/`aggregate_type`/`aggregate_id`/`payload`) with an 8-byte big-endian
  length prefix and writes `seq`/`occurredMicros` fixed-width, so `(actor=ab,action=c)`
  and `(actor=a,action=bc)` cannot collide; `entry_hash = sha256(prev_hash ‖ canonical)`.
  The event `Data` is stored as its exact marshaled bytes in a `payload BYTEA` column and
  Verify rehashes *those stored bytes* — never re-marshals — so Go/`encoding/json`
  field-order or map-key nondeterminism across future edits can never break an old chain.
  `occurred_at` is truncated to microseconds before both hashing and storing and hashed
  as `UnixMicro()` (an absolute instant, timezone-independent), making the Postgres
  `timestamptz` round-trip byte-exact so a valid chain never shows a false `hash_mismatch`.
  The queryable dimensions (who/what/which-aggregate/when) remain typed columns; only the
  free-form Data is opaque. (Rejected: storing the event as jsonb AND a preimage — two
  copies that can silently disagree; or hashing the outbox envelope bytes — its marshal
  order is not guaranteed canonical under later edits.)
- **Append-only enforced by a `BEFORE UPDATE/DELETE/TRUNCATE` trigger raising an
  exception.** Role-independent (the repo's migrations manage no app role to `REVOKE`
  from), self-contained in migration 0009, and live-testable. It is defense-in-depth —
  the hash chain, not the trigger, is the tamper-EVIDENCE guarantee (a superuser can
  `DISABLE TRIGGER`), but it cheaply blocks accidental/casual mutation and makes
  "append-only" a demonstrable property.
- **Coverage this slice: the 5 state transitions (payment.created/canceled,
  settlement.confirmed/reorged/finalized) + the two four-eyes operator actions
  (operator.propose, operator.approve).** `Claim` already ran in a tx; `Propose` was
  wrapped in one (mirroring `Claim`) so its append is same-tx too, keyed to the payment
  aggregate with the operator handle as actor. **Deferred (explicit, user-confirmed):**
  auditing the direct `submit` broadcast and `replay-webhook`. The broadcast is an
  external chain send that CANNOT be atomic with a Postgres append, so a same-tx record
  is impossible and a best-effort append would violate the chosen fail-closed doctrine;
  its money-moving effect is already audited downstream as `settlement.confirmed`.
  `replay-webhook` has no ledger effect and no operator-identity flag today. Both need a
  new `--operator` flag (and, for broadcast, a non-same-tx mechanism) — a follow-up slice.
- **`paymentrailctl audit verify` is the operator-facing headline.** A pure
  `Verify(rows []db.AuditLog, opts...) (Result, error)` re-walks genesis→head checking
  seq contiguity, prev_hash linkage, and per-row hash, returning a typed `TamperError`
  (`Kind` ∈ gap/broken_link/hash_mismatch/head_mismatch). The CLI prints
  `AUDIT CHAIN VALID: <n> entries; head seq=X hash=Y` and exits 0, or a tamper/DB/hex
  error to stderr and exits non-zero (the monitoring signal). The printed head hash is
  what an operator records as the next run's `--expect-head-hash` anchor.

## Consequences
- New `internal/audit` package (`Append`, `canonical`, `entryHash`, pure `Verify` +
  `WithExpectedHead`; stdlib + `internal/db` only, mirroring `internal/outbox`),
  `cmd/paymentrailctl/audit.go` (the `audit verify` subcommand), migration 0009
  (`audit_log` + immutability triggers), `db/query/audit.sql` (4 queries). `db.Querier`
  grew 4 methods; the two in-memory fakes and the payments/settlement test fakes gained
  matching append-only-chain behavior. `Propose` in `approvalstore.go` is now
  tx-wrapped. No new config knob (reuses `PAYMENT_RAIL_POSTGRES_DSN`); no proto/binary
  change (still seven binaries).
- Verified end-to-end against live Postgres: full `-race` suite green; the CLI verifies a
  112-entry chain (exit 0), a trigger-disabled interior mutation is caught as
  `hash_mismatch` (exit 1) and restoration returns the identical head hash, a wrong
  `--expect-head-hash` yields `head_mismatch`, and a plain `UPDATE`/`DELETE`/`TRUNCATE`
  is blocked by the trigger. Hermetic tests cover every tamper class + length-prefix
  collision resistance + genesis + the anchor cases.
- **Known limitation (inherent to hash chains, accepted):** `verify` without an anchor
  catches interior deletion/reorder/relink/forgery, but a capable attacker with DB write
  access who edits a row and recomputes every forward hash produces a self-consistent
  chain that verifies clean. The only cross-check is `--expect-head-hash` recorded
  out-of-band; automatic head anchoring (a notary/checkpoint) is a future concern, and
  the append-only trigger is not a substitute (droppable by a superuser).
