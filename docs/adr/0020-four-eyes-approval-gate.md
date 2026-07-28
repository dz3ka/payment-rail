# ADR-0020: Four-eyes approval above a threshold

- **Status:** Accepted. Third slice of M5 (PRD F8c). Extends the `submit.go` policy seam of
  ADR-0018 (denylist) and ADR-0019 (velocity) with a third, independent control;
  supersedes neither. The audit log (F9) remains a later slice at its own seam.
- **Date:** 2026-07-21
- **Deciders:** Bogdan Dzekic

## Context
PRD F8 requires "four-eyes approval above threshold". Unlike the denylist (a
stateless screen) and velocity (stateful but single-actor), four-eyes needs **two
distinct humans separated in time**: a proposer initiates a large payment and a
different approver releases it. The enforcement seam is `cmd/paymentrailctl` — a
**one-shot CLI process** with no identity/auth layer (it knows only the signing
`--key-id`, which is signing authority, not a person). A synchronous single
invocation therefore cannot deliver four-eyes; durable cross-invocation state is
unavoidable, and operator identity must be introduced from scratch.

## Decision
- **Two-phase maker-checker backed by a `payment_approvals` table (migration 0008).**
  `submit --proposer=<id>` on an amount **≥ threshold** parks the full payment intent
  (to, amount, asset, key-id, payment-id) + proposer as a `pending` row and exits
  WITHOUT broadcasting; a new `approve <id> --approver=<id>` subcommand validates the
  approver, atomically claims the row (`pending → approved`), and broadcasts the stored
  intent. Below threshold / feature disabled → broadcast exactly as before. Storing the
  intent (not re-supplying it at approve time) closes a tamper window and lets `approve`
  replay the exact proposed payment.
- **Opaque allowlisted operator-id strings for both roles** (`--proposer`, `--approver`,
  `PAYMENT_RAIL_POLICY_APPROVERS` CSV). A signing key-id is not a person, so
  self-approval distinctness would be meaningless against it. The proposer must ALSO be
  allowlisted (checked at propose-time via `KnownApprover`) so "distinct known approver"
  is a real two-person control, not one known party vs. an arbitrary string.
- **Threshold + allowlist logic in a db-free `policy.ApprovalGate`; SQL in a `cmd`
  store — NO named `ApprovalStore` port, deliberately diverging from ADR-0019.** Unlike
  the velocity limiter, **no** `internal/policy` code calls the store; the `cmd` layer
  orchestrates directly. A port interface would have exactly one impl and no domain-side
  consumer, failing the "second consumer or removal date" bar. Instead the decision
  *types* (`Intent`, `PendingApproval`, `ApprovalGate` with `Required`/`KnownApprover`/
  `Authorize`) live in `policy`, and the store injects `gate.Authorize` as a `decide`
  callback into its transaction — preserving policy's db-freedom WITHOUT an abstraction
  that does not earn its place. (Mirror a prior pattern only where the same forces apply.)
- **`SELECT … FOR UPDATE` on the single approval row, NOT an advisory lock.** ADR-0019
  locked because its check was an append-only `SUM` with no row to lock; an approval is
  one row, so row-level `FOR UPDATE` is the smaller correct tool and sidesteps the shared
  advisory-lock keyspace entirely. The approver-distinctness check runs between the
  locked read and the guarded `UPDATE … WHERE status='pending'` (`:execrows`, rows==0 ⇒
  reject), so concurrent `approve` calls serialize to exactly one winner (verified: 20
  goroutines → 1 winner / 19 `ErrAlreadyApproved`).
- **Record-on-attempt: claim commits BEFORE broadcast.** `pending → approved` commits
  first; a crash before/after the irreversible send can then never double-broadcast
  (a re-run sees `approved` and stops). A crash on the *read-only preflight* (unreachable
  RPC → `VerifyChainID` fails) would otherwise strand the row `approved` with a NULL
  `tx_hash`, permanently unclaimable.
- **`ReopenApproval` safety valve, gated on the broadcast dual-return contract.** A
  guarded `UPDATE … SET status='pending', approver=NULL WHERE status='approved' AND
  tx_hash IS NULL` auto-reopens a stranded approval — but ONLY when nothing was sent.
  Safety rests on `broadcastIntent` returning an **empty** tx-hash on every pre-`Submit`
  failure and a **non-empty** hash on every post-`Submit` (bookkeeping) failure: `approve`
  reopens on empty-hash (safe), and NEVER on non-empty-hash (the tx landed — reopening
  would risk a double-send). The `tx_hash IS NULL` SQL guard is a second, independent
  barrier: a landed payment cannot be resurrected even if the hash check regressed.
- **Fail-closed throughout** (package doctrine): `IsInt64` amount guard before any write;
  config-coherence fail-fast (threshold set but empty approver allowlist ⇒ payments would
  be un-approvable); bare `ErrSelfApproval`/`ErrUnknownApprover` sentinels (unknown checked
  before distinctness); store sentinels `ErrApprovalNotFound`/`ErrAlreadyApproved` from
  `sql.ErrNoRows` / rows==0. Amounts and recipients are never logged outside the
  established deny/park exception.

## Consequences
- New `internal/policy/approval.go` (gate + types + sentinels), `cmd/paymentrailctl`
  gains `approvalstore.go` (Propose/Claim/MarkBroadcast/Reopen), `broadcast.go` (the
  `broadcastIntent` sequence extracted from `submit.go` so both the below-threshold path
  and `approve` share one money-movement code path), and `approve.go`; `submit.go` gains
  the park-vs-broadcast branch and a `--proposer` flag; `main.go` dispatches `approve`.
  Two config knobs (`PAYMENT_RAIL_POLICY_APPROVAL_THRESHOLD` decimal string,
  `PAYMENT_RAIL_POLICY_APPROVERS` CSV), migration 0008 + one sqlc query file (5 queries).
  `internal/db`'s `Querier` grew 5 methods; the two in-memory fakes gained matching stubs.
  No API/proto/binary change (still seven binaries).
- Verified end-to-end against live Postgres: park → distinct-approver claim → broadcast,
  every fail-closed rejection (self-approval, unknown approver, missing/un-allowlisted
  proposer, config-coherence), the concurrent single-winner proof, and the reopen path
  (preflight failure → row back to `pending` → re-claimable).
- **Velocity now charges at approve-time** for gated payments (Charge lives in the shared
  `broadcastIntent`, which the park path skips) — correct record-on-attempt: a
  parked-but-never-approved payment consumes zero budget; a reopen+retry re-charges
  (safe-direction overcount, consistent with ADR-0019).
- **Known limitation:** the denylist is screened at propose-time but NOT re-screened at
  approve-time; a destination added to the static file denylist during the (short,
  operator-driven) park→approve window would still broadcast. Acceptable for a testnet
  static-file denylist; a re-screen at approve is the follow-up if the denylist becomes
  dynamic. Audit timestamps (`approved_at`/`broadcast_at`) were deliberately left out —
  they belong to the F9 hash-chained audit log, not this table.
