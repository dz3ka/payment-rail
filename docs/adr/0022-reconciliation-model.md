# ADR-0022: Reconciliation model: finalized-only expected, confirmed bridge, proof-of-reserves

- **Status:** Accepted. M6 (PRD F10), first of three M6 ADRs (this + registry ADR-0023 +
  BalanceReader ADR-0024). Extends the derived-balance ledger (ADR-0004) and the
  settlement status model (ADR-0014/0016); supersedes none. Delivered as
  `paymentrailctl reconcile`, modelled on `audit verify` (ADR-0021).
- **Date:** 2026-07-21
- **Deciders:** Bogdan Dzekic

## Context
PRD F10 (P1): "reconciliation job: expected vs. actual on-chain balances per treasury
address; discrepancy report; proof-of-reserves-style statement (internal liabilities
vs. on-chain balances)." Three open forces the code had to resolve, none answered by
the existing schema:
- **What "expected" means.** The ledger credits the per-asset `onchain_settlement`
  house account on `PhaseConfirmed` and merely flips status on `PhaseFinalized` (no
  ledger effect — `settlement.go`). So the *live house-account balance* reflects
  confirmed **and** finalized value together and cannot isolate finalized-only.
- **How to define "internal liabilities"** given a generic `accounts(name, kind, asset)`
  model where `kind` was dormant and customer accounts are arbitrary caller UUIDs.
- **How a monitor consumes the result** — `audit verify` only has a binary pass/fail;
  reconciliation must distinguish "ran clean" from "found drift" from "couldn't run."

## Decision
- **Finalized-only "expected" via settlement-row aggregation, NOT a house-balance read**
  (user-confirmed). `expected = Σ payment.amount` over `settlements JOIN payments WHERE
  status='finalized'`, per asset. The rejected house-account-balance read cannot separate
  confirmed from finalized value, which the finalized-only requirement demands. A reorg
  inside the 64-block window walks a `finalized` row back out via the existing reverse
  posting, so trusting only `finalized` gives a reorg-stable expected quantity.
- **Confirmed-not-final is a reconciling BRIDGE term, never a discrepancy** (user-confirmed).
  Those funds are already on-chain (so they show up in `balanceOf`) but not yet ledger-final,
  so `Discrepancy = ActualOnChain − ExpectedFinalizedMinor − ConfirmedPendingMinor`
  (`report.go`). A treasury holding only tracked settlement funds nets to `Discrepancy == 0`
  — the PRD "zero discrepancies" success metric — while in-flight settlements do NOT
  register as false drift. Naively comparing `expected` to `actual` would flag every
  in-flight settlement as a discrepancy.
- **Proof-of-reserves: liabilities = −Σ(non-house account balances), house = reserves**
  (user-confirmed). Account balance = `SUM(credit) − SUM(debit)`; double-entry makes the
  system-wide sum zero, so `Σ(non-house) = −houseBalance` and, because the house account is
  only ever credited on settle, `Σ(non-house) ≤ 0`. `SumNonHouseLiabilities` returns that
  signed sum and the Go caller negates it **exactly once** (`reconcile.go`) to a
  non-negative owed value. Non-house accounts are identified by `kind <> 'onchain_settlement'`
  — the seed sets both `name` and `kind` to that value (migration 0003), giving the dormant
  `kind` column its first real consumer. Verdict: `ActualOnChain ≥ Liabilities → OK`, else
  `UNDERCOLLATERALIZED`. (Rejected: filtering by `name` — doesn't generalise if a second
  house-kind account is ever seeded; `kind` is the category dimension.)
- **Tri-state exit code owned by `runReconcile() int`:** `0` clean (`report.Clean`), `1` a
  discrepancy OR an `UNDERCOLLATERALIZED` verdict (report still prints to stdout), `2`
  usage/config/DB/RPC/operational error (stderr). A cron/monitor gates on these three; the
  generic `err!=nil → exit(1)` dispatch used elsewhere cannot separate "ran, found drift"
  from "failed to run," which is the whole point of a reconcile tool. A single treasury's
  `BalanceOf` failure aborts the entire run (exit 2) rather than silently reporting a
  partial "clean."
- **Amounts on stdout only; the `slog` summary is amount-free** (extends the repo's
  `logResult` discipline). The report is the amount-bearing artifact by design, so
  expected/actual/discrepancy/liabilities print to stdout; the structured summary logs only
  asset count, treasury count, `discrepancy_present`, per-asset verdict labels, `clean`, and
  `duration_ms` — never amounts.
- **Keyset cursoring over `GROUP BY SUM`, deliberately.** A single grouped SUM would be
  terser and cheaper, but M6's named Go learning objective is "cursoring large tables";
  `AggregateSettlements` pages settlements by the row-value tuple `(created_at, id) > (...)`
  accumulating per-asset finalized/confirmed sums in Go, matching the repo's existing keyset
  idiom (`ListPaymentsAfter`). Flagged as a learning-vehicle choice, not a perf necessity.

## Consequences
- New `internal/reconcile` package (`AggregateSettlements`, pure `BuildReport` + `WriteText`,
  `Registry`/`LoadRegistry`; stdlib + `internal/db` only, mirroring `internal/audit`),
  `db/query/reconcile.sql` (3 queries), `cmd/paymentrailctl/{reconcile.go,reconcilestore.go}`
  + a `reconcile` dispatch. `db.Querier` grew 3 methods; the ledger/settlement test fakes
  gained matching panic-stubs (those domains never reconcile). No schema change, no migration,
  no new binary; reuses the existing Postgres DSN plus the M6 `ReconcileTreasuries` knob.
- Verified end-to-end against live Postgres: zero-discrepancy path reconciles clean (exit 0),
  an injected `balanceOf` drift is flagged (non-zero discrepancy, exit 1), and seeded
  liabilities exceeding on-chain actual yield `UNDERCOLLATERALIZED` (exit 1). Hermetic tests
  cover the finalized/confirmed split, the confirmed-bridge (in-flight ≠ discrepancy),
  surplus/shortfall, and single-vs-double negation of liabilities.
- **Known limitation (accepted):** an asset with settlements but no registry entry reports
  `liabilities: 0` on its row. This cannot produce a false CLEAN (such an asset has
  `actual = 0`, `expected > 0`, so `Discrepancy ≠ 0` forces exit 1); it usefully surfaces an
  unregistered settlement asset rather than hiding it.
