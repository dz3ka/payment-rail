# ADR-0027: In-process fault injection through existing seams

- **Status:** Accepted. M7 (chaos-tests half). Consumes the ports established across M2–M6
  (`ledger.Store`, evm `ethRPC`/`chainReader`, `settlement.Sink`); adds no production
  code. Supersedes none.
- **Date:** 2026-07-21
- **Deciders:** Bogdan Dzekic

## Context
The chaos suite must prove the PRD's headline NFR — "kill any component mid-flow; the
system converges with no lost or duplicated payments" — as a *demoable test*, not a
claim. The question was HOW to inject faults (crash-before-commit, DB connection death,
broken RPC, reorg) deterministically, in-process, with zero production change and no new
dependency. The repo has no toxiproxy/pumba precedent and CI has no container runtime.

## Decision
- **Faults come from the constructor-injected ports the domain already depends on.**
  A test-only `faultStore` implements `ledger.Store`, running `fn` in a real
  READ-COMMITTED tx exactly like `SQLStore` and then, once `fn` succeeds, injecting the
  fault in place of COMMIT (crash = rollback + sentinel). Broken RPC wraps a
  go-ethereum `simulated.Client` and shadows only `SendTransaction`. Reorgs are driven
  by hand-built `evm.Status` values fed synchronously to a real `settlement.Sink.OnStatus`
  (never the ticker-timed watcher `Run` loop), keeping every scenario deterministic
  under `-race`. Structural interfaces + constructor injection make the system testable
  under fault for free — no production seam was added.
- **Connection death is server-side, not `sql.DB.Close()`.** A first design closed the
  pool before COMMIT to model DB failover; it was empirically wrong — a `*sql.Tx` holds
  an already-checked-out connection that the pool close does not touch, so the doomed
  COMMIT *succeeds and the write persists*. Real connection death is modeled with
  `pg_terminate_backend(pid)` against the specific backend a pinned connection uses
  (`driver: bad connection`, 0 rows), on a dedicated throwaway `*sql.DB` so the
  surviving assertion pool is never at risk. The pool-close "fault" mode was removed.
- **Where a code path isn't injectable, reconstruct it faithfully.** `payments.Service`
  builds its own `SQLStore` and can't take a `faultStore`, so the crash/failover tests
  compose the payment tx body directly (PostWithin + InsertPayment, mirroring
  `payments.go`); omitting only `outbox.Emit`/`audit.Append`, which merely *add*
  rollback triggers — the atomicity proof stays conservative.

## Consequences
- Convergence is asserted on real persisted state through a *surviving* connection
  (global Σ(credit−debit)=0 over non-`opening` lines; reconcile Discrepancy==0;
  single-net-settle via dst balance + reversal count), never on in-memory bookkeeping.
- The faithful-reconstruction compromise is documented in-code; if `payments.Service`
  later gains an injectable store, the crash test can drive `Create` directly.
