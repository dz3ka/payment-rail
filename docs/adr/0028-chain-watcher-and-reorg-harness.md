# ADR-0028: Chain watcher: synchronous poll seam, two-pronged reorg detection, simulated-backend harness

- **Status:** Accepted — but the **terminal-Reorged consequence below is superseded by [0014]**
  (extended by [0016]): a reorged tx is reset to Pending and stays tracked, not dropped. The
  poll seam, two-pronged detection, and harness decisions stand.
- **Date:** 2026-07-20
- **Deciders:** Bogdan Dzekic

## Context
M3 slice 1 (PRD F5) needs a component that watches an EVM node for the transactions
the adapter broadcast, tracks each to a configurable confirmation depth N, and detects
reorgs — a previously-mined tx that dropped or re-mined on a different block. The hard
constraints: it must be *provable in CI with no live testnet*, it must never mistake a
transient RPC fault for a reorg (a false reorg would roll back a real payment), and
"depth N" must mean exactly N confirmations, not N+1.

## Decision
- **`poll(ctx) []Status` is the pure synchronous core; `Run` is a thin ticker shell.**
  `poll` has no timers, goroutines, or wall clock — a test drives the whole lifecycle by
  calling it directly with a scripted reader. `Run` just calls `poll` on a ticker and logs
  each transition. No exported Status channel this slice (deferred as YAGNI).
- **The watcher owns a narrow 3-method `chainReader` interface** (`TransactionReceipt`,
  `HeaderByNumber`, `BlockNumber`), not `*ethclient.Client`. Compile-time `var _` assertions
  prove both the live client and go-ethereum's `simulated.Client` satisfy it, so the *same*
  code runs against a live node and an in-memory chain.
- **Reorg detection is two-pronged and conservative:** a tx is Reorged only when its receipt
  disappears (`errors.Is(err, ethereum.NotFound)`) *or* the canonical block at its recorded
  height has a different hash. Any *other* RPC error is logged and skipped — never a reorg.
- **Confirmation threshold is exact:** the pending→mined branch confirms immediately when the
  tx is already buried ≥ N deep on first sighting, so N=1 confirms at depth 1 (an earlier
  cut made N mean N+1; pinned now by `TestWatcherConfirmationDepthConfigurable`).
- **The mutex is never held across an RPC call:** `poll` snapshots the tracked keys under the
  lock, does all network I/O unlocked, then re-locks to mutate — a slow node never stalls a
  concurrent `Track`.
- **Hermetic reorg proof uses `simulated.Backend.Fork`, committing twice** so the side chain
  is *strictly longer* (2 > 1) and canonical deterministically, rather than relying on the
  probabilistic equal-length flip.

## Consequences
- Reorged is terminal this slice: a reorged tx is dropped from tracking, not re-followed onto
  the new chain. Re-tracking/replacement is future work.
- The narrow reader seam means any go-ethereum signature change fails to compile at the `var _`
  assertion — we fix the interface, never cast.
- Two config knobs (`PAYMENT_RAIL_WATCHER_CONFIRMATIONS` default 12,
  `PAYMENT_RAIL_WATCHER_POLL_INTERVAL_SECONDS` default 15) follow the repo's env convention.
- `RedactRPCError` reduces `*url.Error` endpoints to scheme://host at the log boundary, so a
  routine node-unreachable tick can't leak the API key embedded in a managed-node URL. See the
  chainwatcher wiring, which deliberately omits the RPC URL from its startup log for the same
  reason.

## Related
[0012] fake-chain test harness for reorgs — owns the simulated-backend harness decision; the
`simulated.Backend.Fork` bullet above is this ADR's slice-1 statement of it, kept for context.
[0014] reorg-safe settlement effects — supersedes the terminal-Reorged consequence above.
[0016] settlement recovery, resume anchor & finality eviction — extends [0014].
