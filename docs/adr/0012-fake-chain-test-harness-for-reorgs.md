# ADR-0012: Fake-chain test harness for scripted reorgs

- **Status:** Accepted
- **Date:** 2026-07-20
- **Deciders:** Bogdan Dzekic

## Context

M3's chain-watcher tracks a transaction through confirmation depth and must handle
the hard case correctly: a **reorg** that reverses an already-confirmed tx, then a
**reapply** onto the new canonical chain, plus **finality eviction** once buried
deep enough. This logic is the reason M3 exists, so it needs thorough, repeatable
tests. A live testnet cannot be used to prove it: you cannot force a reorg on
command, timing is non-deterministic, and runs are slow and flaky. The watcher's
five-phase lifecycle (`Pending → Mined → Confirmed → Reorged → Finalized`) needs
every edge deterministically reachable.

## Decision

Test the watcher behind its narrow `chainReader` interface (head number, receipt
lookup, header-by-height) with a **two-tier** seam:

1. **Scripted `fakeReader` (primary).** A struct with plain fields — `blockNumber`,
   canned receipts and headers — that a test mutates step by step and drives via
   `w.poll(ctx)`. Because the test *is* the chain, it can script any sequence
   deterministically: deepen confirmations, rewrite the canonical header under a
   tracked tx to trigger `PhaseReorged`, or advance the head past the finality depth
   to trigger eviction. This gives fast, hermetic coverage of every phase transition.
2. **go-ethereum `simulated.Backend` (one honesty check).** A single end-to-end test
   mines a real signed EIP-1559 tx, forks from before its block, and extends a side
   chain until it is canonical — a **genuine** reverse+reapply against a real in-memory
   EVM, no network. It proves the watcher's behavior against real chain semantics, not
   just our model of them.

## Alternatives considered

- **Live testnet (Sepolia):** cannot script a reorg on demand, non-deterministic,
  slow, and flaky in CI — unusable as the primary reorg test.
- **Mock the `ethclient` at the HTTP/JSON-RPC layer:** brittle and couples tests to
  wire encoding rather than to the behavior under test.
- **Only the simulated backend:** real but awkward to steer into specific edge cases
  (exact confirmation depths, finality eviction, dedupe races); the scripted fake
  reaches those precisely, so the two tiers are complementary, not redundant.

## Consequences

- Easier: reorg, reapply, and finality-eviction logic is covered deterministically
  and fast, with no network; the `chainReader` interface is the single seam that makes
  both tiers possible; the simulated backend keeps one path honest against a real EVM.
- Harder / deferred: the scripted fake encodes *our* understanding of chain behavior,
  so a wrong assumption could pass the fake yet fail reality — mitigated, not eliminated,
  by the one simulated-backend end-to-end test. Both harnesses live in `_test.go` files
  and never ship in a binary.
