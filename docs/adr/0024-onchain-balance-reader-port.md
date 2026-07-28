# ADR-0024: On-chain BalanceReader port and fail-closed balanceOf decode

- **Status:** Accepted. M6 (PRD F10), third of three M6 ADRs (reconciliation model ADR-0022 +
  registry ADR-0023 + this). Follows the ports-and-adapters convention for chains
  (ADR-0010/0012) and the RPC-error redaction idiom already in `internal/chain/evm`;
  supersedes none.
- **Date:** 2026-07-21
- **Deciders:** Bogdan Dzekic

## Context
Reconciliation's "actual" side needs an on-chain ERC-20 `balanceOf` read. No balance-read
path existed: the `ethRPC` seam in `internal/chain/evm/rpc.go` carried only
nonce/gas/header/broadcast methods, and the chain-neutral port (`chain.Adapter`) exposed
only `Submit`. Two decisions: where the read seam lives, and how to decode
attacker/node-controlled return bytes safely.

## Decision
- **A new chain-neutral `chain.BalanceReader` port** (`BalanceOf(ctx, token, holder) (*big.Int,
  error)`), stdlib-only, mirroring `chain.Adapter` — so `internal/reconcile` and the CLI depend
  on an interface, not go-ethereum, and the read stays testable with a fake. The EVM impl
  (`internal/chain/evm/balancereader.go`) confines all go-ethereum specifics.
- **`CallContract` folded into the existing `ethRPC` interface, NOT a parallel seam** (razor
  fold). The architect's first design added a separate one-method `contractCaller` interface;
  the razor cut it because `ethRPC` already abstracts exactly the `*ethclient.Client` surface
  and both the real client and go-ethereum's `simulated.Backend` already satisfy
  `CallContract(ctx, msg, blockNumber)`. Adding one method to the seam every EVM consumer
  already shares is smaller than a second seam whose only effect would be forcing every adapter
  fake to grow an unused method.
- **Fail-closed decode of the `balanceOf` return.** The returned bytes are node/attacker
  controlled, so `BalanceOf`: (1) rejects a malformed `token` or `holder` via
  `common.IsHexAddress` **before** any RPC call (wrapping `chain.ErrInvalidIntent`); (2) guards
  `len(out) < 32` with a clear error rather than decoding a short buffer (a non-contract address
  returns empty); (3) takes exactly the first word, `new(big.Int).SetBytes(out[:32])`, so an
  oversized return can neither panic nor mis-decode; (4) wraps any transport error through the
  existing `RedactRPCError` so a managed-node endpoint/API-key never reaches a caller, report,
  or log. No panic path on any input.
- **`packERC20BalanceOf` mirrors `packERC20Transfer`** — selector `0x70a08231` + the 32-byte
  left-padded holder address — keeping calldata construction uniform and golden-testable.

## Consequences
- New `internal/chain/balance.go` (port) + `internal/chain/evm/balancereader.go` (impl,
  `var _ chain.BalanceReader`); `packERC20BalanceOf` appended to `calldata.go`; one method added
  to the `ethRPC` interface (both real and simulated clients already satisfy it, and the compile-
  time `rpc_test.go` assertions still pass). The evm test fake gained a `CallContract` stub.
- The read boundary is verified: golden calldata bytes, a `balanceOf` happy path over a fake
  `ethRPC` returning a known 32-byte word, and the malformed-address / short-return / redacted-
  error paths. The CLI wires an `ethclient` dial (mirroring `broadcast.go`) into
  `evm.NewBalanceReader`; the dial-error message redacts the endpoint to `scheme://host`.
- The port is thin (its only consumer, the CLI, already imports `evm` concretely) — it earns its
  place as convention-parity and by keeping `internal/reconcile` go-ethereum-free, not as
  load-bearing indirection. `--format json`/machine output was deferred (no consumer yet).
