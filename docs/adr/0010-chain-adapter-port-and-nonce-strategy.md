# ADR-0010: Chain-adapter port, signer-boundary isolation & nonce strategy

- **Status:** Accepted
- **Date:** 2026-07-18
- **Deciders:** Bogdan Dzekic

## Context

M2 slice 2 adds the EVM payment path (F3): build an ERC-20 USDC transfer, get it
signed by the isolated signer (ADR-0009), and broadcast it. Three things need a
recorded "why". (1) F3 and the non-goals require a **chain-adapter interface** so a
non-EVM chain (Solana) could be added later without touching callers. (2) The
adapter is a *client* of the signer across a process boundary — it must not become a
back door around the key-isolation of ADR-0001/0009. (3) Ethereum nonces are
per-sender sequential and gap-intolerant, but `Submit` must be safe under concurrent
callers, and the chain — not our memory — is the authority on the next nonce.

## Decision

**Port:** a chain-neutral `chain.Adapter` (`Submit(ctx, PaymentIntent) (TxHash, error)`)
in `internal/chain`, imports stdlib only. The EVM impl lives in `internal/chain/evm`;
a future `internal/chain/solana` would implement the same port without importing `evm`.

**Signer boundary:** the EVM adapter depends on an outbound `evm.Signer` interface with
its own `SignerRequest`/`SignedTx` DTOs — never `internal/signer`'s domain types nor the
generated `internal/signerpb`. The proto↔domain client mapping lives in the composition
root (`cmd/paymentrailctl/signerclient.go`); uint256 fields cross the wire as canonical
big-endian bytes. The adapter holds no keys and aborts before broadcast if `signed.From`
≠ the configured sender. This mirrors how the signer keeps its domain proto-free.

**Nonce:** an in-memory per-sender high-water; `nonce = max(highWater, PendingNonceAt)`;
one `sync.Mutex` held across the whole `{query → sign → broadcast}` section; the
high-water advances **only on broadcast success** (gap-free), and a restart re-seeds from
the chain (fail-safe) — the same in-memory, commit-on-success shape as ADR-0009's spend
counter.

## Alternatives considered

- **No port / concrete `*evm.Adapter` only** (razor's proposal): rejected — F3 and §3/§7
  mandate the adapter *interface* as a demonstrated pattern; a `solana` impl must not
  import `evm` for the shared types.
- **Import the signer's `SignRequest` or `signerpb` directly:** couples a cross-process
  client to server internals / drags proto into a domain package — breaks the isolation
  and dependency-direction invariants.
- **Return `PendingNonceAt` with no local counter:** two concurrent submits collide on the
  same nonce (txpool lag). **Advance-at-allocation:** a failed broadcast burns a nonce and
  stalls the sender.
- **Decimal-string uint256 on the wire:** precision/format disagreement at a money
  boundary; big-endian bytes round-trip octet-for-octet.

## Consequences

- Easier: one seam to add a chain; the signer stays the only key holder; concurrent
  `Submit`s are correct and gap-free, proven by a full-wire gRPC→real-signer→simulated-EVM
  e2e and a `-race` concurrency test.
- Harder / deferred: the shared mutex serializes all sends for a sender (fine for the
  one-shot CLI; per-sender parallelism deferred). Gas/fee **caps** are configurable and a
  hostile RPC estimate is rejected via an overflow-safe buffer multiply. Observability is a
  redacted slog line now; the OTel span is designed but deferred to a tracer-provider slice
  (NFR §6). The signer-client adapter is extracted to `internal/signerclient` on its second
  consumer (`cmd/api`).
