# ADR-0009: Signer key custody & per-key spend limits

- **Status:** Accepted
- **Date:** 2026-07-17
- **Deciders:** Bogdan Dzekic

## Context

The signer is the one service that holds private keys and must stay isolatable from the
rest of the system (ADR-0001). Slice 1 needs custody with no secret in env or repo (an
NFR) and a spend cap so a compromised or buggy caller can't drain a key. Caller
authentication (mTLS) is not yet built, so the blast radius must be bounded elsewhere.

## Decision

We decided two linked things.

**Key custody:** private keys are raw-hex files at mode **0600**, permission-checked at
load — a group/world-readable key is rejected, not warned. A committed, secret-free JSON
manifest (`key_id`, `key_file`, `chain_id`, `spend_limit`) names them; `.gitignore`
already covers `*.key`/`*.pem`/`.env`, so no secret ever enters env or repo.

**Spend limits:** each key has a **cumulative** cap over process lifetime, held in memory
behind one `sync.Mutex` per key. Invariant: per key, Σ(charged amounts of successful
signs) ≤ `spend_limit`, enforced in one critical section {check → sign → commit}, so no
two signs both pass the check and jointly exceed the cap.

**Deferred:** mTLS / caller auth on the gRPC endpoint is out of slice 1, mitigated by
binding the signer to loopback (`127.0.0.1`) only.

## Alternatives considered

- **Encrypted keystore:** its unlock passphrase just becomes a new env secret — the
  thing the NFR forbids. **Key hex in env:** the same violation, more exposed.
- **Atomic counter for the cap:** a uint256 total needs `big.Int`, which needs a mutex
  anyway. **Per-time-window cap:** wall-clock makes tests non-deterministic.

## Consequences

- Easier: custody and cap reproduce from committed config plus local key files; the
  per-key mutex keeps the money invariant in one auditable critical section.
- Limitation: in-memory counters reset on restart. This fails **safe** — a reset only
  lowers the spent total, never grants more than a fresh limit — and the upgrade is a
  **signer-owned** persistent store, never the ledger DB (that would break the
  service-isolation boundary of ADR-0001). HSM/MPC is the future custody path.
