# Threat Model (STRIDE-lite)

> **M0 stub.** This is the seed of the threat model; it is expanded as each
> component gains real behavior. It records the trust boundaries the design must
> honor so security is designed in, not bolted on after an incident.

## Scope & assumptions

- **Testnet only.** No real funds; no mainnet. This bounds impact but the
  controls below are designed as if funds were real (that is the exercise).
- Deployment target is a single operator's infrastructure (self-hosted).
- Secrets (signer keys, DB creds) are provided out-of-band, never committed.

## Trust boundaries

1. **Public REST edge (`api`)** — untrusted callers. Everything crossing it is
   hostile until validated.
2. **Signer boundary (`signer`)** — the highest-value asset (keys). Network-
   isolated; reachable only by `api`/orchestration over gRPC; signs only
   well-formed, policy-approved payloads; enforces per-key spend limits
   independently of the caller.
3. **Datastore boundary (Postgres)** — holds the ledger, outbox, and audit log;
   integrity here is the definition of correctness.
4. **Chain boundary (EVM RPC)** — external, adversarial, and reorg-prone; never
   treated as a trusted source of truth.

## STRIDE-lite (per boundary, to be expanded)

| Threat | Where it bites | Planned control | Milestone |
|--------|----------------|-----------------|-----------|
| **S**poofing | Webhook consumers, API callers | HMAC-signed webhooks; API auth | M4 / M1 |
| **T**ampering | Ledger rows, audit log | Serializable txns; hash-chained append-only audit log | M1 / M5 |
| **R**epudiation | Operator actions | Immutable audit log covering every state transition | M5 |
| **I**nfo disclosure | Keys, secrets | Signer isolation; no secrets in repo; `.env` git-ignored | M0 / M2 |
| **D**enial of service | API, chain RPC | Velocity limits; gas caps; backoff | M5 / M2 |
| **E**levation of privilege | Signer misuse | Per-key spend limits; four-eyes approval above threshold | M2 / M5 |

## Non-goals (v1)

Custody/HSM/MPC productization, real KYC vendor integration, and mainnet
hardening are out of scope; the HSM/MPC custody path is documented as a future
ADR (ADR-0006).
