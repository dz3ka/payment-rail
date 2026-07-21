# ADR-0013: Signer custody upgrade & mTLS caller auth (deferred)

- **Status:** Proposed (deferred — records a deliberate scope boundary; not implemented)
- **Date:** 2026-07-21
- **Deciders:** Bogdan Dzekic

## Context

ADR-0009 set the signer's base custody posture: private keys held in a process-local
keyring, per-key spend limits, well-formed-payload-only signing, exposed over a
**loopback-only** gRPC endpoint with **no** mTLS or caller authentication (see the
package doc comment in `cmd/signer/main.go`). For a testnet, portfolio-scope rail
that is a defensible boundary — the signer is safe *because* it binds loopback and
moves only testnet funds. This ADR records the decision that would have to change
**first** before this system could go anywhere near mainnet or real value, so the
gap is explicit rather than an unstated assumption a future reader has to reverse-engineer.

## Decision (proposed, not yet built)

When/if the rail leaves testnet, upgrade the signer on two axes:

- **Custody:** move key material out of process memory into an **HSM or MPC** signer,
  so a host compromise no longer yields the raw keys. The existing per-key spend
  limits and well-formed-payload validation stay in front of it as defense-in-depth.
- **Caller authentication:** replace loopback-only trust with **mTLS** and a caller
  identity allowlist, so only the `api` / `paymentrailctl` service identities can
  request a signature even on a shared network.

Until that work is scheduled, this ADR stays **Proposed**: it commits to *why* the
current posture is acceptable now and *what* must precede a production deployment,
without claiming shipped work.

## Alternatives considered

- **Cloud KMS-managed keys:** viable, but introduces provider lock-in and is a poorer
  fit for a self-hostable project; HSM/MPC keeps the "self-hostable" property.
- **App-level bearer tokens instead of mTLS:** authenticates the request but not the
  transport, and leaks if logged; mTLS binds identity to the connection.
- **Do nothing / keep loopback-only off testnet:** unacceptable — loopback trust
  assumes a single-tenant host and offers no key isolation against host compromise.

## Consequences

- Easier: the scope boundary is now written down — the current signer is understood to
  be safe *only* under loopback + testnet, and the exact upgrade path is on record.
- Harder / deferred: the upgrade itself (HSM/MPC integration, cert issuance/rotation,
  a caller allowlist) is real work not yet done; this ADR must move to **Accepted** and
  a follow-up must implement it before any mainnet or real-value use. Supersedes nothing;
  it extends ADR-0009's custody decision.
