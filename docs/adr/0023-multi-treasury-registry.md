# ADR-0023: Multi-treasury registry: per-asset expected, per-address actual

- **Status:** Accepted. M6 (PRD F10), second of three M6 ADRs (reconciliation model ADR-0022 +
  this + BalanceReader ADR-0024). Reuses the JSON-manifest loader convention from the
  denylist (ADR-0018) and the signer keyring; supersedes none.
- **Date:** 2026-07-21
- **Deciders:** Bogdan Dzekic

## Context
F10 says reconcile "per treasury address" (plural), but the deployed reality is exactly
one sender address (`ChainFromAddress`) and one asset (USDC), with a single per-asset
`onchain_settlement` house account. Two forces:
- **The wording implies a set; the config had a scalar.** Building a registry now (vs.
  hardcoding the one address) was a user-confirmed choice — future-proofing the "per
  address" contract without a later refactor.
- **A genuine model tension:** the ledger has ONE house account per asset and records no
  per-address on-chain attribution. So "expected balance *per address*" is not derivable
  — the ledger cannot say how the finalized total is split across N treasury addresses.

## Decision
- **Registry = per-asset expected with a per-address actual breakdown.** Expected value and
  the discrepancy/PoR verdict are computed **per asset** (the granularity the ledger
  actually tracks); each registered treasury address contributes its own `balanceOf` as a
  displayed line and those actuals sum into the asset's total. This degrades cleanly to the
  1:1 single-(address,asset) reality today. The rejected per-address *expected* fails
  because the ledger records no per-address on-chain attribution to divide the finalized
  total by — inventing one would be a fictional number.
- **Registry from a JSON manifest file, with a `Chain*` single-entry fallback.**
  `PAYMENT_RAIL_RECONCILE_TREASURIES=""` (the default) makes the reconcile command derive a
  one-entry registry via `reconcile.SingleEntry(ChainFromAddress, "USDC", ChainUSDCAddress)`
  — the single treasury that exists today needs no manifest. A set path loads a manifest via
  `reconcile.LoadRegistry`, fail-closed: unreadable file, malformed JSON, zero entries, an
  entry failing validation, or a duplicate `(address, asset)` pair each return an error and a
  zero registry rather than silently reconciling against nothing. The manifest is a bare JSON
  array of `{address, asset, token}` objects, mirroring the denylist manifest shape. (Rejected:
  a CSV env knob — it cannot cleanly express the 3-field tuple the manifest carries.)
- **`internal/reconcile` stays stdlib-only** (like `internal/audit`). Registry address
  validation is a lightweight stdlib check (`0x` + 40 hex via `encoding/hex`); canonical
  address validation is the `BalanceReader`'s job (ADR-0024, fail-closed at the RPC boundary),
  so the domain core takes no go-ethereum dependency.

## Consequences
- `internal/reconcile/registry.go` (`TreasuryEntry`, `Registry`, `LoadRegistry`, `SingleEntry`,
  `TreasuryEntry.Validate`) + one config knob `ReconcileTreasuries`. The reconcile core seeds
  `actuals` with an entry for every registry asset (empty slice if it has no addresses) and
  computes `liabilities[asset]` per distinct registry asset, so `BuildReport` sees consistent
  keys and no nil `*big.Int` surprises.
- Multi-address/multi-asset reconciliation becomes a manifest edit, not a code change — but is
  untested beyond the single real address, and per-address expected remains fundamentally
  unavailable (documented; the asset-level roll-up is authoritative).
- Asset symbols are matched case-sensitively in the duplicate check (only the hex address is
  case-folded), matching the ledger's asset-string semantics.
