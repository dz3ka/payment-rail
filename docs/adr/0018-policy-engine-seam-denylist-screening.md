# ADR-0018: Policy engine seam + destination-address denylist screening

- **Status:** Accepted. First slice of M5 (PRD F8 policy engine). Introduces the reusable policy
  seam; velocity limits (F8b) and four-eyes approval (F8c) are later slices at their
  own seams and are NOT built here. Supersedes nothing.
- **Date:** 2026-07-21
- **Deciders:** Bogdan Dzekic

## Context
PRD F8 requires inline compliance controls; §7 explicitly names "ports-and-adapters
for chains/**screening providers**" as a pattern to demonstrate, and F8 calls for a
"denylist screening (pluggable provider, mock included)". The meaningful subject of an
address denylist is the on-chain destination — but that address does not exist in the
payment-create/API path (payments carry internal ledger account UUIDs); the `--to 0x…`
address only enters at broadcast time in `cmd/paymentrailctl/submit.go`. So slice 1
screens there, before the tx is signed or broadcast. Must be provable hermetically
(no chain, no signer, no DB): the screen returns before any dial.

## Decision
- **Screen the on-chain `--to` address in `submit.go`, before every dial/sign.** A
  denylisted or unreadable-manifest outcome aborts before the signer/chain are dialled.
  Ordering in the composition root is the security property.
- **One `Screener` interface kept; Request/Decision structs and a two-package split
  CUT.** A razor pass argued the interface itself was speculative (one concrete impl).
  Overridden: PRD F8 mandates a *pluggable* provider — the file `Denylist` is the "mock
  included", a real sanctions-API screener (I/O-backed) is the intended second impl at
  this **same** address-screening seam, hence the interface and the `ctx` parameter
  stay. Rejected as YAGNI: a single-field `Request{To string}` wrapper (pass a
  `string`) and a `Decision{Allowed,Reason}` value (return an `error`).
- **Deny-as-error, sentinel `ErrDenied`.** `Screen` returns nil to allow and
  `fmt.Errorf("…%w", ErrDenied)` to deny; `errors.Is` distinguishes a denial from an
  operational failure, and both **fail closed**. Matches the repo's existing
  `%w`-sentinel idiom (`chain.go`) rather than adding a Decision value type.
- **File/JSON manifest storage, not a DB table.** Loaded via a config path
  (`PAYMENT_RAIL_POLICY_DENYLIST`), keyring-style. `""` = screening disabled (allow-all,
  no file read) — preserving submit's "Postgres untouched without `--payment-id`"
  contract. A set-but-missing/malformed/bad-address manifest aborts (fail-closed).
  Manifest and Screen normalize through the **same** `common.HexToAddress`, closing an
  EIP-55 checksum/case bypass. Trigger to move to a DB table: a runtime
  operator-mutable/shared denylist without redeploy.
- **Log-hygiene exception:** a denied recipient + reason ARE logged at WARN (the
  compliance audit event); operational screen failures log at ERROR. Allowed payments
  log nothing about the recipient; the amount is never logged. Deliberate, deny-only
  exception to the repo's "never log recipient" rule.

## Consequences
- New `internal/policy` package (interface + concrete `Denylist`), one config knob,
  ~4 files; no schema, API, or proto change. Fully gated by the env var — unset = the
  legacy no-op path.
- Fixed a latent bug in passing: `submit.go` never format-validated `--to`, so
  `common.HexToAddress`'s right-most-20-bytes truncation would silently accept garbage;
  an up-front `common.IsHexAddress` guard now makes the screen sound.
- The submit-CLI is the only screening seam today; when the REST create path gains an
  on-chain destination, the same `Screener` wires in there too.
