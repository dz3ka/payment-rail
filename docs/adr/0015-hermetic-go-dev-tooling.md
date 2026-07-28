# ADR-0015: Hermetic Go dev tooling: golangci-lint and sqlc as go.mod tool directives

- **Status:** Accepted. Adopts retro `2026-07-20-m3-slice2-settlement-effects` (3rd occurrence of the
  unpinned-Go-dev-tool class).
- **Date:** 2026-07-20
- **Deciders:** Bogdan Dzekic

## Context
`make lint` and `make sqlc` invoked bare `golangci-lint` / `sqlc` binaries that had to be
`go install`ed by hand onto each contributor's PATH. Three separate ship sessions hit the same
friction (golangci-lint twice, sqlc once), and because a manual install leaves no go.mod/go.sum
trace, the gap is invisible to the next fresh checkout. The repo already had the fix pattern in
place for exactly one tool — `proto: go tool buf generate` against a `tool (...)` block in
go.mod — so the tools were inconsistent, not the pattern.

## Decision
- **Pin `golangci-lint` and `sqlc` via `go.mod` tool directives** (Go 1.24+ `go tool`),
  alongside the existing `buf` entry, and switch the two Makefile targets to
  `go tool golangci-lint run` / `go tool sqlc generate`. A fresh checkout now needs only
  `go` on PATH — the tool versions and their transitive deps are locked in go.mod/go.sum.
- **`sqlc` pinned to v1.31.1**, the exact version that generated the committed `internal/db/*`,
  so `go tool sqlc generate` reproduces that package byte-for-byte (verified: no diff).
- **`golangci-lint` pinned to latest v2 (2.12.2)**, not the dead v1 line. Adopting a
  still-maintained major is worth the one-time config migration below.
- **Applied via `go get -tool ...@<version>` + `go mod tidy`**, not by hand-editing go.mod, so
  the tool directive and the go.sum locks land atomically and consistently.

## Consequences
- **`.golangci.yml` migrated v1 → v2 schema** (unanticipated by the retro, forced by the
  latest-v2 pin). Done with golangci-lint's own `migrate` subcommand, then verified `0 issues`.
  The migration is behavior-preserving: errcheck/govet/ineffassign/staticcheck/unused are
  default-enabled in v2 so they dropped out of the explicit `enable` list while staying active;
  gofmt/goimports moved to the new `formatters` block; `linters-settings` → `linters.settings`
  and `issues.exclude-rules` → `linters.exclusions.rules`. The migrator strips comments, so the
  errcheck/test-helper rationale comments were restored by hand.
- go.mod/go.sum grow substantially (both tools pull large transitive graphs as tool deps) — the
  cost of hermeticity, paid once in the lockfiles rather than repeatedly on every checkout.
- **CI's lint job switched from `golangci-lint-action@v6` (pinned v1.64.8) to
  `go tool golangci-lint run`.** The action's pinned v1 could not parse the migrated v2-schema
  config and would have failed; running the go.mod-pinned tool makes CI use the exact same
  version as `make lint`, with no second version to drift. `.github/workflows/ci.yml` is the
  one repo file outside go.mod/go.sum/Makefile/.golangci.yml that this change touches.
- **`go mod tidy` bumped the `go` directive `1.25.10` → `1.26.0`** (a new tool dep requires it).
  CI's `setup-go@v5` still requests `1.24` and relies on Go's toolchain auto-download (already
  true for the prior `1.25.10`), so no CI job breaks — it just fetches the 1.26 toolchain.
- This is a tooling change logically independent of the M3 slice 2 diff; the two were committed
  separately.

## Related
Retro `2026-07-20-m3-slice2-settlement-effects` (the proposal this adopts). Mirrors the `buf`
tool-directive pattern already in go.mod.
