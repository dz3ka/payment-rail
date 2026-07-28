# ADR-0026: Gating the chaos suite: the repo's first build tag + DSN skip

- **Status:** Accepted. M7 (chaos-tests half). Introduces the repo's first `//go:build` tag;
  follows the `PAYMENT_RAIL_TEST_DSN` + `t.Skip` integration-test convention
  (store_sql_integration_test.go et al.) rather than superseding it. Supersedes none.
- **Date:** 2026-07-21
- **Deciders:** Bogdan Dzekic

## Context
The M7 chaos suite (`internal/chaos`) drives the real payment/settlement lifecycle
against dev-stack Postgres and injects faults — it is slow, destructive (kills
backends, rolls transactions), and needs a live migrated DB. Two things had to be true:
it must NOT run in the default `go test ./...` / CI path (CI has no Postgres and the
tests are heavy), and it must no-op gracefully when a developer runs it without a DB.
The repo had **no build tags at all** — every "needs infra" test gates purely at
runtime on `os.Getenv("PAYMENT_RAIL_TEST_DSN")` + `t.Skip`. A runtime skip alone
cannot keep a test *out of* `go test -race ./...`; the test still compiles and runs,
it just skips — which is wrong for a suite that is expensive and destructive by design.

## Decision
- **Both gates, each carrying distinct weight.** `//go:build chaos` on line 1 of every
  `internal/chaos/*.go` file excludes the whole package from untagged builds (`go test
  ./...` reports "matched no packages" for it — verified), so the suite is invisible to
  the default path and CI. The `PAYMENT_RAIL_TEST_DSN` + `t.Skip` gate then makes
  `-tags chaos` a clean no-op when no DB is configured, matching the existing convention.
- **A `test-chaos` Makefile target** (`go test -race -tags chaos ./internal/chaos/...`)
  is the discoverability surface for the novel tag, consistent with the existing
  `test`/`cover`/`vet`/`lint` convenience targets.
- Rejected: DSN-skip only (can't exclude from `-race ./...`). Rejected: tag only
  (would hard-fail instead of skip without a DB, breaking the repo's skip convention).

## Consequences
- The suite is local-and-explicit: it runs only under `make test-chaos` with a DSN set,
  never in CI (documented — CI has no Postgres service, so it is a dev-stack exercise).
- Contributors unaware of the tag never exercise chaos; mitigated by the Makefile target
  + docs note. Rollback is trivial: delete `internal/chaos/` and the target.
