# ADR-0025: Hermetic Go migration bootstrap for the load-test harness

- **Status:** Accepted. M7 (load tests + benchmark), the only ADR of this slice (chaos tests
  deferred to a separate run). Follows the hermetic-tooling stance of ADR-0015 and the
  `paymentrailctl` subcommand idiom of ADR-0018/0022; supersedes none.
- **Date:** 2026-07-21
- **Deciders:** Bogdan Dzekic

## Context
The M7 load-test harness (`paymentrailctl loadtest`) must run against a schema-bearing
Postgres to produce a *reproducible* benchmark on a fresh checkout. But the repo has
**no migration-apply tooling at all**: the 9 `db/migrations/000N_*.{up,down}.sql`
pairs are applied by hand, `sqlc` is codegen-only, and there is no `migrate` binary,
library, or Makefile target. Meanwhile ADR-0015 makes hermeticity a hard constraint —
no host installs, no new external dependencies — which rules out both a `make migrate`
that shells to host `psql` and pulling in a migration framework (`golang-migrate` et
al.) just so a benchmark can bootstrap a database.

So the choice was: leave schema-apply a documented manual prerequisite (leaving the
"published benchmark" non-reproducible without an out-of-band step), or give the tool a
minimal way to apply the schema itself within the hermetic constraint.

## Decision
- **A `--migrate` flag on `loadtest` that Execs `db/migrations/*.up.sql` over the pool
  the seeder already opens** (`applyMigrations`: `os.ReadDir` → filter `*.up.sql` →
  `sort.Strings` (lexical = migration order, because the files are zero-padded
  `000N_`) → `sqlDB.ExecContext` each in turn, fail-fast with `%w`). Pure Go over the
  existing `database/sql` pool: no host `psql`, no new dependency — the only path that
  honors ADR-0015.
- **Deliberately a run-once fresh-DB bootstrap, not a migration manager.** No version
  table, no down migrations, no re-run safety. Re-running `--migrate` against an
  already-migrated database fails fast on the first `CREATE TABLE`
  (`relation … already exists`); that is the documented contract, and `make down`
  (which drops the volume) is the reset. This is acceptable *precisely because* it is a
  bench-tool bootstrap, not production schema management — building version tracking
  here would be exactly the framework ADR-0015 tells us not to add.
- **Off by default.** A normal run assumes the schema exists (the common case: a dev
  stack that's already been migrated); `--migrate` is the explicit opt-in for a
  first-run-on-a-fresh-DB.

## Consequences
- The benchmark is reproducible from a clean checkout with in-repo tooling only:
  `make up` → `loadtest --migrate …` → results, no external step. The reproduce section
  of `docs/benchmark.md` documents the run-once semantics and the `make down` reset.
- `applyMigrations` is ~20 lines living in `cmd/paymentrailctl/loadtest.go`; it is a
  bench-tool convenience, not a sanctioned migration path — production/CI still applies
  migrations by hand as before. If a real migration need lands elsewhere (a service
  that must self-migrate, CI schema provisioning), that is the trigger to introduce
  proper tooling (version table, up/down, idempotency) and retire this flag — noted as
  the removal trigger so it does not calcify into a fake framework.
- Down migrations remain unused by the tool; a benchmark reset is volume-drop, not a
  rollback, which keeps the tool honest about being one-directional.
