# ADR-0002: Persistence & data access (sqlc)

- **Status:** Accepted
- **Date:** 2026-07-16
- **Deciders:** Bogdan Dzekic

## Context

The ledger is the source of truth and demands hand-controlled SQL: serializable
transactions, explicit row locking, derived (not stored) balances, and a
transactional outbox written in the same transaction as journal entries. We need
a data-access approach that keeps SQL first-class and type-safe without hiding
transaction boundaries behind an ORM.

## Decision

Use **sqlc**: write SQL, generate type-safe Go query methods from it. Postgres
is the single relational store (ledger + outbox + idempotency keys). Migrations
are plain SQL files applied by a lightweight migrator (tool chosen in M1).

## Alternatives considered

- **GORM (or another ORM):** fast for CRUD, but obscures exactly the things this
  ledger cares about — lock modes, isolation levels, and the precise shape of
  each transaction. Correctness-over-throughput makes hidden SQL a liability.
- **Hand-rolled `database/sql` repositories:** maximal control, but boilerplate
  per query and easy to drift from the schema. sqlc gives the same control with
  generated, schema-checked types.

## Consequences

- Easier: SQL stays reviewable and version-controlled; queries are type-checked
  against the schema at generate time; transaction boundaries are explicit in Go.
- Harder: adds a codegen step (`sqlc generate`) to the workflow and a schema
  source of truth that migrations and queries must stay aligned with.
- Follow-up: schema, migrations, and the first generated queries land in M1;
  property-based tests assert ledger invariants (debits == credits).
