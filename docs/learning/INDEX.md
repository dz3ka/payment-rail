# Conduit — Go Learning Journal

Conduit is a deliberate Go learning vehicle. Each milestone produces a lesson
capturing the **why** behind the code: the Go idioms used, the alternatives
weighed, and the mistakes avoided. This index links them in order.

| Milestone | Lesson | Focus |
|-----------|--------|-------|
| M0 | [m0-repo-skeleton.md](m0-repo-skeleton.md) | Module & `cmd/`/`internal/` layout, thin entrypoints, table-driven tests, graceful shutdown skeleton |
| M1 | [m1-double-entry-ledger.md](m1-double-entry-ledger.md) | Derived (never-stored) balances, `%w`/`errors.Is`/`errors.As` sentinels, and a fakeable `Store`/`Querier` transactor seam tested with no database |
| M1 | [m1-payments-idempotency.md](m1-payments-idempotency.md) | Composing a payment + journal entry in one tx via a shared `Querier` (`PostWithin`), an idempotency middleware built on a buffering `http.ResponseWriter`, and stable keyset (cursor) pagination |

_Lessons for M2–M7 are added as those milestones ship._
