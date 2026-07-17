# Conduit — Go Learning Journal

Conduit is a deliberate Go learning vehicle. Each milestone produces a lesson
capturing the **why** behind the code: the Go idioms used, the alternatives
weighed, and the mistakes avoided. This index links them in order.

| Milestone | Lesson | Focus |
|-----------|--------|-------|
| M0 | [m0-repo-skeleton.md](m0-repo-skeleton.md) | Module & `cmd/`/`internal/` layout, thin entrypoints, table-driven tests, graceful shutdown skeleton |
| M1 | [m1-double-entry-ledger.md](m1-double-entry-ledger.md) | Derived (never-stored) balances, `%w`/`errors.Is`/`errors.As` sentinels, and a fakeable `Store`/`Querier` transactor seam tested with no database |
| M1 | [m1-payments-idempotency.md](m1-payments-idempotency.md) | Composing a payment + journal entry in one tx via a shared `Querier` (`PostWithin`), an idempotency middleware built on a buffering `http.ResponseWriter`, and stable keyset (cursor) pagination |
| M2 | [m2-isolated-grpc-signer.md](m2-isolated-grpc-signer.md) | A pb-agnostic signing domain behind a proto↔domain gRPC adapter (sentinel→status mapping), a per-key `sync.Mutex` spend limiter with `{check→sign→commit}` as one critical section, defensive `deepCopy` of mutable `*big.Int`/slices at a key-holding trust boundary, and go-ethereum EIP-1559 signing |

_Lessons for M2–M7 are added as those milestones ship._
