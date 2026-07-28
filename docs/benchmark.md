# Payment Rail — Load Test & Benchmark (M7)

This is the published benchmark for the REST payments API. It measures the
**payment-create path** (`POST /v1/payments`) under sustained concurrency and
reports throughput and latency percentiles. Numbers below come from a real run of
the `paymentrailctl loadtest` harness; the "Reproduce" section reproduces them
verbatim.

> ⚠️ Testnet/portfolio project. These are **local single-host** numbers against an
> **untuned dev Postgres**, meant to characterize the create path's shape and its
> bottleneck — not a production capacity statement.

## What is measured

`POST /v1/payments` is a **pure-Postgres** path — no chain, signer, or Kafka dial is
on the request path (those live in separate binaries). One create is a single
`ExecTx` that locks the source and destination accounts, runs the double-entry
balance check, inserts the journal entry + lines and the payment row, and — in the
same transaction — appends an outbox event and an audit-log record. So the benchmark
is effectively measuring **serialized double-entry commit throughput** with two
extra row writes riding each commit, plus HTTP framing over loopback.

Reads and cancels are intentionally **out of scope** for this run (see
[Limitations](#limitations)); a read/write mix was deferred to avoid mixing read
latencies into the write-path percentiles.

## Methodology

The harness (`cmd/paymentrailctl/loadtest.go`) is stdlib-only and does the following:

- **Seeds funded accounts** directly over the DSN: it creates `--accounts` source and
  `--accounts` destination accounts and credits each source an opening balance
  (`--opening-balance`, default `2^40`) via the same derived-balance "opening entry"
  shortcut the integration tests use. A high opening balance keeps sources solvent for
  the whole run so latency reflects the create path, not `insufficient_funds` churn
  (each payment is `amount=1`).
- **Spreads traffic across many accounts.** Each request picks a *random* source and
  destination from the seeded pool. Concurrent creates against the *same* source
  serialize on its `SELECT … FOR UPDATE` row lock, so a large `--accounts` pool is
  what lets concurrency translate into throughput.
- **Sends a unique `Idempotency-Key` per request** (a fresh UUID). The API requires
  the header and replays a cached response for a repeated key — so a reused key would
  measure idempotent-replay latency, not real creates.
- **Times each request in the load loop, not in the request code.** Each worker owns
  its own latency slice and outcome tally (no shared lock on the hot path, which would
  distort the latency being measured); the slices are merged and sorted once at the
  end and percentiles read off by **nearest-rank** (every reported figure is a value
  that actually occurred).
- **Classifies every request** into one of four outcomes — `OK` (2xx), `ClientError`
  (4xx), `ServerError` (5xx), `TransportError` (dial/timeout) — so saturation failure
  modes are visible, not hidden inside a single error count.

## Reproduce

Prerequisites: Docker, Go 1.25+, `make`. From the repo root:

```bash
# 1. Bring up the dev stack (Postgres 16 on :5432, matches the default DSN)
make up

# 2. Build and start the API (binds :8080)
go build -o bin/api ./cmd/api
./bin/api &            # or run in a second terminal; Ctrl-C stops it cleanly

# 3. First run ONLY: --migrate applies db/migrations to the fresh database,
#    then seeds accounts and drives load. (see note below)
go run ./cmd/paymentrailctl loadtest --migrate --requests=5000 --concurrency=32 --accounts=100

# 4. Subsequent runs: omit --migrate (schema already applied)
go run ./cmd/paymentrailctl loadtest --requests=20000 --concurrency=50 --accounts=200

# 5. Reset (drops the Postgres volume) and tear down
kill %1                 # stop the API
make down
```

**`--migrate` is a run-once, fresh-DB bootstrap.** It Execs `db/migrations/*.up.sql`
in order over the pool — no version tracking, no down migrations — because
[ADR-0025](adr/0025-loadtest-hermetic-migration-bootstrap.md) rules out shelling to host `psql` or adding a migration
dependency. Re-running it against an already-migrated database fails fast on the first
`CREATE TABLE` (`relation … already exists`); that is expected. `make down` (which
runs `docker compose down -v`) is the reset.

Flags: `--url` (default `http://localhost:8080`), `--dsn` (default the
`PAYMENT_RAIL_POSTGRES_DSN` value), `--concurrency` (32), `--duration` (30s) **XOR**
`--requests` (0), `--asset` (`USD`), `--accounts` (100), `--opening-balance` (`2^40`),
`--migrate` (false).

## Environment

| | |
|---|---|
| Go | go1.26.5 |
| OS / arch | Darwin (macOS) / arm64 |
| CPU | Apple M1 Pro, 10 cores |
| Postgres | `postgres:16-alpine` (docker-compose defaults, untuned) |
| Client → server | localhost loopback (same host) |

## Results

**Run 1 — concurrency 32, 5,000 requests**

| Metric | Value |
|---|---|
| Throughput | **567.93 req/s** |
| Latency Min / P50 / P95 / P99 / Max | 31.5 / 45.7 / 104.2 / 146.5 / 225.9 ms |
| Outcomes | OK 5000 · ClientError 0 · ServerError 0 · TransportError 0 |
| Elapsed | 8.80 s |

**Run 2 — concurrency 50, 20,000 requests**

| Metric | Value |
|---|---|
| Throughput | **568.03 req/s** |
| Latency Min / P50 / P95 / P99 / Max | 50.6 / 72.4 / 157.5 / 214.5 / 398.9 ms |
| Outcomes | OK 20000 · ClientError 0 · ServerError 0 · TransportError 0 |
| Elapsed | 35.21 s |

**100% success across 25,000 requests**, zero server or transport errors.

## Analysis

- **Throughput plateaus at ~568 req/s.** Raising concurrency from 32 to 50 left
  throughput essentially flat (567.9 → 568.0 req/s) while latency rose across the board
  (P50 45.7 → 72.4 ms, P99 146.5 → 214.5 ms). That is the signature of a **saturated
  bottleneck**: past the knee, extra concurrency only queues — it buys latency, not
  throughput. The bottleneck is commit throughput on a single untuned Postgres (each
  create is one synchronous, fsync-bounded transaction that also writes an outbox and
  an audit row), not the Go client or HTTP.
- **The numbers are internally consistent** with Little's Law (throughput ≈
  concurrency / mean-latency): 32 / 0.0457 s ≈ 700 and 50 / 0.0724 s ≈ 690 — in the
  right neighborhood of the observed ~568 once the tail is included.
- **Right-sizing concurrency** for this hardware sits near the run-1 point (~32
  workers): it achieves the same throughput as 50 at roughly half the P99.

## Limitations

- **Single-host loopback** — client and server share the CPU; no network latency and
  some client/server contention. Real deployments will differ in both directions.
- **Untuned dev Postgres** — default `shared_buffers`, `max_connections`, `fsync=on`,
  local Docker volume. Storage and Postgres tuning would move the plateau.
- **Create-only** — reads (`GET /v1/payments…`) and cancels are not exercised here;
  a read-heavy mix would report very different, and generally higher, throughput.
- **`amount=1`, fixed asset** — the harness drives a homogeneous workload; it does not
  model realistic amount/asset distributions or hot-account skew beyond random spread.
- The `idempotency_keys` table grows for the duration of a run (swept on a 24h TTL);
  a very long run bloats it — reset with `make down` between large runs.

## Decision

The one design decision worth recording is the `--migrate` hermetic bootstrap; see
[ADR-0025](adr/0025-loadtest-hermetic-migration-bootstrap.md). The harness itself follows the existing subcommand idiom
(`submit`/`reconcile`) and adds no new dependency.
