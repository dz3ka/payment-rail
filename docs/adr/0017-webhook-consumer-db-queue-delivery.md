# ADR-0017: Webhook consumer: DB-queue delivery, lease-claim worker, HMAC signing

- **Status:** Accepted. Fulfills the delivery/dead-lettering scope [0011] explicitly deferred to
  "M4 slice 3" (no supersede — [0011] named this as a follow-up, not a decision).
- **Date:** 2026-07-20
- **Deciders:** Bogdan Dzekic

## Context
M4 slice 1 [0011] shipped the producer side: a transactional outbox drained to Kafka
topic `payment-rail.events` at-least-once, with the explicit contract "consumers must
dedupe on the envelope `id`." Slice 3 builds the first consumer: `cmd/webhookd` (until
now an M0 skeleton) must deliver each domain event to operator-registered subscriber
endpoints as an HMAC-signed HTTP POST, retry with backoff, and dead-letter after N
attempts (PRD F7), plus a replay surface (PRD F11). The callee is now **untrusted,
possibly-slow external HTTP** — the opposite trust posture of slice 1's Kafka broker.
Must be provable hermetically (no broker, no Postgres): kafka-go and net/http stay in
adapters, the domain is spy-tested.

## Decision
- **DB-queue delivery, not inline retry.** The Kafka consume loop does the minimum
  durable thing — parse the envelope, `FanOutDelivery` one pending `webhook_deliveries`
  row per active subscription matching the event type, then commit the offset — and a
  **separate `webhook.Worker` poll-loop** owns the POST + backoff + dead-letter.
  Rejected: delivering inline in the consume loop. Backoff can sleep up to `backoffCap`
  (1h); blocking the partition on it head-of-line-blocks every later event and stalls
  offset commits. The two loops run under one `errgroup` in `cmd/webhookd`.
- **Offset ordering = at-least-once, crash-safe.** `FetchMessage` (manual commit) →
  fan-out DB write → `CommitMessages`, committing **only** after a successful fan-out.
  A crash between the DB commit and the offset commit ⇒ Kafka redelivers ⇒ the
  `UNIQUE (event_id, subscription_id)` + `ON CONFLICT DO NOTHING` no-ops. A poison
  message (unparseable envelope / bad event id) wraps sentinel `ErrPoisonMessage` →
  skip + commit (never wedge the partition on an always-failing message); a transient
  DB error is returned raw → **not** committed → the loop returns and the process
  restarts from the last committed offset (return-and-restart; re-processing is
  idempotent). This deliberately **inverts [0011]'s `drainBatch`**, which held the
  Kafka publish *inside* the DB tx — safe there because the broker is trusted; here the
  POST is untrusted and must never run inside a tx or hold a row lock.
- **Lease-claim in one statement.** `ClaimDueDeliveries` is a single
  `UPDATE … FROM … WHERE id IN (SELECT … WHERE status='pending' AND next_attempt_at<=now()
  ORDER BY next_attempt_at LIMIT $n FOR UPDATE SKIP LOCKED) RETURNING …` that atomically
  selects due rows **and** pushes their `next_attempt_at` forward by `leaseSeconds` (60).
  This is the crash-recovery window: a worker that dies mid-POST leaves its rows leased,
  so they are not re-claimed until the lease expires, then retried — without holding any
  lock across the untrusted HTTP call. `FOR UPDATE SKIP LOCKED` bounds concurrent-claim
  blast radius. Mirrors [0011]'s `ClaimUnsentOutbox` idiom.
- **Single `webhook_deliveries` table with a status enum** (`pending`/`delivered`/
  `dead_letter`), not a separate dead-letter table — dead-letter is a terminal status,
  replay flips it back to `pending`. Mirrors the outbox's single-table `sent_at`
  sentinel (migration 0006).
- **HMAC-SHA256, Stripe-style.** Header `X-Payment-Rail-Signature: t=<unix>,v1=<hex>`
  where the MAC covers `"<t>." + body` — the timestamp in the signed content gives the
  receiver replay protection, and the body is written to the MAC **raw** (never through
  `%s`, which would mangle non-UTF-8 bytes into an unverifiable signature). Secret is a
  `[]byte`, never logged.
- **Export `outbox.Envelope` + `ParseEnvelope`** as the single wire source of truth.
  The consumer is the second reader [0011] wanted before exporting; a private mirror
  struct would silently drift on a `schema_version` bump. Field order/tags unchanged
  (still test-asserted) — the wire bytes are byte-identical.
- **Razor outcomes.** CUT a `MessageStream` port (the reader stays concrete in
  `cmd/webhookd/kafka.go`; the pure `Handle` is spy-tested, mirroring how slice 1 kept
  the producer concrete and tested `drainBatch`). CUT the `WebhookConsumerGroup` config
  key → an `internal/webhook` constant (`DefaultConsumerGroup="webhookd"`, one fixed
  identity). CUT dual-mode replay → a single `paymentrailctl replay-webhook
  --subscription-id` (re-drive all dead-lettered rows for one subscription — the real
  "I fixed my endpoint" action). KEPT `WebhookPollInterval` config (per-env cadence knob,
  like `OutboxPollInterval`), the `Sender` port (tests the worker without HTTP), and the
  `internal/webhook` package (shared by the CLI replay path). `leaseSeconds` kept as a
  constant over the razor's DEFER: it is load-bearing for crash recovery, not just
  multi-worker safety.

## Consequences
- Seven binaries now real incl. `webhookd`; new `internal/webhook` domain package
  (transport-free: no net/http, no kafka-go), with kafka-go confined to
  `cmd/webhookd/kafka.go` and net/http to `cmd/webhookd/httpsender.go`.
- Migration 0006 adds `webhook_subscriptions` + `webhook_deliveries` (status enum,
  `UNIQUE(event_id, subscription_id)`, partial index on due pending rows).
- `internal/config` gains `WebhookPollInterval` (`PAYMENT_RAIL_WEBHOOK_POLL_INTERVAL_SECONDS`,
  default 5s); `KafkaBrokers` reused as-is.
- At-least-once delivery: a subscriber may receive a duplicate (e.g. a late row past its
  60s lease, or redelivery after a crash) — the signed `t`/event-id header let the
  subscriber dedupe. Documented, in-contract.
- **SSRF stance:** scheme allowlist (`http`/`https`) enforced before the request,
  redirects refused (`CheckRedirect` errors), `HTTPTimeout` (10s), response body bounded
  by `io.LimitReader` (`RespBodyCap`). `url.Parse` failures return a **generic** error
  (its message echoes the raw URL, which could carry embedded userinfo credentials).
  **Deferred (named):** HTTPS-only enforcement + private/reserved-IP (DNS-rebind)
  blocking — acceptable because subscriber URLs are operator-registered and the
  deployment is testnet-only. Trigger to promote: accepting subscriber URLs from
  end-user/tenant input, or a production (mainnet) deployment.
- Secrets: `signing_secret` and the event payload never appear in logs, `last_error`, or
  stdout (asserted by a redaction test on the worker's most verbose path).
- **Deferred follow-ups:** subscription CRUD API (rows seeded directly for now),
  SASL/TLS to Kafka, a per-outcome metrics gauge (slog-only, per [0011]), multi-replica
  webhookd delivery ordering, and a live broker+Postgres e2e (no Docker in CI — hermetic
  spy tests are the gate, matching slice 1).

## Related
[0011] transactional outbox & event schema — the producer this consumes; named this
slice's dead-lettering/backoff as deferred scope; supplies the `Envelope` wire contract
and the Claim/`SKIP LOCKED` idiom. [0015] hermetic Go dev tooling — the sqlc/lint pins
this slice's regen + `errorlint`-clean error wrapping run under.
