# ADR-0029: Preview environments — a topology-only PaymentStack template

- **Status:** Accepted
- **Date:** 2026-08-02
- **Deciders:** Bogdan Dzekic

## Context

bosun (the sibling platform repo) exposes a reusable GitHub workflow,
`dz3ka/bosun/.github/workflows/preview-environment.yml`, that renders a committed
`PaymentStack` manifest into a per-PR `PreviewEnvironment` and applies it. payment-rail
needs the *caller* half: the manifest plus a `pull_request`-triggered workflow.

Two constraints shape everything below. payment-rail **builds no container images** — there
is no Dockerfile anywhere in the repo, and `Makefile:22-23` produces native binaries into
`./bin`. And the callee **creates no namespace**: `spec.messaging.clusterRef` must name a
Redpanda cluster already living in the target namespace. So the honest goal for this round
is a preview that proves *topology* — that the manifest, the CRD schema and the caller/callee
input contract all agree — not a preview that serves traffic.

## Decision

Ship `deploy/paymentstack.yaml` + `.github/workflows/preview-environment.yml` with:

1. **Exactly one app, `api`.** `cmd/ledger` runs in-process under it (ADR-0006); `cmd/signer`
   defaults to loopback; `cmd/chainwatcher`, `cmd/outboxrelay` and `cmd/webhookd` listen on
   nothing. Their absence is deliberate and re-decided when payment-rail publishes images.
2. **A placeholder image**, `ghcr.io/dz3ka/payment-rail/api:0.0.0-unpublished`, in per-binary
   path form so bosun's name-matched `--image` override can later target one app at a time.
3. **One shared namespace**, `payment-rail-previews`, rather than one per PR. Object names are
   already PR-scoped via `preview.Name`.
4. **`ttl-minutes: 480` explicitly**, because bosun computes `expiresAt` from the object's
   creation time and a re-apply on a later push does not extend it — the default 60 would
   delete the preview an hour after the PR's first push, mid-review.
5. **No fork `if:` gate.** The trigger is `pull_request`, not `pull_request_target`, so GitHub
   already withholds the secret from forks. Trigger to add one: the first fork PR.

## Alternatives considered

- **Four apps** (adding the three workers with placeholder ports) — the design's original
  shape, rejected on review. Its rationale, "the topic needs a producer", is false about the
  CRD: `PaymentStackSpec` links `apps` and `messaging.topics` not at all, so the operator
  creates topics regardless of who runs. With no images published, the extra apps buy three
  unschedulable Deployments and three endpoint-less Services.
- **A namespace per PR** — needs a Postgres and a Redpanda provisioned per PR, and the callee
  provisions neither. Not rejected on taste; unavailable.
- **A shared placeholder image tag across apps** — cheaper to write, but bosun's `--image`
  override matches by image *name*, so one override would later rewrite every app at once.
- **`secrets: inherit`** — ships every secret in this repo to a workflow owned by another
  repository. The single `kubeconfig` secret is passed by name instead.

## Consequences

- On a real cluster the preview has **two known reds**: `api` sits in `ImagePullBackOff`, and
  the Topic child stays not-Ready because nothing in either repo provisions the
  `payment-rail` Redpanda cluster. The stack therefore never reaches Ready. Accepted — the
  value this round is contract agreement, not a running system.
- Even with images, a preview would not be *functional*: `PaymentStack` apps carry no env-var
  field, so `api` would use its default `localhost:5432` DSN and never reach the preview's
  database. Closing that is a bosun-side change.
- Concurrent previews **collide** on the broker topic name and on webhookd's consumer group;
  the callee exposes no topic-name override, so this is a property of the contract.
- The template is bounded to **PR numbers ≤ 9999**: bosun derives the Topic's
  `app.kubernetes.io/name` label as `<preview-name>--stack--<topic>`, which reaches the 64-char
  limit at five digits. Shortening `payment-rail.events` would make the template disagree with
  `outbox.DefaultTopic`, so the bound is accepted; the fix belongs in bosun's name budget.
- Verification is **local render only** (bosun's `cmd/preview-manifest`, strict decode) plus a
  caller/callee input-name agreement check. No cluster and no CI secret were exercised, so the
  first real run is still the first real run.
- Follow-ups: an image build/push job here (then `images: api=<sha>` is passed, and both the app
  set and the placeholder tag get revisited); a fork gate at the first fork PR.
