# ADR-0001: Service topology & repo layout

- **Status:** Accepted — but the gRPC-between-services / separate-ledger-process
  decision is **superseded for M1 by ADR-0006** (api runs the ledger in-process; no
  gRPC on the payments path). The topology stands as the target end state.
- **Date:** 2026-07-16
- **Deciders:** Bogdan Dzekic

## Context

Conduit is a multi-service payment system (api, ledger, signer, chainwatcher,
webhookd) plus an operator CLI. Every arrow in the payment flow is a failure
boundary, and the signer must be isolatable from the rest of the system. We need
a repository structure and inter-service transport that make those boundaries
explicit without drowning a solo/learning project in operational overhead.

## Decision

- **Single Go module, monorepo**, standard `cmd/` + `internal/` layout: one
  directory per binary under `cmd/`, all private code under `internal/`.
- **Separate binaries** (not one process with flags): each service is its own
  `main`, so it can be scheduled, scaled, and network-isolated independently —
  the signer especially.
- **gRPC for internal** service-to-service transport, **REST for the external**
  API. gRPC gives typed contracts and streaming for internal calls; REST keeps
  the public API accessible to any HTTP client.
- A shared `internal/service` bootstrap keeps each `main` a thin entrypoint
  (config + structured logging + signal-driven graceful shutdown).

## Alternatives considered

- **Multi-module / multi-repo (one repo per service):** matches production
  team boundaries but adds cross-repo versioning friction with no benefit at
  this scale; the monorepo keeps a single CI and atomic cross-service changes.
- **Single binary, sub-commands:** simplest to run, but it collapses the trust
  boundary — the signer could not be network-isolated, defeating a core goal.
- **REST everywhere (no gRPC):** fewer moving parts, but loses typed internal
  contracts and makes streaming (chainwatcher → ledger) awkward.

## Consequences

- Easier: independent deploys, clear ports-and-adapters seams, atomic refactors.
- Harder: must run several processes locally (mitigated by docker-compose and
  `make` targets) and define gRPC contracts before wiring services (M1+).
- Follow-up: protobuf/gRPC scaffolding and a `proto/` layout land in M1–M2.
