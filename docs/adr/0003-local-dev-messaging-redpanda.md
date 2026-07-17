# ADR-0003: Local dev messaging (Redpanda over Kafka)

- **Status:** Accepted
- **Date:** 2026-07-16
- **Deciders:** Bogdan Dzekic

## Context

Payment Rail emits domain events through a transactional outbox onto a Kafka-API bus.
For local development and CI we need a broker that is Kafka-wire-compatible but
light enough to run on a laptop-class machine alongside Postgres and an OTel
collector. A full Kafka + ZooKeeper/KRaft cluster is heavy for that purpose.

## Decision

Run **Redpanda** as the dev/CI broker via docker-compose. It speaks the Kafka
protocol, so application code and client libraries target "Kafka" and remain
portable to real Kafka in production. Nothing in the codebase depends on
Redpanda-specific features.

## Alternatives considered

- **Apache Kafka + ZooKeeper/KRaft:** the production reference, but a multi-GB,
  multi-container footprint that slows `make up` and CI for no dev-time gain.
- **NATS / RabbitMQ:** lighter, but not Kafka-wire-compatible, which would fork
  dev and prod client code and undermine the outbox→Kafka design.

## Consequences

- Easier: `make up` stays fast; a single container provides the event bus;
  identical client code in dev and prod.
- Harder: Redpanda and Kafka can diverge at the edges — production readiness
  requires validating against real Kafka before any non-testnet use.
- Constraint: application code MUST NOT use Redpanda-only APIs; treat it strictly
  as a Kafka endpoint.
