# ADR-0008: Proto toolchain — buf via Go tool directives

- **Status:** Accepted
- **Date:** 2026-07-17
- **Deciders:** Bogdan Dzekic

## Context

M2 slice 1 makes the signer a real gRPC service (ADR-0001), so we now need `.proto` →
Go codegen. The constraint: a bare checkout must generate code with only the Go
toolchain — no system `protoc`, no Homebrew — the go.mod-pinned story we run for sqlc.

## Decision

We decided the proto toolchain is **buf, pinned in `go.mod` and driven by Go `tool`
directives**; `make proto` runs `go tool buf generate`. buf bundles a pure-Go protobuf
compiler, so codegen needs no system `protoc` — mirroring the sqlc-via-Make pattern
(ADR-0002): codegen tools live in go.mod and are invoked through Make.

## Alternatives considered

- **Bare `protoc-gen-go` / `protoc-gen-go-grpc`:** Go plugins, but they can't compile
  `.proto` without a system `protoc` — an unpinnable binary outside go.mod.
- **connect-go:** capable, but swaps gRPC's stub/wire surface for Connect's own,
  obscuring the "gRPC in Go" learning objective this milestone teaches.

## Consequences

- Easier: codegen is reproducible from `go.mod` alone — a new contributor needs only
  the Go toolchain, and CI adds no protoc install step.
- Harder: buf and its plugins are more version pins to track; drop the `tool`
  directives to remove.
