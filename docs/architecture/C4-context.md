# C4 — Level 1: System Context

How Payment Rail sits between the people/systems that use it and the external rails
it depends on. (C4 model: https://c4model.com)

```mermaid
C4Context
    title System Context — Payment Rail

    Person(platformEng, "Platform engineer", "Integrates payouts into their product via REST + events")
    Person(opsUser, "Finance / ops operator", "Lists payments, inspects ledger, runs reconciliation")

    System(paymentrail, "Payment Rail", "Stablecoin payment orchestration rail: idempotent API, double-entry ledger, reorg-safe settlement")

    System_Ext(evm, "EVM chains", "Base Sepolia + Ethereum Sepolia (ERC-20 USDC settlement)")
    System_Ext(screening, "Screening provider", "Address denylist / sanctions (pluggable; mock in v1)")
    System_Ext(downstream, "Downstream systems", "Consume domain events via webhooks / Kafka")

    Rel(platformEng, paymentrail, "Creates & tracks payments", "REST / HTTPS")
    Rel(opsUser, paymentrail, "Operates & reconciles", "paymentrailctl / REST")
    Rel(paymentrail, evm, "Submits transfers, tracks confirmations", "JSON-RPC")
    Rel(paymentrail, screening, "Screens counterparties", "gRPC / HTTP")
    Rel(paymentrail, downstream, "Emits payment.* events", "Webhooks / Kafka")
```

## Notes

- The chain is a **settlement rail**, not the database. Internal truth lives in
  the double-entry ledger; the chain is reconciled against it.
- Screening and chain access are **ports** with adapters, so providers/chains
  can be swapped (Solana is designed-for but not built in v1).
