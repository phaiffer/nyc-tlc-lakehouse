# ADR-0005: Contracts and Quality Gate Enforcement

- Status: Accepted
- Date: 2026-02-28

## Context

Portfolio-quality lakehouse repos need explicit data governance. Schema drift and unchecked expectation failures reduce trust in metrics and make reruns non-deterministic.

## Decision

- Keep YAML contracts as the schema source of truth for Bronze, Silver, and Gold datasets.
- Enforce contracts at runtime with quarantine capture for invalid records.
- Keep severity-aware quality gates (`error`, `warn`, `info`) and support strict mode for failing runs on `error` outcomes.
- Keep CI breaking-change detection that requires contract version bumps for incompatible changes.

## Consequences

- Governance controls are visible in both local runs and CI.
- Data quality behavior remains auditable through violation and metrics tables.
- Breaking schema changes become explicit project decisions instead of silent runtime side effects.
