# ADR-0002: Contract-Driven Schema Governance

- Status: Accepted
- Date: 2026-02-26

## Context

Schema drift and implicit evolution increase rerun risk and make data quality outcomes non-deterministic.

## Decision

- Contracts are the source of truth for Bronze/Silver/Gold schemas.
- Runtime enforcement validates schema and expectation rules.
- CI blocks unversioned breaking contract changes.
- Implicit schema evolution is disabled (`mergeSchema=false`, Delta `autoMerge=false`).

## Consequences

- Breaking schema changes require explicit contract version bumps.
- Pipeline behavior is deterministic across reruns.
- Governance is visible both at runtime and in CI.
