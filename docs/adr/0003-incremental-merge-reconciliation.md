# ADR-0003: Incremental Merge Strategy and Reconciliation

- Status: Accepted
- Date: 2026-02-26

## Context

The platform must support repeatable monthly reruns and late-arriving data without full-table rebuilds.

## Decision

- Use watermark-based incremental merge for Silver and Gold.
- Apply bounded backfill (`late_arrival_days`) for late arrivals.
- Deterministically deduplicate source rows by primary key, watermark, and stable tie-break hash.
- Run reconciliation checks and write auditable metrics for each run.

## Consequences

- Same-window reruns remain idempotent.
- Late arrivals are absorbed without unmanaged full refreshes.
- Merge outcomes are explainable and observable.
