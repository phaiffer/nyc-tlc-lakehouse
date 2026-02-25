# Incremental Processing Design

## Objective

Ensure deterministic, idempotent upserts for Silver and Gold Delta tables while handling late-arriving data safely.

## Merge Strategy

`pipelines/common/delta_incremental_merge.py` is used for both Silver and Gold.

Behavior:

1. Read max watermark from target table.
2. Compute lower bound using `late_arrival_days`.
3. Filter source records to the incremental window.
4. Deterministically deduplicate source by:
   - partition: primary key
   - order: watermark desc, stable hash desc
5. Merge (Delta API when available, SQL fallback otherwise).

## Watermark and Late Arrival

- Silver contract:
  - `watermark: updated_at`
  - `late_arrival_days: 7`
- Gold contract:
  - `watermark: trip_date`
  - `late_arrival_days: 7`

This provides a bounded backfill window to absorb delayed records without rebuilding full history.

## Deterministic Dedup Rationale

When multiple source rows share the same PK:

- Most recent watermark wins.
- If watermark ties, a stable row hash tie-breaker is applied.

This removes nondeterministic merge ordering and keeps reruns reproducible.

## Month-Window Runs

`--year` and `--month` apply a deterministic month window at runtime:

- Bronze: filters source parquet by pickup timestamp.
- Silver: filters canonical rows by `pickup_date`.
- Gold: filters source Silver rows by `pickup_date`; reconciliation checks day coverage.

## Operational Notes

- Rerunning the same command with same inputs is safe.
- Use `--reset` for explicit clean stage reruns.
- Quality and observability artifacts are written to Delta tables for auditability.
