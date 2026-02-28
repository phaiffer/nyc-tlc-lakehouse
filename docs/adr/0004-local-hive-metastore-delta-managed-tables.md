# ADR-0004: Local Hive Metastore With Delta Managed Tables

- Status: Accepted
- Date: 2026-02-28

## Context

The repository is intentionally local-first for deterministic execution in a portfolio setting. Spark stores managed Delta tables in a local warehouse while Hive-compatible metadata is persisted through embedded Derby metastore state.

## Decision

- Keep local embedded Hive metastore + managed Delta tables as the default execution topology.
- Treat Hive SerDe warnings for Delta provider mapping as expected local behavior, not pipeline failure.
- Standardize default runtime paths under `.local/` for warehouse, metastore, and Spark local state.
- Keep cleanup deterministic through explicit reset and filesystem cleanup commands.

## Consequences

- Contributors can run the full pipeline without external infrastructure.
- Logs include known warnings that are now documented as expected signals.
- Local runtime artifacts are isolated and should remain untracked.
