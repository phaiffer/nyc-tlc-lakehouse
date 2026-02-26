# ADR-0001: Embedded Hive Metastore Constraints and Mitigation

- Status: Accepted
- Date: 2026-02-26

## Context

The project runs locally with Spark + Delta + embedded Hive metastore (Derby). This topology is
convenient for portfolio/local reproducibility but can surface schema-alter incompatibilities during
managed Delta overwrites.

## Decision

- Keep embedded Hive metastore for local-first DX.
- Accept known informational Hive SerDe warnings for Delta tables.
- Mitigate schema drift with deterministic casting and controlled drop/recreate logic for managed
  tables when schema conflict is detected.

## Consequences

- Reruns are stable without manual metastore cleanup in most cases.
- Local behavior remains predictable and close to managed-catalog workflows.
- Some local warnings remain expected and documented.
