# Requirements & Backlog

Run date: 2026-02-27  
Scope: low-risk, incremental hardening only.

## P0 (Required / Immediate)

### 1) Developer diagnostics target (`make doctor`, `make verify`)
- Status: **Implemented**
- Problem: no single command to print runtime environment + Spark local paths.
- Requirement: deterministic diagnostics command for local supportability.
- Acceptance criteria:
  - `make doctor` prints Python, Java, configured YEAR/MONTH/STRICT, and local Spark path diagnostics.
  - `make verify` runs diagnostics + `make check`.
- Risk: low.
- Effort: `S`.

### 2) Documentation clarity for architecture/runbook/troubleshooting
- Status: **Implemented**
- Problem: key operational details existed but were fragmented and less explicit for demo/review.
- Requirement: concise architecture diagram (text), consistent runbook commands, troubleshooting callouts.
- Acceptance criteria:
  - architecture text diagram included in docs.
  - runbook includes doctor/verify path and troubleshooting SQL syntax.
  - operations doc uses Makefile-centric flow.
- Risk: low.
- Effort: `S`.

### 3) Release-gate validation and evidence
- Status: **Implemented**
- Problem: need explicit evidence that quality gates and pipeline run pass post-hardening.
- Requirement: capture run outputs and assessment in validation report.
- Acceptance criteria:
  - `make check` pass captured.
  - `make reset && make run YEAR=2024 MONTH=1 && make inspect` pass captured.
  - strict smoke + drift sanity tests verified.
- Risk: low.
- Effort: `S`.

## P1 (High Value, Low/Medium Risk)

### 1) Drift baseline model hardening (dataset-typed profile storage)
- Status: **Open**
- Problem: single baseline table schema stores mixed-profile fields across datasets.
- Proposed requirement: split into per-dataset typed baseline tables or add explicit profile schema contract fields.
- Acceptance criteria:
  - baseline schema semantics are explicit per dataset.
  - no ambiguous metric interpretation across Silver/Gold.
- Risk: medium (schema migration and historical compatibility).
- Effort: `M`.

### 2) Drift/quality alert sink interface (noop default)
- Status: **Open**
- Problem: drift/quality failures are persisted but not externally pluggable for alert routing.
- Proposed requirement: add a lightweight sink interface with default no-op adapter.
- Acceptance criteria:
  - interface supports warn/error payload dispatch.
  - default behavior unchanged when no sink configured.
- Risk: low/medium.
- Effort: `M`.

### 3) Contract bump enforcement expansion
- Status: **Partially implemented**
- Current: breaking-change version bump tests exist.
- Proposed requirement: extend tests to include more contract mutation classes (nullability/type/order edge cases).
- Acceptance criteria:
  - CI fails on all designated breaking contract deltas without version bump.
- Risk: low.
- Effort: `S`.

## P2 (Plan Only, Not Implemented)

### 1) dbt serving-layer plan
- Requirement: define ownership boundary between Spark transforms and dbt serving marts.
- Acceptance criteria: architecture decision record + phased migration plan.
- Risk: medium.
- Effort: `M/L`.

### 2) Semantic/dashboard plan
- Requirement: define metric catalog and semantic contracts for BI consumption.
- Acceptance criteria: documented semantic model + sample dashboard specs.
- Risk: medium.
- Effort: `M`.

### 3) Orchestration plan (Airflow)
- Requirement: document DAG decomposition and operational SLAs/retry policies.
- Acceptance criteria: docs-only orchestration blueprint with runbook impacts.
- Risk: medium.
- Effort: `M`.

## Decision Notes

- Pipeline behavior stability and idempotency were prioritized over refactors.
- P1 items are intentionally deferred where schema migration or external integration would add avoidable release risk.
