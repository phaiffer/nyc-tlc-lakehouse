# Enterprise Improvements Backlog

This backlog captures deeper post-PR improvements beyond the current hardening scope.

## P0

### 1) Data Contract Runtime Enforcement Hardening
- Problem statement: Contract checks are enforced in pipeline code, but cross-dataset contract compatibility and contract evolution workflows can still drift as the platform grows.
- Proposed approach: Introduce explicit contract validation stages per layer (Bronze/Silver/Gold), plus automated compatibility checks for downstream dependencies in CI.
- Expected impact: Higher reliability and maintainability through earlier drift detection.
- Effort estimate: `M`
- Acceptance criteria: Every contract change is validated against downstream model expectations, and CI blocks incompatible changes without version bumps.

### 2) Structured Observability + Trace Correlation
- Status: **Implemented (2026-02-28)**
- Problem statement: Current logs and metrics are useful but not consistently structured for fleet-scale operations and run-to-run traceability.
- Proposed approach: Add structured JSON logging with stable `run_id`, stage, severity, and table fields; emit OpenTelemetry-compatible spans around Bronze/Silver/Gold/Quality stages.
- Expected impact: Better operational reliability and debugging speed.
- Effort estimate: `M`
- Acceptance criteria: Each pipeline stage emits structured logs and trace metadata that can be correlated to `quality.pipeline_metrics`.

### 3) Incremental Backfill Orchestration
- Status: **Implemented (2026-02-28)**
- Problem statement: Backfills across multiple months currently require manual repetition and can introduce operational inconsistency.
- Proposed approach: Add a backfill command for parameterized month ranges with checkpointed progress and resumability.
- Expected impact: Better reliability and operator productivity.
- Effort estimate: `M`
- Acceptance criteria: A single command can process a month range idempotently with restart-safe checkpoints and clear per-window status.

## P1

### 4) Gold Modeling Layer with dbt
- Problem statement: Gold logic is implemented in PySpark only, reducing analytics-model portability and governance options.
- Proposed approach: Introduce a dbt layer for serving Gold marts while preserving the existing Spark pipeline as the raw-to-clean foundation.
- Expected impact: Better maintainability and analytics governance.
- Effort estimate: `M`
- Acceptance criteria: Gold marts are reproducible in dbt with tests and documentation generated for serving models.

### 5) Great Expectations Integration for Quality Rule Packs
- Problem statement: Quality gate logic is custom and tightly coupled to current rules; reusable expectation suites are limited.
- Proposed approach: Integrate Great Expectations suites for selected Silver/Gold checks and keep custom checks for domain-specific logic.
- Expected impact: Better maintainability and test coverage consistency.
- Effort estimate: `M`
- Acceptance criteria: Core quality checks run via expectation suites in CI/local runs with stable result artifacts.

### 6) Delta Layout Optimization Strategy
- Problem statement: Local Delta tables are functionally correct but lack a documented optimization strategy for scaling datasets.
- Proposed approach: Define and automate partitioning, compaction, and retention policies compatible with local Delta workflows.
- Expected impact: Better performance and storage efficiency.
- Effort estimate: `S`
- Acceptance criteria: Documented optimization playbook exists and can be executed with deterministic maintenance commands.

## P2

### 7) CI Matrix by Spark/Python Version
- Problem statement: CI currently validates a single runtime, which can hide compatibility regressions.
- Proposed approach: Add a lean matrix covering selected Spark/Python combinations relevant to the project support policy.
- Expected impact: Higher reliability across environments.
- Effort estimate: `M`
- Acceptance criteria: CI matrix runs stable checks for at least two supported runtime combinations with deterministic outcomes.

### 8) Documentation Site Generation (MkDocs)
- Problem statement: Docs are present but navigation and discoverability are limited for portfolio/enterprise audiences.
- Proposed approach: Generate a docs site from `docs/` with architecture, operations, ADRs, and runbook navigation.
- Expected impact: Better maintainability and onboarding.
- Effort estimate: `S`
- Acceptance criteria: Documentation site builds in CI and publishes a navigable versioned artifact.

### 9) Release Process and Changelog Automation
- Problem statement: Release metadata and change visibility are manual.
- Proposed approach: Adopt semantic versioning tags + changelog automation tied to Conventional Commits.
- Expected impact: Better maintainability and portfolio professionalism.
- Effort estimate: `S`
- Acceptance criteria: Tagged releases produce an automated changelog with categorized changes and reproducible release notes.
