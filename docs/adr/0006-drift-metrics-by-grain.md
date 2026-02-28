# ADR-0006: Drift Metrics by Grain (Silver vs Gold)

- Status: Accepted
- Date: 2026-02-28

## Context

Silver and Gold datasets have different semantic grains. Reusing identical drift metrics across both layers can produce misleading observability results, especially when Gold contains pre-aggregated measures.

## Decision

- Evaluate drift with grain-specific metric sets.
- Silver keeps row-level profile metrics (volume, average fare, trip distance, passenger distribution).
- Gold keeps aggregate-compatible metrics (volume and fare-per-trip ratio derived from `SUM(total_fare) / SUM(trips)`).
- Maintain metric names and thresholds as configuration-driven controls.

## Consequences

- Drift alerts align with dataset semantics.
- Gold observability avoids non-applicable metrics that generate noise.
- Analysts can interpret Silver and Gold drift trends without cross-grain ambiguity.
