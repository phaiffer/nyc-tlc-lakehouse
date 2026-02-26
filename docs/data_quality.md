# Data Quality

## Contract Enforcement

Silver and Gold are enforced against YAML contracts:

- schema presence and strict data types
- expectation checks (`not_null`, `range`, `unique`)
- primary key uniqueness checks
- invalid-ratio threshold gate

Invalid rows are written to `quality.quarantine_records`.

## Severity Model

Rule severity values:

- `error`
- `warn`
- `info`

Gate behavior:

- strict mode fails only on `error` rule failures
- `warn` and `info` are recorded but do not fail strict mode

## Violations Summary Schema

`quality.violations_summary` columns:

- `dataset`, `run_id`, `run_ts`
- `window_year`, `window_month`, `contract_version`
- `rule_id`, `rule_name`, `severity`
- `passed`, `failed_count`, `sample_values`
- `threshold`, `details_json`

## Quarantine Schema

`quality.quarantine_records` includes:

- source row payload columns
- `reason_code`, `rule_id`, `rule_name`, `severity`
- `dataset`, `contract_version`, `run_id`, `run_ts`

## Drift Detection

Drift events are written to `quality.drift_events` using thresholds from:

- `config/drift_thresholds.yml`

Profiles are tracked in:

- `quality.drift_baseline_metrics`

Metrics monitored:

- row volume
- average fare amount
- average trip distance
- passenger-count distribution drift (L1 distance)
