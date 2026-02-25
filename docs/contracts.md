# Data Contracts and Governance

## Contract Locations

- Bronze: `contracts/bronze/events_raw.yml`
- Silver: `contracts/silver/trips_clean.yml`
- Gold: `contracts/gold/fct_trips_daily.yml`

## Contract Fields

Each contract defines:

- `dataset`: canonical table name (`<layer>.<table>`)
- `owner`: owning team
- `version`: integer version
- `schema`: column list with `name`, `type`, `nullable`
- `primary_key` (optional for bronze; required for silver/gold)
- `watermark` and `late_arrival_days` for incremental windows
- `expectations`: rule set (`not_null`, `range`, `unique`)

## Runtime Enforcement

Silver and Gold use runtime contract enforcement:

- schema and type checks
- expectation checks
- PK uniqueness checks
- invalid-ratio gating
- quarantine writes for invalid rows

## CI Enforcement

CI gates run:

- `python ci/scripts/validate_contracts.py`
  - JSON Schema validation
  - semantic checks (PK, watermark consistency, layer conventions)
- `python ci/scripts/detect_contract_breaking_changes.py`
  - compares current contracts vs baseline ref
  - blocks breaking changes without version bump

## Version Bump Policy

Version bump is required when introducing breaking changes, including:

- removing a column
- changing column type
- tightening nullability (`true -> false`)
- changing primary key
- removing/changing watermark when baseline has one

Non-breaking changes (for example adding new nullable columns) can be released without bump, but bumping is still recommended for traceability.
