# Semantic Model

## Fact Table

### `gold.fct_trips_daily`

Grain:

- one row per (`trip_date`, `vendor_id`)

Measures:

- `trips` (bigint)
- `total_fare` (decimal(18,2))

## Dimensions

### `gold.dim_vendor`

- key: `vendor_id`
- attributes: `vendor_name`, `is_known_vendor`

### `gold.dim_payment_type`

- key: `payment_type_id`
- attribute: `payment_type_name`

### `gold.dim_rate_code`

- key: `rate_code_id`
- attribute: `rate_code_name`

## Optional Dimension: Location

`dim_location` is not generated in this repo by default because taxi zone lookup input is not
bundled.

To add it:

1. Load `taxi_zone_lookup.csv` into Bronze/reference storage.
2. Build `gold.dim_location` with location identifiers and zone metadata.
3. Add location keys to Silver/Gold contracts and pipeline transformations.

## Query Examples

See:

- `analytics/examples.sql`
