{{ config(materialized='view') }}

with source_data as (
    select *
    from {{ source('silver', 'trips_clean') }}
)

select
    {{ standardize_trip_id('trip_id') }} as trip_id,
    {{ standardize_vendor_id('vendor_id') }} as vendor_id,
    {{ safe_cast('pickup_ts', 'timestamp') }} as pickup_ts,
    {{ safe_cast('dropoff_ts', 'timestamp') }} as dropoff_ts,
    {{ safe_cast('pickup_date', 'date') }} as pickup_date,
    {{ safe_cast('trip_distance', 'double') }} as trip_distance,
    {{ safe_cast('fare_amount', 'decimal(18,2)') }} as fare_amount,
    {{ safe_cast('passenger_count', 'bigint') }} as passenger_count,
    {{ safe_cast('updated_at', 'timestamp') }} as updated_at
from source_data
