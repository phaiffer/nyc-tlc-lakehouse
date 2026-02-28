{{ config(materialized='table') }}

select
    concat(cast(pickup_date as string), '_', vendor_id) as daily_vendor_key,
    pickup_date as trip_date,
    vendor_id,
    count(*) as trips,
    cast(sum(fare_amount) as decimal(18,2)) as total_fare,
    cast(sum(fare_amount) / nullif(count(*), 0) as decimal(18,2)) as avg_fare_per_trip
from {{ ref('stg_silver_trips_clean') }}
where vendor_id is not null
group by pickup_date, vendor_id
