{{ config(materialized='view') }}

with daily as (
    select *
    from {{ ref('mart_daily_revenue') }}
)

select
    vendor_id,
    case
        when vendor_id in ('1', '2') then 'known_tlc_vendor'
        else 'other_vendor'
    end as vendor_tier,
    count(distinct trip_date) as active_days,
    sum(trips) as total_trips,
    cast(sum(total_fare) as decimal(18,2)) as total_fare
from daily
group by vendor_id
