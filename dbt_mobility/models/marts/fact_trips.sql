{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['pickup_month'],
    format='parquet'
) }}

-- Grain: one row per trip.
-- Incremental runs rebuild only the month passed in var('run_month'),
-- so re-running a month is idempotent.
select
    trip_id,
    cast(date_format(pickup_date, '%Y%m%d') as int) as date_key,
    pickup_ts,
    dropoff_ts,
    pu_location_id  as pickup_zone_id,
    do_location_id  as dropoff_zone_id,
    payment_type    as payment_type_id,
    passenger_count,
    trip_distance,
    trip_minutes,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    date_format(pickup_date, '%Y-%m') as pickup_month   -- partition column must be LAST
from {{ ref('stg_trips') }}
{% if is_incremental() and var('run_month', none) %}
where date_format(pickup_date, '%Y-%m') = '{{ var("run_month") }}'
{% endif %}
