{{ config(
    materialized='view'
) }}

select
    event_id,
    sensor_id,
    sensor_type,
    value,
    event_ts,
    sim_label,
    reading_date
from {{ source('mobility_silver', 'sensor_readings') }}
where event_id is not null
  and sensor_id is not null
