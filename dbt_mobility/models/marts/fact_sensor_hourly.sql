{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['reading_date'],
    format='parquet'
) }}

-- Grain: one row per sensor per hour.
with hourly as (

    select
        sensor_id,
        sensor_type,
        reading_hour,
        reading_date,
        count(*) as reading_count,
        avg(value) as avg_value,
        min(value) as min_value,
        max(value) as max_value,
        stddev_samp(value) as stddev_value

    from {{ ref('stg_sensor_readings') }}

    {% if is_incremental() and var('run_date', none) %}
        where reading_date = date '{{ var("run_date") }}'
    {% endif %}

    group by
        sensor_id,
        sensor_type,
        reading_hour,
        reading_date
)

select
    s.sensor_key,
    h.sensor_id,
    s.zone_id,
    h.sensor_type,
    h.reading_hour,
    h.reading_count,
    round(h.avg_value, 2) as avg_value,
    h.min_value,
    h.max_value,
    round(h.stddev_value, 2) as stddev_value,
    h.reading_date

from hourly h

left join {{ ref('dim_sensor') }} s
    on h.sensor_id = s.sensor_id
    and h.reading_hour >= s.valid_from
    and (
        h.reading_hour < s.valid_to
        or s.valid_to is null
    )
