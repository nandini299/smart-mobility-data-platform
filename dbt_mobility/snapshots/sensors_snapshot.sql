{% snapshot sensors_snapshot %}

{{
    config(
        target_schema='mobility_gold',
        unique_key='sensor_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select
    sensor_id,
    sensor_type,
    zone_id,
    firmware_version,
    updated_at
from {{ ref('sensors') }}

{% endsnapshot %}
