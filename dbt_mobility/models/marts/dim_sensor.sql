select
    dbt_scd_id           as sensor_key,
    sensor_id,
    sensor_type,
    zone_id,
    firmware_version,
    dbt_valid_from       as valid_from,
    dbt_valid_to         as valid_to,
    dbt_valid_to is null  as is_current
from {{ ref('sensors_snapshot') }}

