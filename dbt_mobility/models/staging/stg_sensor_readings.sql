select
    event_id,
    sensor_id,
    sensor_type,
    value,
    event_ts,
    date_trunc('hour', event_ts) as reading_hour,
    sim_label,
    reading_date
from {{ source('silver', 'sensor_readings') }}
