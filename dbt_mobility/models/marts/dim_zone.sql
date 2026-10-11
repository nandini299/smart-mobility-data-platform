select
    location_id as zone_id,
    borough,
    zone_name,
    service_zone
from {{ ref('taxi_zone_lookup') }}
