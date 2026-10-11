select trip_id, trip_minutes
from {{ ref('fact_trips') }}
where trip_minutes < 1 or trip_minutes > 300
