select
    trip_id,
    vendor_id,
    pickup_ts,
    dropoff_ts,
    pickup_date,
    passenger_count,
    trip_distance,
    trip_minutes,
    rate_code_id,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount
from {{ source('silver', 'trips') }}
