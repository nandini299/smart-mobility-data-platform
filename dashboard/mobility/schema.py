GOLD_SCHEMA = """
You have access to the following Athena Gold tables.

DATABASE:
mobility_gold

TABLE: fact_trips
Columns:
- trip_id: string
- date_key: int
- pickup_ts: timestamp
- dropoff_ts: timestamp
- pickup_zone_id: int
- dropoff_zone_id: int
- payment_type_id: int
- passenger_count: int
- trip_distance: double
- trip_minutes: double
- fare_amount: double
- tip_amount: double
- tolls_amount: double
- total_amount: double
- pickup_month: string

TABLE: dim_date
Columns:
- date_key: int
- date_day: date
- year: bigint
- quarter: bigint
- month: bigint
- day_of_month: bigint
- day_of_week: bigint
- day_name: string
- is_weekend: boolean

TABLE: dim_payment_type
Columns:
- payment_type_id: int
- payment_type_name: varchar

TABLE: dim_zone
Columns:
- location_id: int
- borough: string
- zone_name: string
- service_zone: string

RELATIONSHIPS:

fact_trips.date_key = dim_date.date_key

fact_trips.payment_type_id = dim_payment_type.payment_type_id

fact_trips.pickup_zone_id = dim_zone.location_id

fact_trips.dropoff_zone_id = dim_zone.location_id

IMPORTANT:
- Use pickup_zone_id when analyzing pickup locations.
- Use dropoff_zone_id when analyzing dropoff locations.
- Only use the tables and columns listed above.
"""
















SENSOR_SCHEMA = """
You have access to the following Athena sensor analytics tables.

DATABASE:
mobility_gold

TABLE: fact_sensor_hourly
Purpose: Contains aggregated sensor readings at an hourly grain.

Columns:
- sensor_key: string
- sensor_id: string
- zone_id: int
- sensor_type: string
- reading_hour: timestamp
- reading_count: bigint
- avg_value: double
- min_value: double
- max_value: double
- stddev_value: double
- reading_date: date

TABLE: dim_sensor
Purpose: Contains sensor metadata and historical versions.

Columns:
- sensor_key: string
- sensor_id: string
- sensor_type: string
- zone_id: int
- firmware_version: string
- valid_from: timestamp
- valid_to: timestamp
- is_current: boolean

IMPORTANT:
- Use fact_sensor_hourly for hourly sensor statistics.
- Use dim_sensor for sensor metadata and firmware history.
- Use reading_date for date-based filtering.
- Use reading_hour for hourly trend analysis.
- Use avg_value for the average measured value.
- Use reading_count for the number of readings in an hourly aggregate.
- Use is_current = TRUE to find current sensor metadata.
- Only use the tables and columns listed above.
- Do not query taxi tables for sensor-related questions.
"""
