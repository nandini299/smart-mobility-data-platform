"""Unit tests for the production trip Bronze-to-Silver transformation."""

import pytest
from pyspark.sql import SparkSession

from glue.scripts.trips_bronze_to_silver import transform_trips


@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for the test suite."""

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("glue-trips-unit-tests")
        .getOrCreate()
    )

    yield session
    session.stop()


@pytest.fixture
def trip_schema():
    """Return the TLC source schema used by the tests."""

    return """
        VendorID string,
        tpep_pickup_datetime string,
        tpep_dropoff_datetime string,
        passenger_count string,
        trip_distance string,
        RatecodeID string,
        PULocationID string,
        DOLocationID string,
        payment_type string,
        fare_amount string,
        tip_amount string,
        tolls_amount string,
        total_amount string,
        congestion_surcharge string,
        airport_fee string
    """


def valid_trip():
    """Return one valid January 2023 TLC trip."""

    return (
        "1",
        "2023-01-10 10:00:00",
        "2023-01-10 10:20:00",
        "1",
        "2.5",
        "1",
        "100",
        "200",
        "1",
        "15.0",
        "2.0",
        "0.0",
        "20.0",
        "2.5",
        "0.0",
    )


def test_valid_trips_are_cast_and_deduplicated(spark, trip_schema):
    """Keep one valid trip and verify its transformed fields."""

    valid = valid_trip()

    zero_distance = (
        "1",
        "2023-01-10 11:00:00",
        "2023-01-10 11:20:00",
        "1",
        "0",
        "1",
        "100",
        "200",
        "1",
        "15.0",
        "2.0",
        "0.0",
        "20.0",
        "2.5",
        "0.0",
    )

    february_trip = (
        "1",
        "2023-02-10 10:00:00",
        "2023-02-10 10:20:00",
        "1",
        "2.5",
        "1",
        "100",
        "200",
        "1",
        "15.0",
        "2.0",
        "0.0",
        "20.0",
        "2.5",
        "0.0",
    )

    source = spark.createDataFrame(
        [valid, valid, zero_distance, february_trip],
        trip_schema,
    )

    result = transform_trips(source, year="2023", month="01")

    assert result.count() == 1

    trip = result.first()
    assert trip.trip_minutes == 20.0
    assert len(trip.trip_id) == 64
    assert trip.pickup_date.isoformat() == "2023-01-10"
    assert trip.vendor_id == 1
    assert trip.trip_distance == 2.5
    assert trip.trip_id is not None


def test_trips_longer_than_five_hours_are_removed(spark, trip_schema):
    """Reject trips whose duration exceeds 300 minutes."""

    long_trip = (
        "1",
        "2023-01-10 10:00:00",
        "2023-01-10 16:00:00",
        "1",
        "10",
        "1",
        "100",
        "200",
        "1",
        "50.0",
        "0.0",
        "0.0",
        "50.0",
        "0.0",
        "0.0",
    )

    source = spark.createDataFrame([long_trip], trip_schema)
    result = transform_trips(source)

    assert result.count() == 0


def test_invalid_pickup_timestamp_is_removed(spark, trip_schema):
    """Reject trips with an invalid pickup timestamp."""

    invalid_trip = (
        "1",
        "not-a-timestamp",
        "2023-01-10 10:20:00",
        "1",
        "2.5",
        "1",
        "100",
        "200",
        "1",
        "15.0",
        "2.0",
        "0.0",
        "20.0",
        "2.5",
        "0.0",
    )

    source = spark.createDataFrame([invalid_trip], trip_schema)
    result = transform_trips(source)

    assert result.count() == 0
