"""Unit tests for the guardrail applied to LLM-generated SQL."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "dashboard" / "mobility"))

from sql_validator import validate_sql

VALID_QUERIES = [
    """
    SELECT d.month, SUM(f.total_amount) AS total_revenue
    FROM mobility_gold.fact_trips f
    JOIN mobility_gold.dim_date d ON f.date_key = d.date_key
    WHERE d.year = 2023
    GROUP BY d.month
    ORDER BY d.month;
    """,
    """
    WITH busy AS (
        SELECT pickup_zone_id, COUNT(*) AS trips
        FROM mobility_gold.fact_trips
        GROUP BY pickup_zone_id
    )
    SELECT z.zone_name, b.trips
    FROM busy b
    JOIN mobility_gold.dim_zone z ON b.pickup_zone_id = z.zone_id
    """,
    "SELECT * FROM awsdatacatalog.mobility_gold.dim_sensor WHERE is_current",
    "SELECT 'please update me' AS note FROM mobility_gold.dim_zone",
]

INVALID_QUERIES = [
    "",
    "DROP TABLE mobility_gold.fact_trips",
    "DELETE FROM mobility_gold.fact_trips",
    "INSERT INTO mobility_gold.dim_zone SELECT * FROM mobility_gold.dim_zone",
    "SELECT 1; DROP TABLE mobility_gold.fact_trips",
    "SELECT * FROM mobility_gold.fact_trips -- sneaky",
    "SELECT * FROM mobility_silver.trips",
    "SELECT * FROM information_schema.tables",
    # Comma join used to bypass the old FROM/JOIN regex check.
    "SELECT * FROM mobility_gold.dim_zone, mobility_silver.trips",
    (
        "SELECT * FROM mobility_gold.dim_zone WHERE zone_id IN "
        "(SELECT pu_location_id FROM mobility_silver.trips)"
    ),
    "SELECT * FROM other_catalog.mobility_gold.fact_trips",
    "SELECT * FROM fact_trips",
    "SELECT 1",
]


@pytest.mark.parametrize("sql", VALID_QUERIES)
def test_allowed_queries_pass(sql):
    valid, message = validate_sql(sql)
    assert valid, message


@pytest.mark.parametrize("sql", INVALID_QUERIES)
def test_disallowed_queries_are_rejected(sql):
    valid, _ = validate_sql(sql)
    assert not valid
