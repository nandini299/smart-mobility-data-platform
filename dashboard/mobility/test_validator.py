from sql_validator import validate_sql

queries = [
    """
    SELECT
        d.month,
        SUM(f.total_amount) AS total_revenue
    FROM mobility_gold.fact_trips f
    JOIN mobility_gold.dim_date d
        ON f.date_key = d.date_key
    WHERE d.year = 2025
    GROUP BY d.month
    ORDER BY d.month
    """,

    "DROP TABLE mobility_gold.fact_trips",

    "DELETE FROM mobility_gold.fact_trips"
]


for query in queries:
    valid, message = validate_sql(query)

    print("Query:")
    print(query.strip())

    print("Result:", valid)
    print("Message:", message)
    print("-" * 50)
