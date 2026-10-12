"""Transform NYC taxi trips from Bronze to Silver using AWS Glue."""

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    sha2,
    to_date,
    unix_timestamp,
)

S3_BUCKET = "nm-2026"

FINAL_COLUMNS = [
    "trip_id",
    "vendor_id",
    "pickup_ts",
    "dropoff_ts",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "pu_location_id",
    "do_location_id",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "trip_minutes",
    "pickup_date",
]


def rename_and_cast_trips(df: DataFrame) -> DataFrame:
    """Rename TLC columns and enforce the Silver data types."""

    df = (
        df.withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_ts")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_ts")
        .withColumnRenamed("RatecodeID", "rate_code_id")
        .withColumnRenamed("PULocationID", "pu_location_id")
        .withColumnRenamed("DOLocationID", "do_location_id")
    )

    return df.select(
        col("vendor_id").cast("int"),
        col("pickup_ts").cast("timestamp"),
        col("dropoff_ts").cast("timestamp"),
        col("passenger_count").cast("int"),
        col("trip_distance").cast("double"),
        col("rate_code_id").cast("int"),
        col("pu_location_id").cast("int"),
        col("do_location_id").cast("int"),
        col("payment_type").cast("int"),
        col("fare_amount").cast("double"),
        col("tip_amount").cast("double"),
        col("tolls_amount").cast("double"),
        col("total_amount").cast("double"),
        col("congestion_surcharge").cast("double"),
        col("airport_fee").cast("double"),
    )


def filter_valid_trips(
    df: DataFrame,
    year: str = "2023",
    month: str = "01",
) -> DataFrame:
    """Filter invalid trips and calculate trip duration."""

    df = df.filter(
        col("pickup_ts").isNotNull()
        & col("dropoff_ts").isNotNull()
        & (col("pickup_ts").substr(1, 7) == f"{year}-{month}")
        & (col("dropoff_ts") > col("pickup_ts"))
        & (col("trip_distance") > 0)
        & (col("trip_distance") < 200)
        & (col("total_amount") >= 0)
    )

    df = df.withColumn(
        "trip_minutes",
        (
            unix_timestamp(col("dropoff_ts"))
            - unix_timestamp(col("pickup_ts"))
        )
        / 60.0,
    )

    return df.filter(col("trip_minutes").between(1, 300))


def finalize_trips(df: DataFrame) -> DataFrame:
    """Generate deterministic IDs, deduplicate, and select Silver columns."""

    df = df.withColumn(
        "trip_id",
        sha2(
            concat_ws(
                "||",
                col("vendor_id").cast("string"),
                col("pickup_ts").cast("string"),
                col("dropoff_ts").cast("string"),
                col("pu_location_id").cast("string"),
                col("do_location_id").cast("string"),
                col("total_amount").cast("string"),
            ),
            256,
        ),
    )

    df = df.withColumn("pickup_date", to_date(col("pickup_ts")))
    df = df.dropDuplicates(["trip_id"])

    return df.select(*FINAL_COLUMNS)


def transform_trips(
    df: DataFrame,
    year: str = "2023",
    month: str = "01",
) -> DataFrame:
    """Apply the complete Bronze-to-Silver trip transformation."""

    renamed = rename_and_cast_trips(df)
    valid = filter_valid_trips(renamed, year, month)
    return finalize_trips(valid)


def main() -> None:
    """Run the AWS Glue Bronze-to-Silver job."""

    args = sys.argv
    year = "2023"
    month = "01"

    if "--year" in args:
        year = args[args.index("--year") + 1]

    if "--month" in args:
        month = args[args.index("--month") + 1]

    from awsglue.context import GlueContext

    spark = (
        SparkSession.builder
        .appName("trips_bronze_to_silver")
        .getOrCreate()
    )

    try:
        spark.conf.set(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic",
        )

        GlueContext(spark.sparkContext)

        input_path = (
            f"s3://{S3_BUCKET}/bronze/trips/"
            f"year={year}/month={month}/"
        )
        output_path = f"s3://{S3_BUCKET}/silver/trips/"

        print(f"Reading: {input_path}")
        print(f"Writing: {output_path}")

        raw = spark.read.parquet(input_path)

        input_rows = raw.count()
        in_month_rows = input_rows

        renamed = rename_and_cast_trips(raw)
        valid = filter_valid_trips(renamed, year, month)
        valid_rows = valid.count()

        df = finalize_trips(valid)
        output_rows = df.count()

        print(f"Input rows: {input_rows}")
        print(f"Output rows: {output_rows}")
        print(f"Rows removed: {input_rows - output_rows}")

        (
            df.repartition("pickup_date")
            .write
            .mode("overwrite")
            .partitionBy("pickup_date")
            .parquet(output_path)
        )

        metrics = spark.createDataFrame(
            [
                (
                    f"{year}-{month}",
                    input_rows,
                    in_month_rows,
                    valid_rows,
                    output_rows,
                )
            ],
            """
            run_month string,
            input_rows long,
            in_month_rows long,
            valid_rows long,
            output_rows long
            """,
        )

        metrics.coalesce(1).write.mode("overwrite").json(
            f"s3://{S3_BUCKET}/dq_metrics/trips/"
            f"run_month={year}-{month}/"
        )

        print(
            f"Input rows: {input_rows}, "
            f"In-month rows: {in_month_rows}, "
            f"Valid rows: {valid_rows}, "
            f"Output rows: {output_rows}"
        )
        print("Silver write completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
