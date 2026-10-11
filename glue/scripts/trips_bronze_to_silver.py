import sys

from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    sha2,
    to_date,
    unix_timestamp,
)

# --------------------------------------------------
# Arguments
# --------------------------------------------------

args = sys.argv

year = "2023"
month = "01"

if "--year" in args:
    year = args[args.index("--year") + 1]

if "--month" in args:
    month = args[args.index("--month") + 1]


S3_BUCKET = "nm-2026"


# --------------------------------------------------
# Spark / Glue setup
# --------------------------------------------------

spark = SparkSession.builder \
    .appName("trips_bronze_to_silver") \
    .getOrCreate()


spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)


glue_context = GlueContext(spark.sparkContext)


# --------------------------------------------------
# Paths
# --------------------------------------------------

input_path = (
    f"s3://{S3_BUCKET}/bronze/trips/"
    f"year={year}/month={month}/"
)

output_path = f"s3://{S3_BUCKET}/silver/trips/"


print(f"Reading: {input_path}")
print(f"Writing: {output_path}")


# --------------------------------------------------
# Read Bronze
# --------------------------------------------------

df = spark.read.parquet(input_path)

input_rows = df.count()
in_month_rows = df.count()
print(f"Input rows: {input_rows}")


# --------------------------------------------------
# Rename columns
# --------------------------------------------------

df = df.withColumnRenamed(
    "VendorID",
    "vendor_id"
).withColumnRenamed(
    "tpep_pickup_datetime",
    "pickup_ts"
).withColumnRenamed(
    "tpep_dropoff_datetime",
    "dropoff_ts"
).withColumnRenamed(
    "passenger_count",
    "passenger_count"
).withColumnRenamed(
    "trip_distance",
    "trip_distance"
).withColumnRenamed(
    "RatecodeID",
    "rate_code_id"
).withColumnRenamed(
    "PULocationID",
    "pu_location_id"
).withColumnRenamed(
    "DOLocationID",
    "do_location_id"
).withColumnRenamed(
    "payment_type",
    "payment_type"
).withColumnRenamed(
    "fare_amount",
    "fare_amount"
).withColumnRenamed(
    "tip_amount",
    "tip_amount"
).withColumnRenamed(
    "tolls_amount",
    "tolls_amount"
).withColumnRenamed(
    "total_amount",
    "total_amount"
).withColumnRenamed(
    "congestion_surcharge",
    "congestion_surcharge"
).withColumnRenamed(
    "airport_fee",
    "airport_fee"
)


# --------------------------------------------------
# Explicit data types
# --------------------------------------------------

df = df.select(
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


# --------------------------------------------------
# Data quality filtering
# --------------------------------------------------

df = df.filter(
    col("pickup_ts").isNotNull()
    & col("dropoff_ts").isNotNull()
    & (col("pickup_ts").substr(1, 7) == f"{year}-{month}")
    & (col("dropoff_ts") > col("pickup_ts"))
    & (col("trip_distance") > 0)
    & (col("trip_distance") < 200)
    & (col("total_amount") >= 0)
)


# --------------------------------------------------
# Trip duration
# --------------------------------------------------

df = df.withColumn(
    "trip_minutes",
    (unix_timestamp(col("dropoff_ts")) - unix_timestamp(col("pickup_ts"))) / 60.0,
)

df = df.filter(col("trip_minutes").between(1, 300))
valid_rows = df.count()
# --------------------------------------------------
# Deterministic trip ID
# --------------------------------------------------

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


# --------------------------------------------------
# Pickup partition
# --------------------------------------------------

df = df.withColumn(
    "pickup_date",
    to_date(col("pickup_ts")),
)


# --------------------------------------------------
# Remove duplicates
# --------------------------------------------------

df = df.dropDuplicates(["trip_id"])
output_rows = df.count()

# --------------------------------------------------
# Final column order
# --------------------------------------------------

df = df.select(
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
)


# --------------------------------------------------
# Output
# --------------------------------------------------

output_rows = df.count()

print(f"Output rows: {output_rows}")
print(f"Rows removed: {input_rows - output_rows}")


df.repartition("pickup_date") \
    .write \
    .mode("overwrite") \
    .partitionBy("pickup_date") \
    .parquet(output_path)


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
    """
)

metrics.coalesce(1).write.mode("overwrite").json(
    f"s3://{S3_BUCKET}/dq_metrics/trips/run_month={year}-{month}/"
)

print(
    f"Input rows: {input_rows}, "
    f"In-month rows: {in_month_rows}, "
    f"Valid rows: {valid_rows}, "
    f"Output rows: {output_rows}"
)

print("Silver write completed successfully.")

spark.stop()
