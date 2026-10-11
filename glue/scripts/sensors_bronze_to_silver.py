"""Bronze -> Silver for sensor readings (one day per run): dedup + compaction."""

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["bucket", "run_date"])
bucket, run_date = args["bucket"], args["run_date"]

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

base = f"s3://{bucket}/bronze/sensors/"

raw = (
    spark.read
    .option("basePath", base)
    .parquet(f"{base}date={run_date}/")
    .cache()
)

input_rows = raw.count()

clean = (
    raw
    .dropDuplicates(["event_id"])
    .filter(F.col("value") >= 0)
    .select(
        "event_id",
        "sensor_id",
        "sensor_type",
        "value",
        "event_ts",
        "sim_label",
        F.col("date").alias("reading_date"),
    )
    .cache()
)

output_rows = clean.count()

# Compaction: many small streaming files -> one partition per day
(
    clean
    .repartition("reading_date")
    .write
    .mode("overwrite")
    .partitionBy("reading_date")
    .parquet(f"s3://{bucket}/silver/sensor_readings/")
)

# Data-quality metrics
(
    spark.createDataFrame(
        [
            (
                run_date,
                input_rows,
                output_rows,
                input_rows - output_rows,
            )
        ],
        "run_date string, input_rows long, output_rows long, removed_rows long",
    )
    .coalesce(1)
    .write
    .mode("overwrite")
    .json(
        f"s3://{bucket}/dq_metrics/sensors/run_date={run_date}/"
    )
)

print(
    f"run_date={run_date} "
    f"input={input_rows} "
    f"output={output_rows} "
    f"removed={input_rows - output_rows}"
)
