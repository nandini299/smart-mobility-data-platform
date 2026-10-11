import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ============================================================
# Configuration
# ============================================================

BUCKET = os.environ["S3_BUCKET"]
REGION = os.getenv("AWS_REGION", "ap-south-1")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")

TOPIC = "sensor-readings"

BASE = f"s3a://{BUCKET}/bronze"

VALID_TYPES = [
    "traffic_count",
    "co_ppm",
    "temperature_c",
    "visibility_m",
]


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("sensor-stream-to-bronze")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "org.apache.hadoop:hadoop-aws:3.3.4"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        f"s3.{REGION}.amazonaws.com"
    )
    .config(
        "spark.sql.session.timeZone",
        "UTC"
    )
    .config(
        "spark.sql.shuffle.partitions",
        "4"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Sensor event schema
# ============================================================

schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("sensor_id", T.StringType()),
    T.StructField("sensor_type", T.StringType()),
    T.StructField("value", T.DoubleType()),
    T.StructField("event_ts_ms", T.LongType()),
    T.StructField("sim_label", T.StringType()),
])


# ============================================================
# Read Kafka
# ============================================================

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 50000)
    .load()
)

# ============================================================
# Parse Kafka JSON
# ============================================================

parsed = (
    raw
    .select(
        F.col("value").cast("string").alias("raw_json"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("partition"),
        F.col("offset"),
    )
    .withColumn(
        "d",
        F.from_json("raw_json", schema)
    )
)


# ============================================================
# Validation
# ============================================================

is_valid = F.coalesce(
    F.col("d.event_id").isNotNull()
    & F.col("d.sensor_id").isNotNull()
    & F.col("d.value").isNotNull()
    & F.col("d.event_ts_ms").isNotNull()
    & F.col("d.sensor_type").isin(VALID_TYPES),
    F.lit(False),
)


# ============================================================
# Valid records
# ============================================================

good = (
    parsed
    .filter(is_valid)
    .select("d.*")
    .withColumn(
        "event_ts",
        F.expr("timestamp_millis(event_ts_ms)")
    )
    .withColumn(
        "date",
        F.to_date("event_ts")
    )
    .withColumn(
        "hour",
        F.hour("event_ts")
    )
)


# ============================================================
# Invalid records
# ============================================================

bad = (
    parsed
    .filter(~is_valid)
    .select(
        "raw_json",
        "kafka_ts",
        "partition",
        "offset",
    )
    .withColumn(
        "date",
        F.to_date("kafka_ts")
    )
)


# ============================================================
# Valid → S3 Bronze
# ============================================================

good_query = (
    good.writeStream
    .format("parquet")
    .option(
        "path",
        f"{BASE}/sensors/"
    )
    .option(
        "checkpointLocation",
         os.path.abspath("checkpoints/sensors")
    )
    .partitionBy(
        "date",
        "hour"
    )
    .trigger(
        processingTime="1 minute"
    )
    .start()
)


# ============================================================
# Invalid → Dead Letter
# ============================================================

bad_query = (
    bad.writeStream
    .format("json")
    .option(
        "path",
        f"{BASE}/dead_letter/"
    )
    .option(
	"checkpointLocation",
	os.path.abspath("checkpoints/dead_letter")
    )
    .partitionBy("date")
    .trigger(
        processingTime="1 minute"
    )
    .start()
)


# ============================================================
# Status
# ============================================================

print("=" * 60)
print("SPARK SENSOR STREAMING STARTED")
print("=" * 60)
print(f"Kafka:       {BOOTSTRAP}")
print(f"Topic:       {TOPIC}")
print(f"S3 bucket:   {BUCKET}")
print(f"Bronze:      {BASE}/sensors/")
print(f"Dead Letter: {BASE}/dead_letter/")
print("=" * 60)

print("\nWaiting for Kafka data...")
print("Spark UI: http://localhost:4040")
print("Press Ctrl+C to stop.\n")


# ============================================================
# Keep streaming
# ============================================================

spark.streams.awaitAnyTermination()
