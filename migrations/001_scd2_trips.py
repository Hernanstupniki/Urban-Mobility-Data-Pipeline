import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, concat_ws, coalesce
from delta.tables import DeltaTable

ENV = os.getenv("ENV", "dev")
SILVER_PATH = f"data/{ENV}/silver/trips"

spark = (
    SparkSession.builder
    .appName("bootstrap_trips_scd2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1) If dont exist, done
if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
    print(f"[bootstrap_trips_scd2] Silver trips no existe (no es Delta): {SILVER_PATH}")
    spark.stop()
    raise SystemExit(0)

df = spark.read.format("delta").load(SILVER_PATH)

required_cols = ["valid_from", "valid_to", "is_current", "scd_hash"]
missing = [c for c in required_cols if c not in df.columns]

# 2) If exists, done
if not missing:
    print("[bootstrap_trips_scd2] SCD2 columns already present. Nothing to do.")
    spark.stop()
    raise SystemExit(0)

print("[bootstrap_trips_scd2] Missing columns:", missing)

# 3) Add SCD2 columns + basic backfill
out = df

# scd_hash same as scd_ready_df (same columns)
if "scd_hash" in missing:
    out = out.withColumn(
        "scd_hash",
        sha2(
            concat_ws(
                "||",
                coalesce(col("passenger_id").cast("string"), lit("")),
                coalesce(col("driver_id").cast("string"), lit("")),
                coalesce(col("vehicle_id").cast("string"), lit("")),
                coalesce(col("status").cast("string"), lit("")),
                coalesce(col("requested_at").cast("string"), lit("")),
                coalesce(col("accepted_at").cast("string"), lit("")),
                coalesce(col("started_at").cast("string"), lit("")),
                coalesce(col("ended_at").cast("string"), lit("")),
                coalesce(col("canceled_at").cast("string"), lit("")),
                coalesce(col("estimated_distance_km").cast("string"), lit("")),
                coalesce(col("actual_distance_km").cast("string"), lit("")),
                coalesce(col("batch_id").cast("string"), lit("")),
                coalesce(col("source_system").cast("string"), lit(""))
            ),
            256
        )
    )

if "valid_from" in missing:
    out = out.withColumn("valid_from", col("raw_loaded_at"))

if "valid_to" in missing:
    out = out.withColumn("valid_to", lit(None).cast("timestamp"))

if "is_current" in missing:
    out = out.withColumn("is_current", lit(True))

# 4) Replace existing data
(
    out.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(SILVER_PATH)
)

print("[bootstrap_trips_scd2] Bootstrap completed. Added:", missing)
spark.stop()
