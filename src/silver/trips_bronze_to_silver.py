import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, when, expr
from pyspark.sql.window import Window
from delta.tables import DeltaTable


# Config
JOB_NAME = "trips_bronze_to_silver"

ENV = os.getenv("ENV", "dev")
BRONZE_BASE_PATH = f"data/{ENV}/bronze/trips"
SILVER_BASE_PATH = f"data/{ENV}/silver/trips"

# Spark session
spark = (
    SparkSession.builder
    .appName(JOB_NAME)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# DEV performance
spark.conf.set("spark.sql.shuffle.partitions", "4")
spark.conf.set("spark.default.parallelism", "4")
spark.conf.set("spark.sql.files.maxPartitionBytes", "64MB")


# Read Bronze (Delta root)
bronze_df = (
    spark.read
    .format("delta")
    .load(BRONZE_BASE_PATH)
)

# Pick latest version per trip_id (raw_loaded_at wins)
window_spec = (
    Window
    .partitionBy("trip_id")
    .orderBy(col("raw_loaded_at").desc())
)

latest_trips_df = (
    bronze_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
    .withColumn("has_distance_in_invalid_status",
                when(
                    ((col("status") != "completed") & (col("status") != "started")) & ((col("actual_distance_km").isNotNull()) | (col("actual_distance_km") > 0)), True)
                .otherwise(False))
                )

# Create Silver table if it does not exist
if not DeltaTable.isDeltaTable(spark, SILVER_BASE_PATH):
    (
        latest_trips_df
        .write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_BASE_PATH)
    )
    print("Silver trips table created")
    spark.stop()
    exit(0)

# MERGE INTO Silver
silver_table = DeltaTable.forPath(spark, SILVER_BASE_PATH)

(
    silver_table.alias("t")
    .merge(
        latest_trips_df.alias("s"),
        "t.trip_id = s.trip_id"
    )
    .whenMatchedUpdate(
        condition="s.raw_loaded_at > t.raw_loaded_at",
        set={
            "trip_id": "s.trip_id",
            "passenger_id": "s.passenger_id",
            "driver_id": "s.driver_id",
            "vehicle_id": "s.vehicle_id",
            "status": "s.status",
            "requested_at": "s.requested_at",
            "raw_loaded_at": "s.raw_loaded_at",
            "batch_id": "s.batch_id",
            "source_system": "s.source_system",
            "has_distance_in_invalid_status": "s.has_distance_in_invalid_status"
        }
    )
    .whenNotMatchedInsert(
        values={
            "trip_id": "s.trip_id",
            "passenger_id": "s.passenger_id",
            "driver_id": "s.driver_id",
            "vehicle_id": "s.vehicle_id",
            "status": "s.status",
            "requested_at": "s.requested_at",
            "raw_loaded_at": "s.raw_loaded_at",
            "batch_id": "s.batch_id",
            "source_system": "s.source_system",
            "has_distance_in_invalid_status": "s.has_distance_in_invalid_status"
        }
    )
    .execute()
)

print("Silver trips MERGE completed")

spark.stop()
