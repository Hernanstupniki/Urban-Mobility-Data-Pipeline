import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, row_number, when, lit,min as spark_min,
    max as spark_max, to_date, current_timestamp, coalesce
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Config
JOB_NAME = "trips_bronze_to_silver"

ENV = os.getenv("ENV", "dev")
BRONZE_BASE_PATH = f"data/{ENV}/bronze/trips"
SILVER_BASE_PATH = f"data/{ENV}/silver/trips"

# Delta control table (watermarks)
CONTROL_BASE_PATH = f"data/{ENV}/_control"
ETL_CONTROL_PATH = f"{CONTROL_BASE_PATH}/etl_control"


# Delta control helpers
def ensure_etl_control_table(spark):
    """
    Create the table control in delta if not exits
    """
    if DeltaTable.isDeltaTable(spark, ETL_CONTROL_PATH):
        return

    (
        spark.createDataFrame(
            [],
            "job_name string, last_loaded_ts timestamp, last_success_ts timestamp, last_status string"
        )
        .write.format("delta")
        .mode("overwrite")
        .save(ETL_CONTROL_PATH)
    )


def read_last_loaded_ts(spark) -> datetime:
    """
    Read watermark(last_loaded_ts) from Delta control table
    """
    if not DeltaTable.isDeltaTable(spark, ETL_CONTROL_PATH):
        return datetime(1970, 1, 1)

    df = (
        spark.read.format("delta").load(ETL_CONTROL_PATH)
        .filter(col("job_name") == lit(JOB_NAME))
    )

    if df.rdd.isEmpty():
        return datetime(1970, 1, 1)

    ts = df.select("last_loaded_ts").first()[0]
    return ts or datetime(1970, 1, 1)


def upsert_etl_control(job_name: str, last_loaded_ts, status: str):
    """
    Upsert watermark in Delta.
    - If last_loaded_ts is none(FAIL), DO NOT step the previous watermark.
    """
    ensure_etl_control_table(spark)

    target = DeltaTable.forPath(spark, ETL_CONTROL_PATH)

    updates = (
        spark.createDataFrame(
            [(job_name, last_loaded_ts, status)],
            "job_name string, last_loaded_ts timestamp, last_status string"
        )
        .withColumn("last_success_ts", current_timestamp())
    )

    (
        target.alias("t")
        .merge(updates.alias("s"), "t.job_name = s.job_name")
        .whenMatchedUpdate(set={
            "last_loaded_ts": "coalesce(s.last_loaded_ts, t.last_loaded_ts)",
            "last_success_ts": "s.last_success_ts",
            "last_status": "s.last_status",
        })
        .whenNotMatchedInsert(values={
            "job_name": "s.job_name",
            "last_loaded_ts": "s.last_loaded_ts",
            "last_success_ts": "s.last_success_ts",
            "last_status": "s.last_status",
        })
        .execute()
    )


# Main
def main():
    global spark
    spark = (
        SparkSession.builder
        .appName(JOB_NAME)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # DEV tuning
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "64MB")

    silver_exists = DeltaTable.isDeltaTable(spark, SILVER_BASE_PATH)

    try:
        # 1) Watermark (raw_loaded_at) from Delta control table
        last_ts = read_last_loaded_ts(spark)
        print(f"[{JOB_NAME}] last_loaded_ts(raw_loaded_at): {last_ts}")

        # 2) Read Bronze incremental by raw_loaded_at
        bronze_reader = spark.read.format("delta").load(BRONZE_BASE_PATH)

        if silver_exists:
            bronze_reader = (
                bronze_reader
                # prune de particiones (Bronze partitionBy(load_date))
                .filter(col("load_date") >= to_date(lit(last_ts)))
                # incremental real
                .filter(col("raw_loaded_at") > lit(last_ts))
            )

        bronze_df = bronze_reader

            # Debug / confirmation
        bronze_count = bronze_df.count()
        print(f"[{JOB_NAME}] bronze_df count: {bronze_count}")

        min_max = bronze_df.select(
            spark_min("raw_loaded_at").alias("min_ts"),
            spark_max("raw_loaded_at").alias("max_ts")
        ).first()
        print(f"[{JOB_NAME}] min/max raw_loaded_at: {min_max}")

        if bronze_count == 0:
            print("No new bronze records to process")
            spark.stop()
            return

        # 3) Latest record per trip_id (only inside the new data)
        window_spec = (
            Window.partitionBy("trip_id")
            .orderBy(col("raw_loaded_at").desc())
        )

        latest_trips_df = (
            bronze_df
            .withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        # 4) Enrichment
        enriched_df = (
            latest_trips_df
            .withColumn(
                "has_distance_in_invalid_status",
                when(
                    (col("actual_distance_km").isNotNull()) &
                    (col("actual_distance_km") > 0) &
                    (~col("status").isin("completed", "started")),
                    lit(True)
                )
                .when(
                    (col("actual_distance_km").isNull()) & 
                    (col("status") == ("completed")), 
                    lit(True))
                .otherwise(lit(False))
            )
        )

        # 5) First run: create Silver with rich schema
        if not silver_exists:
            (
                enriched_df.write.format("delta")
                .mode("overwrite")
                .save(SILVER_BASE_PATH)
            )
            max_ts = enriched_df.select(spark_max("raw_loaded_at")).first()[0]
            upsert_etl_control(JOB_NAME, max_ts, "SUCCESS")
            print("Silver trips table created + etl_control (delta) updated")
            spark.stop()
            return

        # 6) Merge incremental
        silver_table = DeltaTable.forPath(spark, SILVER_BASE_PATH)

        (
            silver_table.alias("t")
            .merge(enriched_df.alias("s"), "t.trip_id = s.trip_id")
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
                    "has_distance_in_invalid_status": "s.has_distance_in_invalid_status",
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
                    "has_distance_in_invalid_status": "s.has_distance_in_invalid_status",
                }
            )
            .execute()
        )

        # 7) Update watermark at the end
        max_ts = enriched_df.select(spark_max("raw_loaded_at")).first()[0]
        upsert_etl_control(JOB_NAME, max_ts, "SUCCESS")

        print("Silver trips MERGE completed + etl_control (delta) updated")
        spark.stop()

    except Exception as e:
        # marcar FAIL sin mover watermark
        try:
            upsert_etl_control(JOB_NAME, None, f"FAIL: {type(e).__name__}")
        except Exception:
            pass
        spark.stop()
        raise

if __name__ == "__main__":
    main()
