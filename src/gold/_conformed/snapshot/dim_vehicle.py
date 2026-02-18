import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, row_number, lit, max as spark_max,
    current_timestamp
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Config
JOB_NAME = "dim_vehicle_snapshot_build_gold_conformed"

ENV = os.getenv("ENV", "dev")

SILVER_BASE_PATH = f"data/{ENV}/silver/vehicles"
GOLD_BASE_PATH = f"data/{ENV}/gold/_conformed/snapshot/dim_vehicle"


def read_target_watermark(spark) -> datetime:
    """
    Watermark = max(raw_loaded_at) in target dim_vehicle (snapshot)
    If target doesn't exist -> 1970-01-01
    """
    if not DeltaTable.isDeltaTable(spark, GOLD_BASE_PATH):
        return datetime(1970, 1, 1)

    df = spark.read.format("delta").load(GOLD_BASE_PATH)
    ts = df.select(spark_max(col("raw_loaded_at")).alias("wm")).first()["wm"]
    return ts or datetime(1970, 1, 1)


def main():
    spark = (
        SparkSession.builder
        .appName(JOB_NAME)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # DEV tuning
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "64MB")

    # Delta schema auto-merge (dev default)
    AUTO_MERGE = os.getenv("DELTA_AUTO_MERGE", "1" if ENV == "dev" else "0") == "1"
    if AUTO_MERGE:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        print("[CONFIG] Delta schema auto-merge: ENABLED")
    else:
        print("[CONFIG] Delta schema auto-merge: DISABLED")

    if not DeltaTable.isDeltaTable(spark, SILVER_BASE_PATH):
        raise RuntimeError(f"[{JOB_NAME}] Silver table not found at: {SILVER_BASE_PATH}")

    target_exists = DeltaTable.isDeltaTable(spark, GOLD_BASE_PATH)

    # 1) Watermark from target
    wm = read_target_watermark(spark)
    print(f"[{JOB_NAME}] target watermark (max raw_loaded_at): {wm}")

    # 2) Read Silver (current snapshot only)
    silver_df = (
        spark.read.format("delta").load(SILVER_BASE_PATH)
        .filter(col("is_current") == lit(True))
    )

    # 3) Incremental filter (only if target exists)
    if target_exists:
        silver_df = silver_df.filter(col("raw_loaded_at") > lit(wm))

    inc_count = silver_df.count()
    print(f"[{JOB_NAME}] silver incremental count: {inc_count}")

    if inc_count == 0:
        print("No new silver records to process")
        spark.stop()
        return

    # 4) Latest per vehicle_id inside incremental batch
    if "vehicle_id" not in silver_df.columns:
        raise ValueError("vehicle_id not found in silver/vehicles schema")

    w = Window.partitionBy("vehicle_id").orderBy(col("raw_loaded_at").desc())
    latest_df = (
        silver_df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # 5) Add DWH audit
    dim_df = latest_df.withColumn("dwh_loaded_at", current_timestamp())

    # 6) First run -> create
    if not target_exists:
        (
            dim_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(GOLD_BASE_PATH)
        )
        print(f"[{JOB_NAME}] dim_vehicle created at: {GOLD_BASE_PATH}")
        spark.stop()
        return

    # 7) Incremental MERGE (SCD1 snapshot)
    target = DeltaTable.forPath(spark, GOLD_BASE_PATH)

    cols = dim_df.columns
    update_set = {c: f"s.{c}" for c in cols if c != "vehicle_id"}
    insert_vals = {c: f"s.{c}" for c in cols}

    (
        target.alias("t")
        .merge(dim_df.alias("s"), "t.vehicle_id = s.vehicle_id")
        .whenMatchedUpdate(
            condition="s.raw_loaded_at > t.raw_loaded_at",
            set=update_set
        )
        .whenNotMatchedInsert(values=insert_vals)
        .execute()
    )

    print(f"[{JOB_NAME}] dim_vehicle MERGE completed at: {GOLD_BASE_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
