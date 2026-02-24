"""
Gold Mart – Fact: fact_trips
Incremental SCD1 snapshot (1 row per trip_id)
Star schema keys validated against Gold dims (0 = UNKNOWN)

Source: Silver trips (is_current=true)
Dims:
- gold/_conformed/snapshot/dim_passenger  (passenger_id)
- gold/_conformed/snapshot/dim_driver     (driver_id)
- gold/_conformed/snapshot/dim_vehicle    (vehicle_id)
- gold/_conformed/static/dim_zone         (zone_id)
"""

import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, row_number, current_timestamp,
    max as spark_max, to_date, date_format, coalesce, when
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Config
JOB_NAME = "fact_trips_build_gold_marts"

ENV = os.getenv("ENV", "dev")

SILVER_BASE_PATH = f"data/{ENV}/silver/trips"
GOLD_BASE_PATH = f"data/{ENV}/gold/_marts/facts/fact_trips"

DIM_PASSENGER_PATH = f"data/{ENV}/gold/_conformed/snapshot/dim_passenger"
DIM_DRIVER_PATH = f"data/{ENV}/gold/_conformed/snapshot/dim_driver"
DIM_VEHICLE_PATH = f"data/{ENV}/gold/_conformed/snapshot/dim_vehicle"
DIM_ZONE_PATH = f"data/{ENV}/gold/_conformed/static/dim_zone"


def delta_exists(spark, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def read_target_watermark(spark) -> datetime:
    """
    Watermark = max(raw_loaded_at) in target fact_trips
    If target doesn't exist -> 1970-01-01
    """
    if not delta_exists(spark, GOLD_BASE_PATH):
        return datetime(1970, 1, 1)

    df = spark.read.format("delta").load(GOLD_BASE_PATH)
    if df.rdd.isEmpty():
        return datetime(1970, 1, 1)

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

    if not delta_exists(spark, SILVER_BASE_PATH):
        raise RuntimeError(f"[{JOB_NAME}] Silver table not found at: {SILVER_BASE_PATH}")

    target_exists = delta_exists(spark, GOLD_BASE_PATH)

    # 1) Watermark from target
    wm = read_target_watermark(spark)
    print(f"[{JOB_NAME}] target watermark (max raw_loaded_at): {wm}")

    # 2) Read Silver current
    silver_df = spark.read.format("delta").load(SILVER_BASE_PATH)
    if "is_current" in silver_df.columns:
        silver_df = silver_df.filter(col("is_current") == lit(True))

    if "trip_id" not in silver_df.columns:
        raise ValueError("trip_id not found in silver/trips schema")

    # 3) Incremental filter
    if target_exists:
        silver_df = silver_df.filter(col("raw_loaded_at") > lit(wm))

    inc_count = silver_df.count()
    print(f"[{JOB_NAME}] silver incremental count: {inc_count}")

    if inc_count == 0:
        print("No new silver records to process")
        spark.stop()
        return

    # 4) Latest per trip_id inside incremental batch
    w = Window.partitionBy("trip_id").orderBy(col("raw_loaded_at").desc())
    latest_df = (
        silver_df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # 5) Basic conformed keys + audit
    fact_df = (
        latest_df
        .withColumn("trip_id", col("trip_id").cast("long"))
        .withColumn("passenger_id", col("passenger_id").cast("long") if "passenger_id" in latest_df.columns else col("passenger_id"))
        .withColumn("driver_id", col("driver_id").cast("long") if "driver_id" in latest_df.columns else col("driver_id"))
        .withColumn("vehicle_id", col("vehicle_id").cast("long") if "vehicle_id" in latest_df.columns else col("vehicle_id"))
        .withColumn("pickup_zone_id", col("pickup_zone_id").cast("long") if "pickup_zone_id" in latest_df.columns else col("pickup_zone_id"))
        .withColumn("dropoff_zone_id", col("dropoff_zone_id").cast("long") if "dropoff_zone_id" in latest_df.columns else col("dropoff_zone_id"))
        .withColumn(
            "request_date_key",
            coalesce(
                date_format(to_date(col("requested_at")), "yyyyMMdd").cast("int") if "requested_at" in latest_df.columns else lit(None).cast("int"),
                date_format(to_date(col("raw_loaded_at")), "yyyyMMdd").cast("int"),
                lit(0).cast("int")
            )
        )
        .withColumn("dwh_loaded_at", current_timestamp())
    )

    # ---------- STAR SCHEMA VALIDATION (0 = UNKNOWN) ----------
    # default keys = ids (or 0)
    if "passenger_id" in fact_df.columns:
        fact_df = fact_df.withColumn("passenger_key", coalesce(col("passenger_id"), lit(0)).cast("long"))
    if "driver_id" in fact_df.columns:
        fact_df = fact_df.withColumn("driver_key", coalesce(col("driver_id"), lit(0)).cast("long"))
    if "vehicle_id" in fact_df.columns:
        fact_df = fact_df.withColumn("vehicle_key", coalesce(col("vehicle_id"), lit(0)).cast("long"))
    if "pickup_zone_id" in fact_df.columns:
        fact_df = fact_df.withColumn("pickup_zone_key", coalesce(col("pickup_zone_id"), lit(0)).cast("long"))
    if "dropoff_zone_id" in fact_df.columns:
        fact_df = fact_df.withColumn("dropoff_zone_key", coalesce(col("dropoff_zone_id"), lit(0)).cast("long"))

    # validate passenger_key
    if delta_exists(spark, DIM_PASSENGER_PATH) and "passenger_key" in fact_df.columns:
        dim_p = spark.read.format("delta").load(DIM_PASSENGER_PATH).select(col("passenger_id").cast("long").alias("p_id")).dropDuplicates(["p_id"])
        fact_df = (
            fact_df
            .join(dim_p, fact_df["passenger_key"] == dim_p["p_id"], "left")
            .withColumn("passenger_key", when(col("p_id").isNull(), lit(0)).otherwise(col("passenger_key")))
            .drop("p_id")
        )

    # validate driver_key
    if delta_exists(spark, DIM_DRIVER_PATH) and "driver_key" in fact_df.columns:
        dim_d = spark.read.format("delta").load(DIM_DRIVER_PATH).select(col("driver_id").cast("long").alias("d_id")).dropDuplicates(["d_id"])
        fact_df = (
            fact_df
            .join(dim_d, fact_df["driver_key"] == dim_d["d_id"], "left")
            .withColumn("driver_key", when(col("d_id").isNull(), lit(0)).otherwise(col("driver_key")))
            .drop("d_id")
        )

    # validate vehicle_key
    if delta_exists(spark, DIM_VEHICLE_PATH) and "vehicle_key" in fact_df.columns:
        dim_v = spark.read.format("delta").load(DIM_VEHICLE_PATH).select(col("vehicle_id").cast("long").alias("v_id")).dropDuplicates(["v_id"])
        fact_df = (
            fact_df
            .join(dim_v, fact_df["vehicle_key"] == dim_v["v_id"], "left")
            .withColumn("vehicle_key", when(col("v_id").isNull(), lit(0)).otherwise(col("vehicle_key")))
            .drop("v_id")
        )

    # validate zones (static)
    if delta_exists(spark, DIM_ZONE_PATH):
        dim_z = spark.read.format("delta").load(DIM_ZONE_PATH).select(col("zone_id").cast("long").alias("z_id")).dropDuplicates(["z_id"])

        if "pickup_zone_key" in fact_df.columns:
            fact_df = (
                fact_df
                .join(dim_z.withColumnRenamed("z_id", "pz_id"), fact_df["pickup_zone_key"] == col("pz_id"), "left")
                .withColumn("pickup_zone_key", when(col("pz_id").isNull(), lit(0)).otherwise(col("pickup_zone_key")))
                .drop("pz_id")
            )

        if "dropoff_zone_key" in fact_df.columns:
            fact_df = (
                fact_df
                .join(dim_z.withColumnRenamed("z_id", "dz_id"), fact_df["dropoff_zone_key"] == col("dz_id"), "left")
                .withColumn("dropoff_zone_key", when(col("dz_id").isNull(), lit(0)).otherwise(col("dropoff_zone_key")))
                .drop("dz_id")
            )

    # 6) First run -> create
    if not target_exists:
        (
            fact_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(GOLD_BASE_PATH)
        )
        print(f"[{JOB_NAME}] fact_trips created at: {GOLD_BASE_PATH}")
        spark.stop()
        return

    # 7) Incremental MERGE (SCD1 snapshot)
    target = DeltaTable.forPath(spark, GOLD_BASE_PATH)

    cols = fact_df.columns
    update_set = {c: f"s.{c}" for c in cols if c != "trip_id"}
    insert_vals = {c: f"s.{c}" for c in cols}

    (
        target.alias("t")
        .merge(fact_df.alias("s"), "t.trip_id = s.trip_id")
        .whenMatchedUpdate(
            condition="s.raw_loaded_at > t.raw_loaded_at",
            set=update_set
        )
        .whenNotMatchedInsert(values=insert_vals)
        .execute()
    )

    print(f"[{JOB_NAME}] fact_trips MERGE completed at: {GOLD_BASE_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()