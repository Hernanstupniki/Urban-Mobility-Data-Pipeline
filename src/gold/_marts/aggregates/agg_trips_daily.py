"""
Gold Mart – Aggregate: agg_trips_daily
Incremental, recompute affected date_keys from fact_trips (late arrivals safe)

Source: gold/_marts/facts/fact_trips
Dim date (optional join): gold/_conformed/static/dim_date
"""

import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp,
    max as spark_max, sum as spark_sum, avg as spark_avg,
    when
)
from delta.tables import DeltaTable

# Config
JOB_NAME = "agg_trips_daily_build_gold_marts"

ENV = os.getenv("ENV", "dev")

FACT_TRIPS_PATH = f"data/{ENV}/gold/_marts/facts/fact_trips"
DIM_DATE_PATH = f"data/{ENV}/gold/_conformed/static/dim_date"

GOLD_BASE_PATH = f"data/{ENV}/gold/_marts/aggregates/agg_trips_daily"


def delta_exists(spark, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def read_target_watermark(spark) -> datetime:
    """
    Watermark = max(max_fact_raw_loaded_at) in target agg_trips_daily
    If target doesn't exist -> 1970-01-01
    """
    if not delta_exists(spark, GOLD_BASE_PATH):
        return datetime(1970, 1, 1)

    df = spark.read.format("delta").load(GOLD_BASE_PATH)
    if df.rdd.isEmpty():
        return datetime(1970, 1, 1)

    ts = df.select(spark_max(col("max_fact_raw_loaded_at")).alias("wm")).first()["wm"]
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

    if not delta_exists(spark, FACT_TRIPS_PATH):
        raise RuntimeError(f"[{JOB_NAME}] fact_trips not found at: {FACT_TRIPS_PATH}")

    target_exists = delta_exists(spark, GOLD_BASE_PATH)

    # 1) Watermark
    wm = read_target_watermark(spark)
    print(f"[{JOB_NAME}] target watermark (max max_fact_raw_loaded_at): {wm}")

    # 2) Find affected date_keys from NEW facts
    fact_inc = spark.read.format("delta").load(FACT_TRIPS_PATH).filter(col("raw_loaded_at") > lit(wm))

    inc_count = fact_inc.count()
    print(f"[{JOB_NAME}] incremental fact rows: {inc_count}")

    if inc_count == 0:
        print("No new fact rows to process")
        spark.stop()
        return

    if "request_date_key" not in fact_inc.columns:
        raise ValueError("request_date_key not found in fact_trips (expected from your fact_trips build)")

    affected_dates = fact_inc.select(col("request_date_key").cast("int").alias("date_key")).dropDuplicates(["date_key"])
    affected_dates_count = affected_dates.count()
    print(f"[{JOB_NAME}] affected date_keys: {affected_dates_count}")

    if affected_dates_count == 0:
        print("No affected date_keys to process")
        spark.stop()
        return

    # 3) Recompute FULL aggregates for those date_keys (late-arrival safe)
    fact_all = spark.read.format("delta").load(FACT_TRIPS_PATH)

    fact_slice = (
        fact_all
        .join(affected_dates, fact_all["request_date_key"] == affected_dates["date_key"], "inner")
        .drop(affected_dates["date_key"])
    )

    # Status semantics (robusto)
    status_col = col("status") if "status" in fact_slice.columns else lit(None).cast("string")

    # Optional numeric columns
    fare_col = col("fare_amount").cast("double") if "fare_amount" in fact_slice.columns else lit(None).cast("double")
    dist_col = None
    if "actual_distance_km" in fact_slice.columns:
        dist_col = col("actual_distance_km").cast("double")
    elif "estimated_distance_km" in fact_slice.columns:
        dist_col = col("estimated_distance_km").cast("double")
    else:
        dist_col = lit(None).cast("double")

    agg = (
        fact_slice
        .groupBy(col("request_date_key").cast("int").alias("date_key"))
        .agg(
            spark_sum(lit(1)).cast("long").alias("trips_total"),
            spark_sum(when(status_col.isin("completed", "complete", "done"), lit(1)).otherwise(lit(0))).cast("long").alias("trips_completed"),
            spark_sum(when(status_col.isin("cancelled", "canceled"), lit(1)).otherwise(lit(0))).cast("long").alias("trips_cancelled"),
            spark_sum(when(status_col.isin("requested", "accepted", "started", "in_progress"), lit(1)).otherwise(lit(0))).cast("long").alias("trips_active"),
            spark_sum(fare_col).alias("sum_fare_amount"),
            spark_avg(fare_col).alias("avg_fare_amount"),
            spark_sum(dist_col).alias("sum_distance_km"),
            spark_avg(dist_col).alias("avg_distance_km"),
            spark_max(col("raw_loaded_at")).alias("max_fact_raw_loaded_at")
        )
        .withColumn("dwh_loaded_at", current_timestamp())
    )

    # 4) Optional join to dim_date to add date column
    if delta_exists(spark, DIM_DATE_PATH):
        dim_date = spark.read.format("delta").load(DIM_DATE_PATH).select(
            col("date_key").cast("int").alias("d_date_key"),
            col("date").alias("date")
        )
        agg = (
            agg
            .join(dim_date, agg["date_key"] == dim_date["d_date_key"], "left")
            .drop("d_date_key")
        )
    else:
        agg = agg.withColumn("date", lit(None).cast("date"))

    out_count = agg.count()
    print(f"[{JOB_NAME}] output rows (affected dates): {out_count}")

    # 5) First run -> create
    if not target_exists:
        (
            agg.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(GOLD_BASE_PATH)
        )
        print(f"[{JOB_NAME}] agg_trips_daily created at: {GOLD_BASE_PATH}")
        spark.stop()
        return

    # 6) MERGE (upsert by date_key)
    target = DeltaTable.forPath(spark, GOLD_BASE_PATH)

    cols = agg.columns
    update_set = {c: f"s.{c}" for c in cols if c != "date_key"}
    insert_vals = {c: f"s.{c}" for c in cols}

    (
        target.alias("t")
        .merge(agg.alias("s"), "t.date_key = s.date_key")
        .whenMatchedUpdate(
            condition="s.max_fact_raw_loaded_at > t.max_fact_raw_loaded_at",
            set=update_set
        )
        .whenNotMatchedInsert(values=insert_vals)
        .execute()
    )

    print(f"[{JOB_NAME}] agg_trips_daily MERGE completed at: {GOLD_BASE_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()