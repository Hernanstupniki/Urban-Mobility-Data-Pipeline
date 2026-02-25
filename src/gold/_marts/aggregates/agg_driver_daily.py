"""
Gold Mart – Aggregate: agg_driver_daily
Incremental daily aggregates per driver (recompute affected (day, driver) pairs)

Source priority:
1) Gold marts facts/fact_trips (preferred)
2) Silver trips (fallback)

Watermark: max(max_raw_loaded_at) in target aggregate
"""

import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp,
    max as spark_max, sum as spark_sum, countDistinct,
    to_date, date_format, when, coalesce
)
from delta.tables import DeltaTable


# Config
JOB_NAME = "agg_driver_daily_build_gold_marts"

ENV = os.getenv("ENV", "dev")

FACT_TRIPS_PATH = f"data/{ENV}/gold/_marts/facts/fact_trips"
SILVER_TRIPS_PATH = f"data/{ENV}/silver/trips"

GOLD_BASE_PATH = f"data/{ENV}/gold/_marts/aggregates/agg_driver_daily"


def delta_exists(spark, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def read_target_watermark(spark) -> datetime:
    """
    Watermark = max(max_raw_loaded_at) in target agg_driver_daily.
    If target doesn't exist / empty -> 1970-01-01
    """
    if not delta_exists(spark, GOLD_BASE_PATH):
        return datetime(1970, 1, 1)

    df = spark.read.format("delta").load(GOLD_BASE_PATH)
    if df.rdd.isEmpty():
        return datetime(1970, 1, 1)

    ts = df.select(spark_max(col("max_raw_loaded_at")).alias("wm")).first()["wm"]
    return ts or datetime(1970, 1, 1)


def pick_trip_ts_col(cols):
    """
    Choose a business timestamp column for daily grain.
    Priority: requested_at -> started_at -> created_at -> raw_loaded_at
    """
    for c in ["requested_at", "started_at", "created_at", "raw_loaded_at"]:
        if c in cols:
            return c
    return None


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

    target_exists = delta_exists(spark, GOLD_BASE_PATH)

    # 1) watermark
    wm = read_target_watermark(spark)
    print(f"[{JOB_NAME}] target watermark (max max_raw_loaded_at): {wm}")

    try:
        # 2) source
        if delta_exists(spark, FACT_TRIPS_PATH):
            source_path = FACT_TRIPS_PATH
            print(f"[{JOB_NAME}] using source: {FACT_TRIPS_PATH}")
        elif delta_exists(spark, SILVER_TRIPS_PATH):
            source_path = SILVER_TRIPS_PATH
            print(f"[{JOB_NAME}] WARN: fact_trips not found. using fallback source: {SILVER_TRIPS_PATH}")
        else:
            raise RuntimeError(f"[{JOB_NAME}] No trips source found at: {FACT_TRIPS_PATH} or {SILVER_TRIPS_PATH}")

        trips_all = spark.read.format("delta").load(source_path)

        if "is_current" in trips_all.columns:
            trips_all = trips_all.filter(col("is_current") == lit(True))

        if "trip_id" not in trips_all.columns:
            raise ValueError(f"[{JOB_NAME}] trip_id not found in source schema: {source_path}")
        if "driver_id" not in trips_all.columns:
            raise ValueError(f"[{JOB_NAME}] driver_id not found in source schema: {source_path}")

        ts_col = pick_trip_ts_col(trips_all.columns)
        if ts_col is None:
            raise ValueError(f"[{JOB_NAME}] No timestamp column found for daily grain in source: {source_path}")

        # normalize driver_id + compute trip_date
        trips_all = trips_all.withColumn("driver_id", coalesce(col("driver_id").cast("long"), lit(0).cast("long")))
        trips_all = trips_all.withColumn("trip_date", to_date(col(ts_col)))

        # mandatory for incremental
        if target_exists and "raw_loaded_at" not in trips_all.columns:
            raise ValueError(f"[{JOB_NAME}] raw_loaded_at not found in source, cannot do incremental watermarking")

        # 3) incremental detector
        trips_inc = trips_all
        if target_exists:
            trips_inc = trips_inc.filter(col("raw_loaded_at") > lit(wm))

        inc_count = trips_inc.count()
        print(f"[{JOB_NAME}] silver/fact incremental count: {inc_count}")

        if inc_count == 0:
            print("No new records to process")
            spark.stop()
            return

        # 4) build dataset to aggregate
        if not target_exists:
            # first run: aggregate all
            trips = trips_all.filter(col("trip_date").isNotNull())
            print(f"[{JOB_NAME}] first run -> aggregating full dataset")
        else:
            affected_pairs = (
                trips_inc
                .select("trip_date", "driver_id")
                .dropna(subset=["trip_date"])
                .dropDuplicates(["trip_date", "driver_id"])
            )

            affected_cnt = affected_pairs.count()
            print(f"[{JOB_NAME}] affected (day, driver) pairs: {affected_cnt}")

            if affected_cnt == 0:
                print("No affected (day, driver) pairs after filtering (nothing to aggregate)")
                spark.stop()
                return

            trips = trips_all.join(affected_pairs, on=["trip_date", "driver_id"], how="inner")

        # 5) aggregate (IMPORTANT: metrics that use status/fare/etc must be inside agg)
        has_status = "status" in trips.columns
        has_fare = "fare_amount" in trips.columns
        has_dist = ("actual_distance_km" in trips.columns) or ("estimated_distance_km" in trips.columns)
        has_passenger = "passenger_id" in trips.columns

        fare_col = "fare_amount" if "fare_amount" in trips.columns else None
        dist_col = "actual_distance_km" if "actual_distance_km" in trips.columns else (
            "estimated_distance_km" if "estimated_distance_km" in trips.columns else None
        )

        agg_exprs = [
            countDistinct(col("trip_id")).alias("trips_count"),
            spark_max(col("raw_loaded_at")).alias("max_raw_loaded_at"),
        ]

        if has_status:
            agg_exprs.append(
                spark_sum(when(col("status") == lit("completed"), lit(1)).otherwise(lit(0))).alias("completed_trips")
            )
            agg_exprs.append(
                spark_sum(when(col("status") == lit("canceled"), lit(1)).otherwise(lit(0))).alias("canceled_trips")
            )

        if has_passenger:
            agg_exprs.append(countDistinct(col("passenger_id")).alias("unique_passengers"))

        if has_fare and fare_col:
            agg_exprs.append(spark_sum(coalesce(col(fare_col).cast("double"), lit(0.0))).alias("total_fare_amount"))

        if has_dist and dist_col:
            agg_exprs.append(spark_sum(coalesce(col(dist_col).cast("double"), lit(0.0))).alias("total_distance_km"))

        g = trips.groupBy("trip_date", "driver_id").agg(*agg_exprs)

        # Ensure all columns exist (set NULL if missing in source)
        if "completed_trips" not in g.columns:
            g = g.withColumn("completed_trips", lit(None).cast("long"))
        if "canceled_trips" not in g.columns:
            g = g.withColumn("canceled_trips", lit(None).cast("long"))
        if "unique_passengers" not in g.columns:
            g = g.withColumn("unique_passengers", lit(None).cast("long"))
        if "total_fare_amount" not in g.columns:
            g = g.withColumn("total_fare_amount", lit(None).cast("double"))
        if "total_distance_km" not in g.columns:
            g = g.withColumn("total_distance_km", lit(None).cast("double"))

        agg_df = (
            g
            .withColumn("trip_date_key", date_format(col("trip_date"), "yyyyMMdd").cast("int"))
            .withColumn("dwh_loaded_at", current_timestamp())
            .select(
                "trip_date_key",
                "trip_date",
                "driver_id",
                "trips_count",
                "completed_trips",
                "canceled_trips",
                "unique_passengers",
                "total_fare_amount",
                "total_distance_km",
                "max_raw_loaded_at",
                "dwh_loaded_at",
            )
        )

        out_cnt = agg_df.count()
        print(f"[{JOB_NAME}] output rows: {out_cnt}")

        # 6) first run -> overwrite
        if not target_exists:
            (
                agg_df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(GOLD_BASE_PATH)
            )
            print(f"[{JOB_NAME}] agg_driver_daily created at: {GOLD_BASE_PATH}")
            spark.stop()
            return

        # 7) merge incremental by (day, driver)
        target = DeltaTable.forPath(spark, GOLD_BASE_PATH)

        (
            target.alias("t")
            .merge(
                agg_df.alias("s"),
                "t.trip_date_key = s.trip_date_key AND t.driver_id = s.driver_id"
            )
            .whenMatchedUpdate(
                condition="s.max_raw_loaded_at > t.max_raw_loaded_at",
                set={
                    "trip_date": "s.trip_date",
                    "trips_count": "s.trips_count",
                    "completed_trips": "s.completed_trips",
                    "canceled_trips": "s.canceled_trips",
                    "unique_passengers": "s.unique_passengers",
                    "total_fare_amount": "s.total_fare_amount",
                    "total_distance_km": "s.total_distance_km",
                    "max_raw_loaded_at": "s.max_raw_loaded_at",
                    "dwh_loaded_at": "s.dwh_loaded_at",
                }
            )
            .whenNotMatchedInsert(values={
                "trip_date_key": "s.trip_date_key",
                "trip_date": "s.trip_date",
                "driver_id": "s.driver_id",
                "trips_count": "s.trips_count",
                "completed_trips": "s.completed_trips",
                "canceled_trips": "s.canceled_trips",
                "unique_passengers": "s.unique_passengers",
                "total_fare_amount": "s.total_fare_amount",
                "total_distance_km": "s.total_distance_km",
                "max_raw_loaded_at": "s.max_raw_loaded_at",
                "dwh_loaded_at": "s.dwh_loaded_at",
            })
            .execute()
        )

        print(f"[{JOB_NAME}] agg_driver_daily MERGE completed at: {GOLD_BASE_PATH}")
        spark.stop()

    except Exception:
        spark.stop()
        raise


if __name__ == "__main__":
    main()