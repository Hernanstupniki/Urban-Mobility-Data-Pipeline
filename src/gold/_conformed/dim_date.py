"""
Gold Build – Conformed Dim: dim_date
"""

import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date,
    to_date, date_sub, date_add, coalesce,
    min as spark_min, max as spark_max,
    explode, sequence, date_format, year, month, dayofmonth,
    weekofyear, quarter, dayofweek, when
)
from delta.tables import DeltaTable

# ============================================================
# Config
# ============================================================
JOB_NAME = "dim_date_build_gold_conformed"

ENV = os.getenv("ENV", "dev")

GOLD_BASE_PATH = f"data/{ENV}/gold/_conformed/dim_date"

CONTROL_BASE_PATH = f"data/{ENV}/_control"
ETL_CONTROL_PATH = f"{CONTROL_BASE_PATH}/etl_control"

# Inputs (optional) to infer date range from Silver
SILVER_TRIPS_PATH = f"data/{ENV}/silver/trips"
SILVER_PAYMENTS_PATH = f"data/{ENV}/silver/payments"
SILVER_RATINGS_PATH = f"data/{ENV}/silver/ratings"

# Date range controls (override if you want)
DATE_START = os.getenv("DATE_START")  # e.g. "2025-01-01"
DATE_END = os.getenv("DATE_END")      # e.g. "2026-12-31"

PAD_DAYS_BEFORE = int(os.getenv("PAD_DAYS_BEFORE", "30"))
PAD_DAYS_AFTER = int(os.getenv("PAD_DAYS_AFTER", "30"))

# Fallback window if cannot infer from Silver
FALLBACK_DAYS_BACK = int(os.getenv("FALLBACK_DAYS_BACK", "365"))
FALLBACK_DAYS_FORWARD = int(os.getenv("FALLBACK_DAYS_FORWARD", "365"))


# ============================================================
# Delta control helpers (same pattern as your jobs)
# ============================================================
def ensure_etl_control_table(spark: SparkSession):
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


def upsert_etl_control(spark: SparkSession, job_name: str, last_loaded_ts, status: str):
    """
    Upsert in Delta:
    - If last_loaded_ts is None(FAIL), DO NOT step on the previous watermark.
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


# ============================================================
# Helpers
# ============================================================
def _min_max_date_from_df(df, ts_col: str):
    # returns (min_date, max_date) as python dates (or None, None)
    row = (
        df.select(
            spark_min(to_date(col(ts_col))).alias("min_d"),
            spark_max(to_date(col(ts_col))).alias("max_d")
        )
        .first()
    )
    return row["min_d"], row["max_d"]


def infer_date_range_from_silver(spark: SparkSession):
    mins = []
    maxs = []

    # trips: use requested_at as "business date spine"
    if DeltaTable.isDeltaTable(spark, SILVER_TRIPS_PATH):
        trips = spark.read.format("delta").load(SILVER_TRIPS_PATH).filter(col("is_current") == lit(True))
        mn, mx = _min_max_date_from_df(trips, "requested_at")
        if mn is not None: mins.append(mn)
        if mx is not None: maxs.append(mx)

    # payments: prefer paid_at else created_at
    if DeltaTable.isDeltaTable(spark, SILVER_PAYMENTS_PATH):
        p = spark.read.format("delta").load(SILVER_PAYMENTS_PATH).filter(col("is_current") == lit(True))
        p2 = p.select(to_date(coalesce(col("paid_at"), col("created_at"), col("raw_loaded_at"))).alias("d"))
        row = p2.select(spark_min(col("d")).alias("min_d"), spark_max(col("d")).alias("max_d")).first()
        if row["min_d"] is not None: mins.append(row["min_d"])
        if row["max_d"] is not None: maxs.append(row["max_d"])

    # ratings: created_at
    if DeltaTable.isDeltaTable(spark, SILVER_RATINGS_PATH):
        r = spark.read.format("delta").load(SILVER_RATINGS_PATH).filter(col("is_current") == lit(True))
        mn, mx = _min_max_date_from_df(r, "created_at")
        if mn is not None: mins.append(mn)
        if mx is not None: maxs.append(mx)

    if len(mins) == 0 or len(maxs) == 0:
        return None, None

    return min(mins), max(maxs)


# ============================================================
# Main
# ============================================================
def main():
    spark = (
        SparkSession.builder
        .appName(JOB_NAME)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # DEV tuning (same spirit as yours)
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "64MB")

    try:
        # 1) Resolve date range
        if DATE_START and DATE_END:
            start_d = datetime.strptime(DATE_START, "%Y-%m-%d").date()
            end_d = datetime.strptime(DATE_END, "%Y-%m-%d").date()
        else:
            inferred_start, inferred_end = infer_date_range_from_silver(spark)
            if inferred_start is None or inferred_end is None:
                # fallback window if Silver not present / empty
                start_d = spark.range(1).select(date_sub(current_date(), lit(FALLBACK_DAYS_BACK)).alias("d")).first()["d"]
                end_d = spark.range(1).select(date_add(current_date(), lit(FALLBACK_DAYS_FORWARD)).alias("d")).first()["d"]
            else:
                start_d = inferred_start
                end_d = inferred_end

        # padding
        start_d = (spark.createDataFrame([(str(start_d),)], "s string")
                   .select(date_sub(to_date(col("s")), lit(PAD_DAYS_BEFORE)).alias("d")).first()["d"])
        end_d = (spark.createDataFrame([(str(end_d),)], "s string")
                 .select(date_add(to_date(col("s")), lit(PAD_DAYS_AFTER)).alias("d")).first()["d"])

        print(f"[{JOB_NAME}] date range: {start_d} -> {end_d}")

        # 2) Build calendar spine
        base = (
            spark.range(1)
            .select(explode(sequence(lit(start_d), lit(end_d))).alias("date"))
        )

        dim = (
            base
            .withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))
            .withColumn("year", year(col("date")))
            .withColumn("month", month(col("date")))
            .withColumn("day", dayofmonth(col("date")))
            .withColumn("week_of_year", weekofyear(col("date")))
            .withColumn("quarter", quarter(col("date")))
            .withColumn("day_name", date_format(col("date"), "EEEE"))
            # Spark: dayofweek -> 1=Sunday .. 7=Saturday
            .withColumn("is_weekend", dayofweek(col("date")).isin(1, 7))
            .withColumn("gold_loaded_at", current_timestamp())
        )

        # 3) Add UNKNOWN row
        unknown = (
            spark.createDataFrame(
                [(0, datetime(1900, 1, 1).date(), 1900, 1, 1, 1, 1, "UNKNOWN", True)],
                "date_key int, date date, year int, month int, day int, week_of_year int, quarter int, day_name string, is_weekend boolean"
            )
            .withColumn("gold_loaded_at", current_timestamp())
        )

        final_df = unknown.unionByName(dim, allowMissingColumns=True)

        # 4) Write Gold (DEV-style overwrite)
        (
            final_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(GOLD_BASE_PATH)
        )

        # 5) Update control (use end date as last_loaded_ts at midnight)
        end_ts = spark.createDataFrame([(str(end_d),)], "d string").select(to_date(col("d")).cast("timestamp").alias("ts")).first()["ts"]
        upsert_etl_control(spark, JOB_NAME, end_ts, "SUCCESS")

        print(f"[{JOB_NAME}] dim_date created at: {GOLD_BASE_PATH}")
        spark.stop()

    except Exception as e:
        try:
            upsert_etl_control(spark, JOB_NAME, None, f"FAIL: {type(e).__name__}")
        except Exception:
            pass
        spark.stop()
        raise


if __name__ == "__main__":
    main()
