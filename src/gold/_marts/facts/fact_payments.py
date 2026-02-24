"""
Gold Mart – Fact: fact_payments
Incremental SCD1 snapshot (1 row per payment_id)
Star schema:
- payment_method_key validated against gold/_conformed/static/dim_payment_method (0 = UNKNOWN)

Source: Silver payments (is_current=true)
"""

import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, row_number, current_timestamp,
    max as spark_max, to_date, date_format, coalesce,
    lower, trim, when, length,
    xxhash64, pmod
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Config
JOB_NAME = "fact_payments_build_gold_marts"

ENV = os.getenv("ENV", "dev")

SILVER_BASE_PATH = f"data/{ENV}/silver/payments"
GOLD_BASE_PATH = f"data/{ENV}/gold/_marts/facts/fact_payments"

DIM_PAYMENT_METHOD_PATH = f"data/{ENV}/gold/_conformed/static/dim_payment_method"

NULL_LIKES = ["null", "n/a", "none", "-", ""]


def delta_exists(spark, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def read_target_watermark(spark) -> datetime:
    """
    Watermark = max(raw_loaded_at) in target fact_payments
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

    if "payment_id" not in silver_df.columns:
        raise ValueError("payment_id not found in silver/payments schema")

    # 3) Incremental filter
    if target_exists:
        silver_df = silver_df.filter(col("raw_loaded_at") > lit(wm))

    inc_count = silver_df.count()
    print(f"[{JOB_NAME}] silver incremental count: {inc_count}")

    if inc_count == 0:
        print("No new silver records to process")
        spark.stop()
        return

    # 4) Latest per payment_id inside incremental batch
    w = Window.partitionBy("payment_id").orderBy(col("raw_loaded_at").desc())
    latest_df = (
        silver_df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # 5) payment_method_key + payment_date_key + audit
    method_norm = lower(trim(col("method")))

    fact_df = (
        latest_df
        .withColumn("payment_id", col("payment_id").cast("long"))
        .withColumn("trip_id", col("trip_id").cast("long") if "trip_id" in latest_df.columns else col("trip_id"))
        .withColumn("method_norm", method_norm)
        .withColumn(
            "method_norm",
            when(col("method_norm").isNull(), lit(None))
            .when(col("method_norm").isin(NULL_LIKES), lit(None))
            .when(length(col("method_norm")) == 0, lit(None))
            .otherwise(col("method_norm"))
        )
        .withColumn(
            "payment_method_key",
            when(col("method_norm").isNull(), lit(0).cast("int"))
            .otherwise((pmod(xxhash64(col("method_norm")), lit(2147483646)) + lit(1)).cast("int"))
        )
        .withColumn(
            "payment_date_key",
            coalesce(
                date_format(to_date(coalesce(col("paid_at"), col("created_at"), col("raw_loaded_at"))), "yyyyMMdd").cast("int"),
                lit(0).cast("int")
            )
        )
        .withColumn("dwh_loaded_at", current_timestamp())
        .drop("method_norm")
    )

    # 5.1) VALIDATE payment_method_key against dim_payment_method (static)
    if delta_exists(spark, DIM_PAYMENT_METHOD_PATH):
        dim_pm = (
            spark.read.format("delta").load(DIM_PAYMENT_METHOD_PATH)
            .select(col("payment_method_key").cast("int").alias("pmk"))
            .dropDuplicates(["pmk"])
        )

        fact_df = (
            fact_df
            .join(dim_pm, fact_df["payment_method_key"] == dim_pm["pmk"], "left")
            .withColumn("payment_method_key", when(col("pmk").isNull(), lit(0)).otherwise(col("payment_method_key")))
            .drop("pmk")
        )
    else:
        print(f"[{JOB_NAME}] WARN: dim_payment_method not found at {DIM_PAYMENT_METHOD_PATH} (keeping computed keys)")

    # 6) First run -> create
    if not target_exists:
        (
            fact_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(GOLD_BASE_PATH)
        )
        print(f"[{JOB_NAME}] fact_payments created at: {GOLD_BASE_PATH}")
        spark.stop()
        return

    # 7) Incremental MERGE (SCD1 snapshot)
    target = DeltaTable.forPath(spark, GOLD_BASE_PATH)

    cols = fact_df.columns
    update_set = {c: f"s.{c}" for c in cols if c != "payment_id"}
    insert_vals = {c: f"s.{c}" for c in cols}

    (
        target.alias("t")
        .merge(fact_df.alias("s"), "t.payment_id = s.payment_id")
        .whenMatchedUpdate(
            condition="s.raw_loaded_at > t.raw_loaded_at",
            set=update_set
        )
        .whenNotMatchedInsert(values=insert_vals)
        .execute()
    )

    print(f"[{JOB_NAME}] fact_payments MERGE completed at: {GOLD_BASE_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()