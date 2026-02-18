"""
Gold Build – Conformed Dim: dim_payment
Incremental SCD1 snapshot (like dim_passenger)
Business key: payment_method_key (derived from payments.method)
Source: Silver payments (is_current=true)
"""

import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, row_number, lit, lower, trim, when, length,
    current_timestamp, max as spark_max,
    xxhash64, pmod, coalesce
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Config
JOB_NAME = "dim_payment_build_gold_conformed"

ENV = os.getenv("ENV", "dev")

SILVER_BASE_PATH = f"data/{ENV}/silver/payments"
GOLD_BASE_PATH = f"data/{ENV}/gold/_conformed/dim_payment"

NULL_LIKES = ["null", "n/a", "none", "-", ""]


def read_target_watermark(spark) -> datetime:
    """
    Watermark = max(raw_loaded_at) in target dim_payment
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

    target_exists = DeltaTable.isDeltaTable(spark, GOLD_BASE_PATH)

    # 1) Watermark from target
    wm = read_target_watermark(spark)
    print(f"[{JOB_NAME}] target watermark (max raw_loaded_at): {wm}")

    # 2) Read Silver (current snapshot only)
    if not DeltaTable.isDeltaTable(spark, SILVER_BASE_PATH):
        raise RuntimeError(f"[{JOB_NAME}] Silver table not found at: {SILVER_BASE_PATH}")

    payments = (
        spark.read.format("delta").load(SILVER_BASE_PATH)
        .filter(col("is_current") == lit(True))
    )

    # 3) Incremental filter (only if target exists)
    if target_exists:
        payments = payments.filter(col("raw_loaded_at") > lit(wm))

    inc_payments_count = payments.count()
    print(f"[{JOB_NAME}] silver incremental count (payments rows): {inc_payments_count}")

    if inc_payments_count == 0:
        print("No new silver records to process")
        spark.stop()
        return

    # 4) Build incremental dim rows from the incremental payments batch
    # Normalize method
    base = (
        payments
        .select(
            lower(trim(col("method"))).alias("method"),
            col("raw_loaded_at").cast("timestamp").alias("raw_loaded_at")
        )
        .withColumn(
            "method",
            when(col("method").isNull(), lit(None))
            .when(col("method").isin(NULL_LIKES), lit(None))
            .when(length(col("method")) == 0, lit(None))
            .otherwise(col("method"))
        )
        .filter(col("method").isNotNull())
    )

    # Deterministic key (0 reserved for UNKNOWN) -> 1..2147483646
    dim_inc = (
        base
        .withColumn("payment_method_key", (pmod(xxhash64(col("method")), lit(2147483646)) + lit(1)).cast("int"))
        .withColumn("payment_method_name", col("method"))
        .withColumn("is_cash", col("method").isin("cash", "efectivo"))
        .withColumn(
            "is_card",
            col("method").contains("card") |
            col("method").contains("credit") |
            col("method").contains("debit")
        )
        .select(
            "payment_method_key",
            "payment_method_name",
            "is_cash",
            "is_card",
            "raw_loaded_at"
        )
    )

    # 5) Latest per payment_method_key inside incremental batch
    w = Window.partitionBy("payment_method_key").orderBy(col("raw_loaded_at").desc())
    latest_df = (
        dim_inc
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # 6) Add DWH audit
    dim_df = latest_df.withColumn("dwh_loaded_at", current_timestamp())

    dim_inc_count = dim_df.count()
    print(f"[{JOB_NAME}] dim incremental distinct methods: {dim_inc_count}")

    if dim_inc_count == 0:
        print("No new dim rows to process after method normalization")
        spark.stop()
        return

    # 7) First run -> create (include UNKNOWN row)
    if not target_exists:
        unknown = (
            spark.createDataFrame(
                [(0, "UNKNOWN", False, False, datetime(1970, 1, 1))],
                "payment_method_key int, payment_method_name string, is_cash boolean, is_card boolean, raw_loaded_at timestamp"
            )
            .withColumn("dwh_loaded_at", current_timestamp())
        )

        final_df = unknown.unionByName(dim_df, allowMissingColumns=True)

        (
            final_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(GOLD_BASE_PATH)
        )

        print(f"[{JOB_NAME}] dim_payment created at: {GOLD_BASE_PATH}")
        spark.stop()
        return

    # 8) Incremental MERGE (SCD1 snapshot)
    target = DeltaTable.forPath(spark, GOLD_BASE_PATH)

    cols = dim_df.columns
    if "payment_method_key" not in cols:
        raise ValueError("payment_method_key not found in incremental dim schema")

    update_set = {c: f"s.{c}" for c in cols if c != "payment_method_key"}
    insert_vals = {c: f"s.{c}" for c in cols}

    (
        target.alias("t")
        .merge(dim_df.alias("s"), "t.payment_method_key = s.payment_method_key")
        .whenMatchedUpdate(
            condition="s.raw_loaded_at > t.raw_loaded_at",
            set=update_set
        )
        .whenNotMatchedInsert(values=insert_vals)
        .execute()
    )

    print(f"[{JOB_NAME}] dim_payment MERGE completed at: {GOLD_BASE_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
