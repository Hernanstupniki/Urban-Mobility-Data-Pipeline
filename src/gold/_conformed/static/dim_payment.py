"""
Gold Build – Conformed Dim: dim_payment_method (STATIC rebuild)
Source: Silver payments (is_current=true)
Business key: payment_method_key (derived from payments.method)
"""

import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, lower, trim, when, length,
    current_timestamp, max as spark_max,
    xxhash64, pmod
)
from delta.tables import DeltaTable

# Config
JOB_NAME = "dim_payment_method_static_build_gold_conformed"

ENV = os.getenv("ENV", "dev")

SILVER_BASE_PATH = f"data/{ENV}/silver/payments"
GOLD_BASE_PATH = f"data/{ENV}/gold/_conformed/static/dim_payment_method"

NULL_LIKES = ["null", "n/a", "none", "-", ""]


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

    try:
        if not DeltaTable.isDeltaTable(spark, SILVER_BASE_PATH):
            raise RuntimeError(f"[{JOB_NAME}] Silver table not found at: {SILVER_BASE_PATH}")

        # 1) Read Silver (current snapshot only)
        payments = (
            spark.read.format("delta").load(SILVER_BASE_PATH)
            .filter(col("is_current") == lit(True))
        )

        silver_count = payments.count()
        print(f"[{JOB_NAME}] silver current rows (is_current=true): {silver_count}")

        if silver_count == 0:
            print("No silver rows to process")
            spark.stop()
            return

        # 2) Normalize method + keep raw_loaded_at
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

        # 3) One row per method + “method_last_seen_at”
        methods = (
            base.groupBy("method")
            .agg(spark_max(col("raw_loaded_at")).alias("raw_loaded_at"))
        )

        distinct_methods = methods.count()
        print(f"[{JOB_NAME}] distinct payment methods (normalized): {distinct_methods}")

        if distinct_methods == 0:
            print("No valid payment methods after normalization")
            spark.stop()
            return

        # 4) Deterministic key (0 reserved for UNKNOWN) -> 1..2147483646
        dim = (
            methods
            .withColumn(
                "payment_method_key",
                (pmod(xxhash64(col("method")), lit(2147483646)) + lit(1)).cast("int")
            )
            .withColumn("payment_method_name", col("method"))
            .withColumn("is_cash", col("method").isin("cash", "efectivo"))
            .withColumn(
                "is_card",
                col("method").contains("card") |
                col("method").contains("credit") |
                col("method").contains("debit")
            )
            .withColumn("dwh_loaded_at", current_timestamp())
            .select(
                "payment_method_key",
                "payment_method_name",
                "is_cash",
                "is_card",
                "raw_loaded_at",
                "dwh_loaded_at"
            )
        )

        # 5) UNKNOWN row
        unknown = (
            spark.createDataFrame(
                [(0, "UNKNOWN", False, False, datetime(1970, 1, 1))],
                "payment_method_key int, payment_method_name string, is_cash boolean, is_card boolean, raw_loaded_at timestamp"
            )
            .withColumn("dwh_loaded_at", current_timestamp())
        )

        final_df = unknown.unionByName(dim, allowMissingColumns=True)

        out_count = final_df.count()
        print(f"[{JOB_NAME}] output rows (including UNKNOWN): {out_count}")

        # 6) Write Gold (STATIC overwrite always)
        (
            final_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(GOLD_BASE_PATH)
        )

        print(f"[{JOB_NAME}] dim_payment_method rebuilt successfully at: {GOLD_BASE_PATH}")
        spark.stop()

    except Exception:
        spark.stop()
        raise


if __name__ == "__main__":
    main()