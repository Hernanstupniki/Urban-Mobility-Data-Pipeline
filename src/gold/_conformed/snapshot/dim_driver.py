import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, trim, lower,
    row_number, max as spark_max,
    coalesce, sha2, concat_ws,
    current_timestamp
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Config
JOB_NAME = "dim_driver_snapshot_build_gold_conformed"

ENV = os.getenv("ENV", "dev")

SILVER_BASE_PATH = f"data/{ENV}/silver/drivers"
GOLD_BASE_PATH = f"data/{ENV}/gold/_conformed/snapshot/dim_driver"


# Helpers
def delta_exists(spark, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def read_target_watermark(spark, path: str) -> datetime:
    """
    Watermark = max(raw_loaded_at) in target dim_driver snapshot
    If target doesn't exist -> 1970-01-01
    """
    if not delta_exists(spark, path):
        return datetime(1970, 1, 1)

    df = spark.read.format("delta").load(path)
    ts = df.select(spark_max(col("raw_loaded_at")).alias("wm")).first()["wm"]
    return ts or datetime(1970, 1, 1)


def ensure_scd_hash_if_missing(df, business_cols):
    """
    If scd_hash exists, keep it.
    Else compute it from business_cols.
    """
    if "scd_hash" in df.columns:
        return df

    parts = [coalesce(col(c).cast("string"), lit("")) for c in business_cols]
    return df.withColumn("scd_hash", sha2(concat_ws("||", *parts), 256))


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

    # Delta schema auto-merge (DEV default)
    AUTO_MERGE = os.getenv("DELTA_AUTO_MERGE", "1" if ENV == "dev" else "0") == "1"
    if AUTO_MERGE:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        print("[CONFIG] Delta schema auto-merge: ENABLED")
    else:
        print("[CONFIG] Delta schema auto-merge: DISABLED")

    try:
        if not delta_exists(spark, SILVER_BASE_PATH):
            raise RuntimeError(f"[{JOB_NAME}] Silver table not found at: {SILVER_BASE_PATH}")

        target_exists = delta_exists(spark, GOLD_BASE_PATH)

        # 1) Watermark from target
        wm = read_target_watermark(spark, GOLD_BASE_PATH)
        print(f"[{JOB_NAME}] target watermark (max raw_loaded_at): {wm}")

        # 2) Read Silver (current snapshot only)
        silver_df = (
            spark.read.format("delta").load(SILVER_BASE_PATH)
            .filter(col("is_current") == lit(True))
            .withColumn("driver_id", col("driver_id").cast("long"))
            .withColumn("source_system", trim(col("source_system")))
        )

        if "status" in silver_df.columns:
            silver_df = silver_df.withColumn("status", lower(trim(col("status"))))

        # 3) Incremental filter (only if target exists)
        if target_exists:
            silver_df = silver_df.filter(col("raw_loaded_at") > lit(wm))

        inc_count = silver_df.count()
        print(f"[{JOB_NAME}] silver incremental count: {inc_count}")

        if inc_count == 0:
            print("No new silver records to process")
            spark.stop()
            return

        # 4) Latest per driver_id inside incremental batch (igual que passenger)
        w = Window.partitionBy("driver_id").orderBy(col("raw_loaded_at").desc())
        latest_df = (
            silver_df
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        # 5) scd_hash if missing + DWH audit
        business_cols_guess = [c for c in [
            "driver_id",
            "full_name", "name",
            "email", "phone",
            "license_number", "status",
            "source_system"
        ] if c in latest_df.columns]

        dim_df = ensure_scd_hash_if_missing(latest_df, business_cols_guess)
        dim_df = dim_df.withColumn("dwh_loaded_at", current_timestamp())

        # 6) First run -> create
        if not target_exists:
            (
                dim_df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(GOLD_BASE_PATH)
            )
            print(f"[{JOB_NAME}] dim_driver_snapshot created at: {GOLD_BASE_PATH}")
            spark.stop()
            return

        # 7) Incremental MERGE (SCD1 snapshot)
        target = DeltaTable.forPath(spark, GOLD_BASE_PATH)

        cols = dim_df.columns
        if "driver_id" not in cols:
            raise ValueError("driver_id not found in silver/drivers schema")

        update_set = {c: f"s.{c}" for c in cols if c != "driver_id"}
        insert_vals = {c: f"s.{c}" for c in cols}

        (
            target.alias("t")
            .merge(dim_df.alias("s"), "t.driver_id = s.driver_id")
            .whenMatchedUpdate(
                condition="s.raw_loaded_at > t.raw_loaded_at AND s.scd_hash <> t.scd_hash",
                set=update_set
            )
            .whenNotMatchedInsert(values=insert_vals)
            .execute()
        )

        print(f"[{JOB_NAME}] dim_driver_snapshot MERGE completed at: {GOLD_BASE_PATH}")
        spark.stop()

    except Exception:
        spark.stop()
        raise


if __name__ == "__main__":
    main()
