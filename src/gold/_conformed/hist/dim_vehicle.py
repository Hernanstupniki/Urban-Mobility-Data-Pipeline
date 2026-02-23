import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, row_number, lit, max as spark_max,
    current_timestamp, coalesce, sha2, concat_ws
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Config
JOB_NAME = "dim_vehicle_hist_build_gold_conformed"

ENV = os.getenv("ENV", "dev")

SILVER_BASE_PATH = f"data/{ENV}/silver/vehicles"
GOLD_BASE_PATH = f"data/{ENV}/gold/_conformed/hist/dim_vehicle_hist"


# Helpers
def delta_exists(spark, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def read_target_watermark(spark) -> datetime:
    """
    Watermark = max(raw_loaded_at) in target dim_vehicle_hist (SCD2)
    If target doesn't exist -> 1970-01-01
    """
    if not delta_exists(spark, GOLD_BASE_PATH):
        return datetime(1970, 1, 1)

    df = spark.read.format("delta").load(GOLD_BASE_PATH)
    if df.rdd.isEmpty():
        return datetime(1970, 1, 1)

    ts = df.select(spark_max(col("raw_loaded_at")).alias("wm")).first()["wm"]
    return ts or datetime(1970, 1, 1)


def ensure_scd_hash_if_missing(df, business_cols):
    """
    Si viene scd_hash desde Silver, lo dejamos.
    Si no existe, lo calculamos con business_cols.
    """
    if "scd_hash" in df.columns:
        return df

    parts = [coalesce(col(c).cast("string"), lit("")) for c in business_cols if c in df.columns]
    return df.withColumn("scd_hash", sha2(concat_ws("||", *parts), 256))


def ensure_scd2_fields(df):
    """
    Garantiza columnas SCD2 mínimas para Gold hist:
    - valid_from (timestamp)
    - valid_to (timestamp nullable)
    - is_current (boolean)
    """
    if "valid_from" not in df.columns:
        df = df.withColumn("valid_from", col("raw_loaded_at"))
    if "valid_to" not in df.columns:
        df = df.withColumn("valid_to", lit(None).cast("timestamp"))
    if "is_current" not in df.columns:
        df = df.withColumn("is_current", lit(True))
    return df


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

    try:
        # 2) Read Silver (SCD2 ya armado en Silver)
        silver_all = spark.read.format("delta").load(SILVER_BASE_PATH)

        if "vehicle_id" not in silver_all.columns:
            raise ValueError("vehicle_id not found in silver/vehicles schema")

        # --- FIRST RUN (seed full history) ---
        if not target_exists:
            seed_df = silver_all

            business_cols_guess = [c for c in [
                "vehicle_id",
                "driver_id",
                "plate_number",
                "vehicle_type",
                "make",
                "model",
                "year",
                "status",
                "source_system"
            ] if c in seed_df.columns]

            seed_df = ensure_scd_hash_if_missing(seed_df, business_cols_guess)
            seed_df = ensure_scd2_fields(seed_df)

            if "dwh_loaded_at" not in seed_df.columns:
                seed_df = seed_df.withColumn("dwh_loaded_at", current_timestamp())

            seed_count = seed_df.count()
            print(f"[{JOB_NAME}] silver seed count (all history): {seed_count}")

            (
                seed_df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(GOLD_BASE_PATH)
            )

            print(f"[{JOB_NAME}] dim_vehicle_hist created at: {GOLD_BASE_PATH}")
            spark.stop()
            return

        # --- INCREMENTAL RUNS (derive changes from current snapshot) ---
        silver_df = silver_all
        if "is_current" in silver_df.columns:
            silver_df = silver_df.filter(col("is_current") == lit(True))

        # 3) Incremental filter
        silver_df = silver_df.filter(col("raw_loaded_at") > lit(wm))

        inc_count = silver_df.count()
        print(f"[{JOB_NAME}] silver incremental count: {inc_count}")

        if inc_count == 0:
            print("No new silver records to process")
            spark.stop()
            return

        # 4) Latest per vehicle_id inside incremental batch
        w = Window.partitionBy("vehicle_id").orderBy(col("raw_loaded_at").desc())
        latest_df = (
            silver_df
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        # 5) Ensure SCD2 + hash + audit
        business_cols_guess = [c for c in [
            "vehicle_id",
            "driver_id",
            "plate_number",
            "vehicle_type",
            "make",
            "model",
            "year",
            "status",
            "source_system"
        ] if c in latest_df.columns]

        dim_df = ensure_scd_hash_if_missing(latest_df, business_cols_guess)
        dim_df = ensure_scd2_fields(dim_df)
        dim_df = dim_df.withColumn("dwh_loaded_at", current_timestamp())

        target = DeltaTable.forPath(spark, GOLD_BASE_PATH)

        # 6) SCD2 step 1: close current version if changed
        (
            target.alias("t")
            .merge(
                dim_df.alias("s"),
                "t.vehicle_id = s.vehicle_id AND t.is_current = true"
            )
            .whenMatchedUpdate(
                condition="s.raw_loaded_at > t.raw_loaded_at AND s.scd_hash <> t.scd_hash",
                set={
                    "valid_to": "s.raw_loaded_at",
                    "is_current": "false"
                }
            )
            .execute()
        )

        # 7) SCD2 step 2: insert new version (MANUAL estilo tu Silver 7.2)
        insert_vals_full = {
            "vehicle_id": "s.vehicle_id",
            "driver_id": "s.driver_id",
            "plate_number": "s.plate_number",
            "vehicle_type": "s.vehicle_type",
            "make": "s.make",
            "model": "s.model",
            "year": "s.year",
            "status": "s.status",
            "created_at": "s.created_at",
            "updated_at": "s.updated_at",

            "batch_id": "s.batch_id",
            "source_system": "s.source_system",
            "raw_loaded_at": "s.raw_loaded_at",

            # enrichment flags
            "missing_plate_number": "s.missing_plate_number",
            "missing_vehicle_type": "s.missing_vehicle_type",
            "invalid_vehicle_type": "s.invalid_vehicle_type",
            "missing_driver_id": "s.missing_driver_id",
            "invalid_year": "s.invalid_year",
            "invalid_status": "s.invalid_status",

            # SCD2
            "scd_hash": "s.scd_hash",
            "valid_from": "s.valid_from",
            "valid_to": "CAST(NULL AS TIMESTAMP)",
            "is_current": "true",

            # audit
            "dwh_loaded_at": "s.dwh_loaded_at",
        }

        # Insert only existing columns (in case the schema changes)
        insert_vals = {}
        dim_cols = set(dim_df.columns)
        for k, expr in insert_vals_full.items():
            if k not in dim_cols:
                continue
            if expr.startswith("s."):
                src_col = expr[2:]
                if src_col in dim_cols:
                    insert_vals[k] = expr
            else:
                insert_vals[k] = expr

        (
            target.alias("t")
            .merge(
                dim_df.alias("s"),
                "t.vehicle_id = s.vehicle_id AND t.is_current = true"
            )
            .whenNotMatchedInsert(values=insert_vals)
            .execute()
        )

        print(f"[{JOB_NAME}] dim_vehicle_hist SCD2 MERGE completed at: {GOLD_BASE_PATH}")
        spark.stop()

    except Exception:
        spark.stop()
        raise


if __name__ == "__main__":
    main()
