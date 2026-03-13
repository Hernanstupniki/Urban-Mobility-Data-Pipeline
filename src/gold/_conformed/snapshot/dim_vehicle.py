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
ANON_PLATE_PREFIX = os.getenv("ANON_PLATE_PREFIX", "ANON-PLATE-")


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


def apply_gdpr_backfill_snapshot(target: DeltaTable, gdpr_events_df):
    if gdpr_events_df.rdd.isEmpty():
        return

    cols = set(target.toDF().columns)
    set_map = {}

    def set_if_exists(column: str, expr: str):
        if column in cols:
            set_map[column] = expr

    anon_expr = f"concat('{ANON_PLATE_PREFIX}', cast(g.vehicle_id as string))"
    if "plate_number" in cols:
        set_map["plate_number"] = anon_expr
    set_if_exists("is_deleted", "true")
    deleted_at_expr = "coalesce(g.deleted_at, t.deleted_at, current_timestamp())"
    if "deleted_at" in cols:
        set_map["deleted_at"] = deleted_at_expr
    set_if_exists("updated_at", "current_timestamp()")
    set_if_exists("missing_plate_number", "false")

    if not set_map:
        return

    (
        target.alias("t")
        .merge(gdpr_events_df.alias("g"), "t.vehicle_id = g.vehicle_id")
        .whenMatchedUpdate(set=set_map)
        .execute()
    )

    print(f"[{JOB_NAME}] GDPR snapshot backfill applied to dim_vehicle.")


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

    try:
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
            return

        # 7) Incremental MERGE (SCD1 snapshot)
        target = DeltaTable.forPath(spark, GOLD_BASE_PATH)

        cols = dim_df.columns
        update_set = {c: f"s.{c}" for c in cols if c != "vehicle_id"}
        insert_vals = {c: f"s.{c}" for c in cols}

        # ✅ Mejora: si existe scd_hash, solo actualizar si cambió el hash
        merge_condition = "s.raw_loaded_at > t.raw_loaded_at"
        if "scd_hash" in cols:
            merge_condition += " AND s.scd_hash <> t.scd_hash"

        (
            target.alias("t")
            .merge(dim_df.alias("s"), "t.vehicle_id = s.vehicle_id")
            .whenMatchedUpdate(
                condition=merge_condition,
                set=update_set
            )
            .whenNotMatchedInsert(values=insert_vals)
            .execute()
        )

        if "is_deleted" in dim_df.columns:
            gdpr_events_df = (
                dim_df
                .filter(col("is_deleted") == lit(True))
                .select(
                    col("vehicle_id").alias("vehicle_id"),
                    col("deleted_at").alias("deleted_at")
                )
                .dropDuplicates(["vehicle_id"])
            )
            apply_gdpr_backfill_snapshot(target, gdpr_events_df)

        print(f"[{JOB_NAME}] dim_vehicle MERGE completed at: {GOLD_BASE_PATH}")

    except Exception:
        print(f"[{JOB_NAME}] FAILED snapshot build")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
