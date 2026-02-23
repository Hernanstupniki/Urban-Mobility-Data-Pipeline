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
JOB_NAME = "dim_driver_scd3_build_gold_conformed"

ENV = os.getenv("ENV", "dev")

SILVER_BASE_PATH = f"data/{ENV}/silver/drivers"
GOLD_BASE_PATH = f"data/{ENV}/gold/_conformed/scd3/dim_driver"


# Helpers
def delta_exists(spark, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def read_target_watermark(spark) -> datetime:
    """
    Watermark = max(raw_loaded_at) in target dim_driver (SCD3)
    If target doesn't exist -> 1970-01-01
    """
    if not delta_exists(spark, GOLD_BASE_PATH):
        return datetime(1970, 1, 1)

    df = spark.read.format("delta").load(GOLD_BASE_PATH)
    if df.rdd.isEmpty():
        return datetime(1970, 1, 1)

    ts = df.select(spark_max(col("raw_loaded_at")).alias("wm")).first()["wm"]
    return ts or datetime(1970, 1, 1)


def drop_scd2_cols(df):
    drop_cols = [c for c in ["valid_to", "is_current"] if c in df.columns]
    return df.drop(*drop_cols) if drop_cols else df


def add_prev_null_columns(df, key_col: str):
    for c in df.columns:
        if c == key_col:
            continue
        prev_c = f"prev_{c}"
        if prev_c in df.columns:
            continue
        df = df.withColumn(prev_c, lit(None).cast(df.schema[c].dataType))
    return df


def build_seed_scd3(silver_all, key_col: str):
    base = drop_scd2_cols(silver_all)
    order_col = "valid_from" if "valid_from" in base.columns else "raw_loaded_at"

    w = Window.partitionBy(key_col).orderBy(col(order_col).desc())
    ranked = base.withColumn("rn", row_number().over(w))

    current_df = ranked.filter(col("rn") == 1).drop("rn")
    prev_df = ranked.filter(col("rn") == 2).drop("rn")

    prev_select = [col(key_col)]
    for c in prev_df.columns:
        if c == key_col:
            continue
        prev_select.append(col(c).alias(f"prev_{c}"))

    prev_pref = prev_df.select(*prev_select)

    return (
        current_df.alias("c")
        .join(prev_pref.alias("p"), on=key_col, how="left")
        .withColumn("dwh_loaded_at", current_timestamp())
    )


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

    wm = read_target_watermark(spark)
    print(f"[{JOB_NAME}] target watermark (max raw_loaded_at): {wm}")

    try:
        silver_all = spark.read.format("delta").load(SILVER_BASE_PATH)

        if "driver_id" not in silver_all.columns:
            raise ValueError("driver_id not found in silver/drivers schema")

        # FIRST RUN
        if not target_exists:
            seed_df = build_seed_scd3(silver_all, "driver_id")

            seed_count = seed_df.count()
            print(f"[{JOB_NAME}] seed count (scd3): {seed_count}")

            (
                seed_df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(GOLD_BASE_PATH)
            )

            print(f"[{JOB_NAME}] dim_driver_scd3 created at: {GOLD_BASE_PATH}")
            spark.stop()
            return

        # INCREMENTAL
        silver_df = silver_all
        if "is_current" in silver_df.columns:
            silver_df = silver_df.filter(col("is_current") == lit(True))

        silver_df = drop_scd2_cols(silver_df)
        silver_df = silver_df.filter(col("raw_loaded_at") > lit(wm))

        inc_count = silver_df.count()
        print(f"[{JOB_NAME}] silver incremental count: {inc_count}")

        if inc_count == 0:
            print("No new silver records to process")
            spark.stop()
            return

        w = Window.partitionBy("driver_id").orderBy(col("raw_loaded_at").desc())
        latest_df = (
            silver_df
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        dim_df = latest_df.withColumn("dwh_loaded_at", current_timestamp())
        dim_df = add_prev_null_columns(dim_df, "driver_id")

        target = DeltaTable.forPath(spark, GOLD_BASE_PATH)

        all_cols = dim_df.columns
        current_cols = [c for c in all_cols if c != "driver_id" and not c.startswith("prev_")]

        update_set = {c: f"s.{c}" for c in current_cols}
        for c in current_cols:
            prev_c = f"prev_{c}"
            if prev_c in all_cols:
                update_set[prev_c] = f"t.{c}"

        set_all = {c: f"s.{c}" for c in all_cols}

        cond = "s.raw_loaded_at > t.raw_loaded_at"
        if "scd_hash" in current_cols:
            cond += " AND s.scd_hash <> t.scd_hash"

        (
            target.alias("t")
            .merge(dim_df.alias("s"), "t.driver_id = s.driver_id")
            .whenMatchedUpdate(condition=cond, set=update_set)
            .whenNotMatchedInsert(values=set_all)
            .execute()
        )

        print(f"[{JOB_NAME}] dim_driver_scd3 MERGE completed at: {GOLD_BASE_PATH}")
        spark.stop()

    except Exception:
        spark.stop()
        raise


if __name__ == "__main__":
    main()