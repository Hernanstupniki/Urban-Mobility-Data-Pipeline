import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, trim, lower, when, current_timestamp,
    row_number
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Config
JOB_NAME = "dim_zone_static_build_gold_conformed"

ENV = os.getenv("ENV", "dev")
SILVER_BASE_PATH = f"data/{ENV}/silver/zones"
GOLD_BASE_PATH = f"data/{ENV}/gold/_conformed/static/dim_zone"


def main():
    spark = (
        SparkSession.builder
        .appName(JOB_NAME)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # DEV tuning (igual a tus scripts)
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
            print(f"[{JOB_NAME}] Silver zones table not found at: {SILVER_BASE_PATH}")
            spark.stop()
            return

        # 1) Read Silver (FULL REBUILD ALWAYS)
        silver_df = spark.read.format("delta").load(SILVER_BASE_PATH)

        # Safety cast (por si algún día viene como string)
        if "is_current" in silver_df.columns:
            silver_df = silver_df.withColumn("is_current", col("is_current").cast("boolean"))

        # 2) Only current rows (si existe la columna)
        current_df = silver_df
        if "is_current" in silver_df.columns:
            current_df = silver_df.filter(col("is_current") == lit(True))

        # 3) Ensure 1 row per zone_id (pick latest by raw_loaded_at just in case)
        if "zone_id" not in current_df.columns:
            raise ValueError("zone_id not found in silver/zones schema")

        # desc_nulls_last() puede variar según versión; orden simple y listo
        w = Window.partitionBy("zone_id").orderBy(col("raw_loaded_at").desc())
        current_df = (
            current_df
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        count_current = current_df.count()
        print(f"[{JOB_NAME}] current silver rows (is_current=true): {count_current}")

        if count_current == 0:
            print("No current zones found in Silver (nothing to rebuild)")
            spark.stop()
            return

        # 4) Conformed dimension (clean + consistent fields)
        # (si *_norm existe en Silver, lo mantenemos; si no, lo regeneramos)
        dim_df = (
            current_df
            .withColumn("zone_id", col("zone_id").cast("long"))
            .withColumn("zone_name", when(trim(col("zone_name")) == lit(""), lit(None)).otherwise(col("zone_name")))
            .withColumn("city", when(trim(col("city")) == lit(""), lit(None)).otherwise(col("city")))
            .withColumn("region", when(trim(col("region")) == lit(""), lit(None)).otherwise(col("region")))
            .withColumn(
                "zone_name_norm",
                when(col("zone_name_norm").isNotNull(), col("zone_name_norm"))
                .otherwise(lower(trim(col("zone_name"))))
            )
            .withColumn(
                "city_norm",
                when(col("city_norm").isNotNull(), col("city_norm"))
                .otherwise(lower(trim(col("city"))))
            )
            .withColumn(
                "region_norm",
                when(col("region_norm").isNotNull(), col("region_norm"))
                .otherwise(lower(trim(col("region"))))
            )
            .withColumn("dwh_loaded_at", current_timestamp())
        )

        # 5) Select final schema (limpio para conformed)
        # (solo seleccionamos flags si existen)
        base_cols = [
            "zone_id",
            "zone_name", "city", "region",
            "zone_name_norm", "city_norm", "region_norm",
            "created_at", "raw_loaded_at", "source_system",
            "dwh_loaded_at",
        ]

        flag_cols = [c for c in ["zone_name_is_null", "city_is_null", "region_is_null", "has_missing_fields"] if c in dim_df.columns]

        dim_df = dim_df.select(*[c for c in base_cols if c in dim_df.columns], *flag_cols)

        out_count = dim_df.count()
        print(f"[{JOB_NAME}] output rows: {out_count}")

        # 6) Write Gold (FULL OVERWRITE)
        (
            dim_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(GOLD_BASE_PATH)
        )

        print(f"[{JOB_NAME}] dim_zone rebuilt successfully at: {GOLD_BASE_PATH}")
        spark.stop()

    except Exception:
        spark.stop()
        raise


if __name__ == "__main__":
    main()