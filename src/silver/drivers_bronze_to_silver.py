import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, row_number, when, lit, min as spark_min,
    max as spark_max, to_date, current_timestamp, coalesce,
    trim, lower, sha2, concat_ws
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Config
JOB_NAME = "drivers_bronze_to_silver"

ENV = os.getenv("ENV", "dev")
BRONZE_BASE_PATH = f"data/{ENV}/bronze/drivers"
SILVER_BASE_PATH = f"data/{ENV}/silver/drivers"

# Delta control table (watermarks)
CONTROL_BASE_PATH = f"data/{ENV}/_control"
ETL_CONTROL_PATH = f"{CONTROL_BASE_PATH}/etl_control"


# Delta control helpers
def ensure_etl_control_table(spark):
    """
    Create the table control in delta if not exists
    """
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


def read_last_loaded_ts(spark) -> datetime:
    """
    Read watermark(last_loaded_ts) from Delta control table
    """
    if not DeltaTable.isDeltaTable(spark, ETL_CONTROL_PATH):
        return datetime(1970, 1, 1)

    df = (
        spark.read.format("delta").load(ETL_CONTROL_PATH)
        .filter(col("job_name") == lit(JOB_NAME))
    )

    if df.rdd.isEmpty():
        return datetime(1970, 1, 1)

    ts = df.select("last_loaded_ts").first()[0]
    return ts or datetime(1970, 1, 1)


def upsert_etl_control(job_name: str, last_loaded_ts, status: str):
    """
    Upsert watermark in Delta.
    - If last_loaded_ts is none(FAIL), DO NOT step the previous watermark.
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


# Main
def main():
    global spark
    spark = (
        SparkSession.builder
        .appName(JOB_NAME)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # DEV tuning
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "64MB")

    silver_exists = DeltaTable.isDeltaTable(spark, SILVER_BASE_PATH)

    try:
        # 1) Watermark (raw_loaded_at) from Delta control table
        last_ts = read_last_loaded_ts(spark)
        print(f"[{JOB_NAME}] last_loaded_ts(raw_loaded_at): {last_ts}")

        # 2) Read Bronze incremental by raw_loaded_at
        bronze_reader = spark.read.format("delta").load(BRONZE_BASE_PATH)

        if silver_exists:
            bronze_reader = (
                bronze_reader
                # prune de particiones (Bronze partitionBy(load_date))
                .filter(col("load_date") >= to_date(lit(last_ts)))
                # incremental real
                .filter(col("raw_loaded_at") > lit(last_ts))
            )

        NULL_LIKES = ["null", "n/a", "none", "-", ""]

        bronze_df = (
            bronze_reader
            # Ids
            .withColumn("driver_id", col("driver_id").cast("long"))
            # Strings
            .withColumn("full_name", trim(col("full_name")))
            .withColumn("license_number", trim(col("license_number")))
            # status normalizado
            .withColumn("status", lower(trim(col("status"))))
            # Soft delete fields
            .withColumn("is_deleted", col("is_deleted").cast("boolean"))
            # Timestamps
            .withColumn("created_at", col("created_at").cast("timestamp"))
            .withColumn("updated_at", col("updated_at").cast("timestamp"))
            .withColumn("deleted_at", col("deleted_at").cast("timestamp"))
            .withColumn("raw_loaded_at", col("raw_loaded_at").cast("timestamp"))
            # source_system
            .withColumn("source_system", trim(col("source_system")))
        )

        # null-likes -> NULL for license_number (if garbage came in)
        bronze_df = (
            bronze_df
            .withColumn(
                "license_number",
                when(col("license_number").isNull(), lit(None))
                .when(lower(col("license_number")).isin(NULL_LIKES), lit(None))
                .otherwise(col("license_number"))
            )
        )

        # Debug / confirmation
        bronze_count = bronze_df.count()
        print(f"[{JOB_NAME}] bronze_df count: {bronze_count}")

        min_max = bronze_df.select(
            spark_min("raw_loaded_at").alias("min_ts"),
            spark_max("raw_loaded_at").alias("max_ts")
        ).first()
        print(f"[{JOB_NAME}] min/max raw_loaded_at: {min_max}")

        if bronze_count == 0:
            print("No new bronze records to process")
            spark.stop()
            return

        # 3) Latest record per driver_id (only inside the new data)
        window_spec = (
            Window.partitionBy("driver_id")
            .orderBy(col("raw_loaded_at").desc())
        )

        latest_df = (
            bronze_df
            .withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        # 4) Simple quality/enrichment flags
        enriched_df = (
            latest_df
            .withColumn("missing_full_name", col("full_name").isNull() | (trim(col("full_name")) == ""))
            .withColumn("missing_license_number", col("license_number").isNull())
            .withColumn(
                "invalid_status",
                ~col("status").isin("active", "inactive", "suspended")
            )
        )

        # 5) SCD2 prep
        scd_ready_df = (
            enriched_df
            .withColumn(
                "scd_hash",
                sha2(
                    concat_ws(
                        "||",
                        coalesce(col("full_name").cast("string"), lit("")),
                        coalesce(col("license_number").cast("string"), lit("")),
                        coalesce(col("status").cast("string"), lit("")),
                        coalesce(col("is_deleted").cast("string"), lit("")),
                        coalesce(col("deleted_at").cast("string"), lit("")),
                        coalesce(col("source_system").cast("string"), lit(""))
                    ),
                    256
                )
            )
            .withColumn("valid_from", col("raw_loaded_at"))
            .withColumn("valid_to", lit(None).cast("timestamp"))
            .withColumn("is_current", lit(True))
        )

        # 6) First run: create Silver
        if not silver_exists:
            (
                scd_ready_df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(SILVER_BASE_PATH)
            )
            max_ts = scd_ready_df.select(spark_max("raw_loaded_at")).first()[0]
            upsert_etl_control(JOB_NAME, max_ts, "SUCCESS")
            print("Silver drivers table created + etl_control (delta) updated")
            spark.stop()
            return

        # 7) Merge incremental (SCD2)
        AUTO_MERGE = os.getenv("DELTA_AUTO_MERGE", "1" if ENV == "dev" else "0") == "1"
        if AUTO_MERGE:
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            print("[CONFIG] Delta schema auto-merge: ENABLED")
        else:
            print("[CONFIG] Delta schema auto-merge: DISABLED")

        silver_table = DeltaTable.forPath(spark, SILVER_BASE_PATH)

        # 7.1) Close current version if changed
        (
            silver_table.alias("t")
            .merge(
                scd_ready_df.alias("s"),
                "t.driver_id = s.driver_id AND t.is_current = true"
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

        # 7.2) Insert new current rows (new drivers or changed ones)
        (
            silver_table.alias("t")
            .merge(
                scd_ready_df.alias("s"),
                "t.driver_id = s.driver_id AND t.is_current = true"
            )
            .whenNotMatchedInsert(values={
                "driver_id": "s.driver_id",
                "full_name": "s.full_name",
                "license_number": "s.license_number",
                "status": "s.status",
                "is_deleted": "s.is_deleted",
                "deleted_at": "s.deleted_at",
                "created_at": "s.created_at",
                "updated_at": "s.updated_at",

                "batch_id": "s.batch_id",
                "source_system": "s.source_system",
                "raw_loaded_at": "s.raw_loaded_at",

                # enrichment flags
                "missing_full_name": "s.missing_full_name",
                "missing_license_number": "s.missing_license_number",
                "invalid_status": "s.invalid_status",

                # SCD2
                "scd_hash": "s.scd_hash",
                "valid_from": "s.valid_from",
                "valid_to": "CAST(NULL AS TIMESTAMP)",
                "is_current": "true"
            })
            .execute()
        )

        # 8) Update watermark
        max_ts = scd_ready_df.select(spark_max("raw_loaded_at")).first()[0]
        upsert_etl_control(JOB_NAME, max_ts, "SUCCESS")

        print("Silver drivers MERGE completed + etl_control (delta) updated")
        spark.stop()

    except Exception as e:
        # marcar FAIL sin mover watermark
        try:
            upsert_etl_control(JOB_NAME, None, f"FAIL: {type(e).__name__}")
        except Exception:
            pass
        spark.stop()
        raise


if __name__ == "__main__":
    main()
