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
JOB_NAME = "ratings_bronze_to_silver"

ENV = os.getenv("ENV", "dev")
BRONZE_BASE_PATH = f"data/{ENV}/bronze/ratings"
SILVER_BASE_PATH = f"data/{ENV}/silver/ratings"

# Delta control table (watermarks)
CONTROL_BASE_PATH = f"data/{ENV}/_control"
ETL_CONTROL_PATH = f"{CONTROL_BASE_PATH}/etl_control"


# Delta control helpers
def ensure_etl_control_table(spark):
    """
    Create the table control in delta if not exits
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
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
            .withColumn("rating_id", col("rating_id").cast("long"))
            .withColumn("trip_id", col("trip_id").cast("long"))
            .withColumn("passenger_id", col("passenger_id").cast("long"))
            .withColumn("driver_id", col("driver_id").cast("long"))

            # Score (1..5)
            .withColumn("score", col("score").cast("int"))
            .withColumn(
                "score",
                when((col("score") < 1) | (col("score") > 5), lit(None).cast("int"))
                .otherwise(col("score"))
            )

            # Comment: trim + null-likes -> NULL
            .withColumn("comment", trim(col("comment")))
            .withColumn(
                "comment",
                when(col("comment").isNull(), lit(None))
                .when(lower(col("comment")).isin(NULL_LIKES), lit(None))
                .otherwise(col("comment"))
            )

            # Timestamps
            .withColumn("created_at", col("created_at").cast("timestamp"))
            .withColumn("raw_loaded_at", col("raw_loaded_at").cast("timestamp"))

            # Source metadata
            .withColumn("source_system", trim(col("source_system")))
            .withColumn("batch_id", col("batch_id"))
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

        # 3) Latest record per trip_id (1 rating per trip)
        window_spec = (
            Window.partitionBy("trip_id")
            .orderBy(col("raw_loaded_at").desc())
        )

        latest_ratings_df = (
            bronze_df
            .withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        # 4) Enrichment / data quality flags
        enriched_df = (
            latest_ratings_df
            .withColumn("score_invalid", col("score").isNull())
            .withColumn("comment_missing", col("comment").isNull())
        )

        # SCD hash (business columns only)
        scd_ready_df = (
            enriched_df
            .withColumn(
                "scd_hash",
                sha2(
                    concat_ws(
                        "||",
                        coalesce(col("trip_id").cast("string"), lit("")),
                        coalesce(col("passenger_id").cast("string"), lit("")),
                        coalesce(col("driver_id").cast("string"), lit("")),
                        coalesce(col("score").cast("string"), lit("")),
                        coalesce(col("comment").cast("string"), lit("")),
                        coalesce(col("created_at").cast("string"), lit("")),
                        coalesce(col("source_system").cast("string"), lit(""))
                    ),
                    256
                )
            )
            .withColumn("valid_from", col("raw_loaded_at"))
            .withColumn("valid_to", lit(None).cast("timestamp"))
            .withColumn("is_current", lit(True))
        )

        # 5) First run: create Silver with rich schema
        if not silver_exists:
            (
                scd_ready_df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(SILVER_BASE_PATH)
            )
            max_ts = scd_ready_df.select(spark_max("raw_loaded_at")).first()[0]
            upsert_etl_control(JOB_NAME, max_ts, "SUCCESS")
            print("Silver ratings table created + etl_control (delta) updated")
            spark.stop()
            return

        # 6) Merge incremental (SCD2)
        AUTO_MERGE = os.getenv("DELTA_AUTO_MERGE", "1" if ENV == "dev" else "0") == "1"
        if AUTO_MERGE:
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            print("[CONFIG] Delta schema auto-merge: ENABLED")
        else:
            print("[CONFIG] Delta schema auto-merge: DISABLED")

        silver_table = DeltaTable.forPath(spark, SILVER_BASE_PATH)

        # 1) Close current version if changed
        (
            silver_table.alias("t")
            .merge(
                scd_ready_df.alias("s"),
                "t.trip_id = s.trip_id AND t.is_current = true"
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

        # 2) Insert new version
        (
            silver_table.alias("t")
            .merge(
                scd_ready_df.alias("s"),
                "t.trip_id = s.trip_id AND t.is_current = true"
            )
            .whenNotMatchedInsert(values={
                # Keys
                "rating_id": "s.rating_id",
                "trip_id": "s.trip_id",
                "passenger_id": "s.passenger_id",
                "driver_id": "s.driver_id",

                # Business fields
                "score": "s.score",
                "comment": "s.comment",

                # Source metadata
                "batch_id": "s.batch_id",
                "source_system": "s.source_system",

                # Timestamps
                "created_at": "s.created_at",
                "raw_loaded_at": "s.raw_loaded_at",

                # Data quality flags
                "score_invalid": "s.score_invalid",
                "comment_missing": "s.comment_missing",

                # SCD2 fields
                "scd_hash": "s.scd_hash",
                "valid_from": "s.valid_from",
                "valid_to": "CAST(NULL AS TIMESTAMP)",
                "is_current": "true"
            })
            .execute()
        )

        # 7) Update watermark at the end
        max_ts = scd_ready_df.select(spark_max("raw_loaded_at")).first()[0]
        upsert_etl_control(JOB_NAME, max_ts, "SUCCESS")

        print("Silver ratings MERGE completed + etl_control (delta) updated")
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
