import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp,
    max as spark_max, min as spark_min, to_date, coalesce,
    row_number, when, trim, lower, sha2, concat_ws
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Config
JOB_NAME = "passengers_bronze_to_silver"

ENV = os.getenv("ENV", "dev")
BRONZE_BASE_PATH = f"data/{ENV}/bronze/passengers"
SILVER_BASE_PATH = f"data/{ENV}/silver/passengers"

# Delta control table (watermarks)
CONTROL_BASE_PATH = f"data/{ENV}/_control"
ETL_CONTROL_PATH = f"{CONTROL_BASE_PATH}/etl_control"

# GDPR configs (simple + aligned to your architecture)
GDPR_ANON_NAME = os.getenv("GDPR_ANON_NAME", "ANONYMIZED")
GDPR_NULLIFY_CITY = os.getenv("GDPR_NULLIFY_CITY", "1") == "1"  # optional


# Delta control helpers
def ensure_etl_control_table(spark: SparkSession):
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


def read_last_loaded_ts(spark: SparkSession) -> datetime:
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


def upsert_etl_control(spark: SparkSession, job_name: str, last_loaded_ts, status: str):
    """
    Upsert in Delta:
    - If last_loaded_ts is None(FAIL), DO NOT step on the previous watermark.
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
    spark = (
        SparkSession.builder
        .appName(JOB_NAME)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # DEV / WSL tuning
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "64MB")

    silver_exists = DeltaTable.isDeltaTable(spark, SILVER_BASE_PATH)

    try:
        # 1) Watermark (raw_loaded_at) desde Delta control table
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
            .withColumn("passenger_id", col("passenger_id").cast("long"))
            # Strings
            .withColumn("full_name", trim(col("full_name")))
            .withColumn("email", lower(trim(col("email"))))
            .withColumn("phone", trim(col("phone")))
            .withColumn("city", trim(col("city")))
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

        # null-likes -> NULL for email/phone (if garbage came in)
        bronze_df = (
            bronze_df
            .withColumn(
                "email",
                when(col("email").isNull(), lit(None))
                .when(lower(col("email")).isin(NULL_LIKES), lit(None))
                .otherwise(col("email"))
            )
            .withColumn(
                "phone",
                when(col("phone").isNull(), lit(None))
                .when(lower(col("phone")).isin(NULL_LIKES), lit(None))
                .otherwise(col("phone"))
            )
        )

        # 2.5) GDPR safety belt:
        # if is_deleted=true, force redaction even if source accidentally sends PII
        bronze_df = (
            bronze_df
            .withColumn(
                "full_name",
                when(col("is_deleted") == lit(True), lit(GDPR_ANON_NAME)).otherwise(col("full_name"))
            )
            .withColumn(
                "email",
                when(col("is_deleted") == lit(True), lit(None).cast("string")).otherwise(col("email"))
            )
            .withColumn(
                "phone",
                when(col("is_deleted") == lit(True), lit(None).cast("string")).otherwise(col("phone"))
            )
        )

        if GDPR_NULLIFY_CITY:
            bronze_df = bronze_df.withColumn(
                "city",
                when(col("is_deleted") == lit(True), lit(None).cast("string")).otherwise(col("city"))
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

        # 3) Latest record per passenger_id (only inside the new data)
        window_spec = (
            Window.partitionBy("passenger_id")
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
            .withColumn("missing_email", col("email").isNull() | (trim(col("email")) == ""))
            .withColumn("missing_phone", col("phone").isNull() | (trim(col("phone")) == ""))
            .withColumn(
                "invalid_email_format",
                when(col("email").isNull(), lit(False))
                .otherwise(~col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"))
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
                        coalesce(col("email").cast("string"), lit("")),
                        coalesce(col("phone").cast("string"), lit("")),
                        coalesce(col("city").cast("string"), lit("")),
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

            # 6.1) GDPR backfill on first run too (in case first load already contains is_deleted=true)
            silver_table = DeltaTable.forPath(spark, SILVER_BASE_PATH)

            gdpr_events_df = (
                scd_ready_df
                .filter(col("is_deleted") == lit(True))
                .select("passenger_id", "deleted_at")
                .dropDuplicates(["passenger_id"])
            )

            gdpr_count = gdpr_events_df.count()
            print(f"[{JOB_NAME}] GDPR passengers in this batch: {gdpr_count}")

            if gdpr_count > 0:
                deleted_at_expr = "coalesce(g.deleted_at, t.deleted_at, current_timestamp())"
                city_expr = "CAST(NULL AS STRING)" if GDPR_NULLIFY_CITY else "t.city"

                (
                    silver_table.alias("t")
                    .merge(gdpr_events_df.alias("g"), "t.passenger_id = g.passenger_id")
                    .whenMatchedUpdate(set={
                        "full_name": f"'{GDPR_ANON_NAME}'",
                        "email": "CAST(NULL AS STRING)",
                        "phone": "CAST(NULL AS STRING)",
                        "city": city_expr,
                        "is_deleted": "true",
                        "deleted_at": deleted_at_expr,
                        # keep flags consistent
                        "missing_email": "true",
                        "missing_phone": "true",
                        "invalid_email_format": "false",
                        "missing_full_name": "false",
                    })
                    .execute()
                )
                print(f"[{JOB_NAME}] GDPR backfill applied to Silver history (first run).")

            max_ts = scd_ready_df.select(spark_max("raw_loaded_at")).first()[0]
            upsert_etl_control(spark, JOB_NAME, max_ts, "SUCCESS")
            print("Silver passengers table created + etl_control (delta) updated")
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
                "t.passenger_id = s.passenger_id AND t.is_current = true"
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

        # 7.2) Insert new current rows (new passengers or changed ones)
        (
            silver_table.alias("t")
            .merge(
                scd_ready_df.alias("s"),
                "t.passenger_id = s.passenger_id AND t.is_current = true"
            )
            .whenNotMatchedInsert(values={
                "passenger_id": "s.passenger_id",
                "full_name": "s.full_name",
                "email": "s.email",
                "phone": "s.phone",
                "city": "s.city",
                "is_deleted": "s.is_deleted",
                "deleted_at": "s.deleted_at",
                "created_at": "s.created_at",
                "updated_at": "s.updated_at",

                "batch_id": "s.batch_id",
                "source_system": "s.source_system",
                "raw_loaded_at": "s.raw_loaded_at",

                # enrichment flags
                "missing_full_name": "s.missing_full_name",
                "missing_email": "s.missing_email",
                "missing_phone": "s.missing_phone",
                "invalid_email_format": "s.invalid_email_format",

                # SCD2
                "scd_hash": "s.scd_hash",
                "valid_from": "s.valid_from",
                "valid_to": "CAST(NULL AS TIMESTAMP)",
                "is_current": "true"
            })
            .execute()
        )

        # 7.3) GDPR backfill: redact PII for ALL historical versions in Silver
        gdpr_events_df = (
            scd_ready_df
            .filter(col("is_deleted") == lit(True))
            .select("passenger_id", "deleted_at")
            .dropDuplicates(["passenger_id"])
        )

        gdpr_count = gdpr_events_df.count()
        print(f"[{JOB_NAME}] GDPR passengers in this batch: {gdpr_count}")

        if gdpr_count > 0:
            deleted_at_expr = "coalesce(g.deleted_at, t.deleted_at, current_timestamp())"
            city_expr = "CAST(NULL AS STRING)" if GDPR_NULLIFY_CITY else "t.city"

            (
                silver_table.alias("t")
                .merge(gdpr_events_df.alias("g"), "t.passenger_id = g.passenger_id")
                .whenMatchedUpdate(set={
                    "full_name": f"'{GDPR_ANON_NAME}'",
                    "email": "CAST(NULL AS STRING)",
                    "phone": "CAST(NULL AS STRING)",
                    "city": city_expr,
                    "is_deleted": "true",
                    "deleted_at": deleted_at_expr,
                    # keep flags consistent
                    "missing_email": "true",
                    "missing_phone": "true",
                    "invalid_email_format": "false",
                    "missing_full_name": "false",
                })
                .execute()
            )
            print(f"[{JOB_NAME}] GDPR backfill applied to Silver history.")

        # 8) Update watermark
        max_ts = scd_ready_df.select(spark_max("raw_loaded_at")).first()[0]
        upsert_etl_control(spark, JOB_NAME, max_ts, "SUCCESS")

        print("Silver passengers MERGE completed + etl_control (delta) updated")
        spark.stop()

    except Exception as e:
        # FAIL
        try:
            upsert_etl_control(spark, JOB_NAME, None, f"FAIL: {type(e).__name__}")
        except Exception:
            pass
        spark.stop()
        raise


if __name__ == "__main__":
    main()
