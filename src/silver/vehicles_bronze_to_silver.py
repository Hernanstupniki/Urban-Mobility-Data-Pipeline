import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, row_number, when, lit, min as spark_min,
    max as spark_max, to_date, current_timestamp, coalesce,
    trim, lower, upper, sha2, concat_ws
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Config
JOB_NAME = "vehicles_bronze_to_silver"

ENV = os.getenv("ENV", "dev")
BRONZE_BASE_PATH = f"data/{ENV}/bronze/vehicles"
SILVER_BASE_PATH = f"data/{ENV}/silver/vehicles"

# Delta control table (watermarks)
CONTROL_BASE_PATH = f"data/{ENV}/_control"
ETL_CONTROL_PATH = f"{CONTROL_BASE_PATH}/etl_control"


# Delta control helpers
def ensure_etl_control_table(spark: SparkSession):
    """
    Create Delta control table if it does not exist.
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


def read_last_loaded_ts(spark: SparkSession) -> datetime:
    """
    Read watermark (last_loaded_ts) from Delta control table.
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


def upsert_etl_control(spark: SparkSession, job_name: str, last_loaded_ts, status: str):
    """
    Upsert watermark in Delta.
    - If last_loaded_ts is None (FAIL), DO NOT step the previous watermark.
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
                # partition pruning (Bronze partitionBy(load_date))
                .filter(col("load_date") >= to_date(lit(last_ts)))
                # true incremental
                .filter(col("raw_loaded_at") > lit(last_ts))
            )

        NULL_LIKES = ["null", "n/a", "none", "-", ""]

        # Normalize and cast
        bronze_df = (
            bronze_reader
            # Ids
            .withColumn("vehicle_id", col("vehicle_id").cast("long"))
            .withColumn("driver_id", col("driver_id").cast("long"))
            # Strings
            .withColumn("plate_number", upper(trim(col("plate_number"))))
            .withColumn("vehicle_type", lower(trim(col("vehicle_type"))))
            .withColumn("make", trim(col("make")))
            .withColumn("model", trim(col("model")))
            # Numerics
            .withColumn("year", col("year").cast("int"))
            # status normalized
            .withColumn("status", lower(trim(col("status"))))
            # Timestamps
            .withColumn("created_at", col("created_at").cast("timestamp"))
            .withColumn("updated_at", col("updated_at").cast("timestamp"))
            .withColumn("raw_loaded_at", col("raw_loaded_at").cast("timestamp"))
            # Metadata
            .withColumn("source_system", trim(col("source_system")))
        )

        # null-likes -> NULL for make/model (optional free text)
        bronze_df = (
            bronze_df
            .withColumn(
                "make",
                when(col("make").isNull(), lit(None))
                .when(lower(col("make")).isin(NULL_LIKES), lit(None))
                .otherwise(col("make"))
            )
            .withColumn(
                "model",
                when(col("model").isNull(), lit(None))
                .when(lower(col("model")).isin(NULL_LIKES), lit(None))
                .otherwise(col("model"))
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

        # 3) Latest record per vehicle_id (only inside the new data)
        window_spec = (
            Window.partitionBy("vehicle_id")
            .orderBy(col("raw_loaded_at").desc())
        )

        latest_df = (
            bronze_df
            .withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        # 4) Simple quality/enrichment flags
        # Keep allowed sets conservative (can expand later without breaking logic)
        ALLOWED_VEHICLE_TYPES = ["sedan", "hatchback", "motorbike"]
        ALLOWED_STATUS = ["active", "inactive"]

        enriched_df = (
            latest_df
            .withColumn("missing_plate_number", col("plate_number").isNull() | (trim(col("plate_number")) == ""))
            .withColumn("missing_vehicle_type", col("vehicle_type").isNull() | (trim(col("vehicle_type")) == ""))
            .withColumn("invalid_vehicle_type", (~col("vehicle_type").isin(*ALLOWED_VEHICLE_TYPES)) & col("vehicle_type").isNotNull())
            .withColumn("missing_driver_id", col("driver_id").isNull())
            # Year constraints (table: 1980..current_year+1). We validate approx with spark year(now()).
            .withColumn(
                "invalid_year",
                when(col("year").isNull(), lit(False))
                .when((col("year") < lit(1980)) | (col("year") > (lit(datetime.now().year) + lit(1))), lit(True))
                .otherwise(lit(False))
            )
            .withColumn("invalid_status", ~col("status").isin(*ALLOWED_STATUS))
        )

        # 5) SCD2 prep
        scd_ready_df = (
            enriched_df
            .withColumn(
                "scd_hash",
                sha2(
                    concat_ws(
                        "||",
                        coalesce(col("driver_id").cast("string"), lit("")),
                        coalesce(col("plate_number").cast("string"), lit("")),
                        coalesce(col("vehicle_type").cast("string"), lit("")),
                        coalesce(col("make").cast("string"), lit("")),
                        coalesce(col("model").cast("string"), lit("")),
                        coalesce(col("year").cast("string"), lit("")),
                        coalesce(col("status").cast("string"), lit("")),
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
            upsert_etl_control(spark, JOB_NAME, max_ts, "SUCCESS")

            print("Silver vehicles table created + etl_control (delta) updated")
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

        # 7.2) Insert new current rows (new vehicles or changed ones)
        (
            silver_table.alias("t")
            .merge(
                scd_ready_df.alias("s"),
                "t.vehicle_id = s.vehicle_id AND t.is_current = true"
            )
            .whenNotMatchedInsert(values={
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
                "is_current": "true"
            })
            .execute()
        )

        # 8) Update watermark
        max_ts = scd_ready_df.select(spark_max("raw_loaded_at")).first()[0]
        upsert_etl_control(spark, JOB_NAME, max_ts, "SUCCESS")

        print("Silver vehicles MERGE completed + etl_control (delta) updated")
        spark.stop()

    except Exception as e:
        # FAIL (do not move watermark)
        try:
            upsert_etl_control(spark, JOB_NAME, None, f"FAIL: {type(e).__name__}")
        except Exception:
            pass
        spark.stop()
        raise


if __name__ == "__main__":
    main()
