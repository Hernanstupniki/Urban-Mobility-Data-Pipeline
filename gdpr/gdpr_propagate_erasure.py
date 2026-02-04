import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, max as spark_max
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# Config
ENV = os.getenv("ENV", "dev")
JOB_NAME = os.getenv("JOB_NAME", "gdpr_propagate_erasure")

# OLTP connection
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "mobility_oltp")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")  # required at runtime

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:5432/{DB_NAME}"

# Lake paths
BRONZE_PASSENGERS_PATH = os.getenv("BRONZE_PASSENGERS_PATH", f"data/{ENV}/bronze/passengers")
SILVER_PASSENGERS_PATH = os.getenv("SILVER_PASSENGERS_PATH", f"data/{ENV}/silver/passengers")

# Control table for watermark
CONTROL_BASE_PATH = os.getenv("CONTROL_BASE_PATH", f"data/{ENV}/_control")
GDPR_CONTROL_PATH = os.getenv("GDPR_CONTROL_PATH", f"{CONTROL_BASE_PATH}/gdpr_control")

# Anon values
ANON_NAME = os.getenv("ANON_NAME", "ANONYMIZED")


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # dev/WSL tuning
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    spark.conf.set("spark.default.parallelism", "4")

    return spark


# Control table helpers
def ensure_gdpr_control_table(spark: SparkSession):
    if DeltaTable.isDeltaTable(spark, GDPR_CONTROL_PATH):
        return

    (
        spark.createDataFrame(
            [],
            "job_name string, last_processed_at timestamp, last_success_ts timestamp, last_status string"
        )
        .write.format("delta")
        .mode("overwrite")
        .save(GDPR_CONTROL_PATH)
    )


def read_last_processed_at(spark: SparkSession) -> datetime:
    if not DeltaTable.isDeltaTable(spark, GDPR_CONTROL_PATH):
        return datetime(1970, 1, 1)

    df = (
        spark.read.format("delta").load(GDPR_CONTROL_PATH)
        .filter(col("job_name") == lit(JOB_NAME))
    )

    if df.rdd.isEmpty():
        return datetime(1970, 1, 1)

    ts = df.select("last_processed_at").first()[0]
    return ts or datetime(1970, 1, 1)


def upsert_gdpr_control(spark: SparkSession, last_processed_at, status: str):
    """
    Upsert in Delta:
    - If last_processed_at is None (FAIL), keep previous watermark.
    """
    ensure_gdpr_control_table(spark)

    target = DeltaTable.forPath(spark, GDPR_CONTROL_PATH)

    updates = (
        spark.createDataFrame(
            [(JOB_NAME, last_processed_at, status)],
            "job_name string, last_processed_at timestamp, last_status string"
        )
        .withColumn("last_success_ts", current_timestamp())
    )

    (
        target.alias("t")
        .merge(updates.alias("s"), "t.job_name = s.job_name")
        .whenMatchedUpdate(set={
            "last_processed_at": "coalesce(s.last_processed_at, t.last_processed_at)",
            "last_success_ts": "s.last_success_ts",
            "last_status": "s.last_status",
        })
        .whenNotMatchedInsert(values={
            "job_name": "s.job_name",
            "last_processed_at": "s.last_processed_at",
            "last_success_ts": "s.last_success_ts",
            "last_status": "s.last_status",
        })
        .execute()
    )


# Read OLTP GDPR requests (incremental)
def read_processed_erasure_requests(spark: SparkSession, last_processed_at: datetime):
    """
    Reads processed erasure requests incrementally using processed_at watermark.
    Source of truth: mobility.gdpr_requests (OLTP).
    """
    if DB_PASSWORD is None:
        raise ValueError("DB_PASSWORD env var is required")

    last_ts_str = last_processed_at.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
      (SELECT request_id, passenger_id, request_type, status, processed_at
       FROM mobility.gdpr_requests
       WHERE request_type = 'erasure'
         AND status = 'processed'
         AND processed_at IS NOT NULL
         AND processed_at > TIMESTAMP '{last_ts_str}'
      ) AS t
    """

    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", query)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


# Apply anonymization in Delta
def anonymize_passengers_delta(spark: SparkSession, table_path: str, passenger_ids_df):
    """
    Anonymizes PII for passenger_id in a Delta table (Bronze/Silver).
    Does NOT delete rows. It overwrites PII fields.
    """
    if not DeltaTable.isDeltaTable(spark, table_path):
        logging.warning(f"SKIP (no Delta table): {table_path}")
        return

    df = spark.read.format("delta").load(table_path)
    cols = set(df.columns)

    if "passenger_id" not in cols:
        logging.warning(f"SKIP (no passenger_id column): {table_path}")
        return

    # Create temp view of ids
    (
        passenger_ids_df
        .select(col("passenger_id").cast("long").alias("passenger_id"))
        .distinct()
        .createOrReplaceTempView("gdpr_passenger_ids")
    )

    cond = "passenger_id IN (SELECT passenger_id FROM gdpr_passenger_ids)"

    set_map = {}

    # PII fields (if present)
    if "full_name" in cols:
        set_map["full_name"] = f"'{ANON_NAME}'"
    if "email" in cols:
        set_map["email"] = "NULL"
    if "phone" in cols:
        set_map["phone"] = "NULL"
    if "city" in cols:
        set_map["city"] = "NULL"

    # Soft delete markers (if present)
    if "is_deleted" in cols:
        set_map["is_deleted"] = "true"
    if "deleted_at" in cols:
        set_map["deleted_at"] = "current_timestamp()"
    if "updated_at" in cols:
        set_map["updated_at"] = "current_timestamp()"

    if not set_map:
        logging.warning(f"SKIP (no columns to anonymize): {table_path}")
        return

    logging.info(f"{table_path} | GDPR anonymize where {cond} | set={list(set_map.keys())}")

    delta_tbl = DeltaTable.forPath(spark, table_path)
    delta_tbl.update(condition=cond, set=set_map)


def main():
    spark = build_spark(f"{JOB_NAME}_{ENV}")

    try:
        last_processed_at = read_last_processed_at(spark)

        logging.info("========================================")
        logging.info("GDPR propagate erasure -> Lake (Bronze/Silver)")
        logging.info(f"ENV={ENV}")
        logging.info(f"JOB_NAME={JOB_NAME}")
        logging.info(f"JDBC_URL={JDBC_URL}")
        logging.info(f"last_processed_at={last_processed_at}")
        logging.info(f"BRONZE_PASSENGERS_PATH={BRONZE_PASSENGERS_PATH}")
        logging.info(f"SILVER_PASSENGERS_PATH={SILVER_PASSENGERS_PATH}")
        logging.info(f"GDPR_CONTROL_PATH={GDPR_CONTROL_PATH}")
        logging.info("========================================")

        req_df = read_processed_erasure_requests(spark, last_processed_at)

        if req_df.rdd.isEmpty():
            logging.info("No new processed erasure requests to propagate")
            upsert_gdpr_control(spark, last_processed_at, "SUCCESS (no-op)")
            return

        passenger_ids_df = req_df.select(col("passenger_id").cast("long").alias("passenger_id")).distinct()
        n = passenger_ids_df.count()
        logging.info(f"Passengers to anonymize (distinct): {n}")

        # Apply anonymization
        anonymize_passengers_delta(spark, BRONZE_PASSENGERS_PATH, passenger_ids_df)
        anonymize_passengers_delta(spark, SILVER_PASSENGERS_PATH, passenger_ids_df)

        # Advance watermark
        new_watermark = req_df.select(spark_max("processed_at")).first()[0]
        logging.info(f"New last_processed_at watermark: {new_watermark}")

        upsert_gdpr_control(spark, new_watermark, f"SUCCESS (anonymized {n} passengers)")
        logging.info("GDPR propagation finished OK")

    except Exception as e:
        logging.error(f"GDPR propagation failed: {type(e).__name__}: {e}")
        try:
            upsert_gdpr_control(spark, None, f"FAIL: {type(e).__name__}")
        except Exception:
            pass
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
