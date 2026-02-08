import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# -------------------------
# Config (env-driven)
# -------------------------
ENV = os.getenv("ENV", "dev")

# Base path for Silver
SILVER_BASE_PATH = os.getenv("SILVER_BASE_PATH", f"data/{ENV}/silver")

# Default: tables that typically have SCD2 history
TABLES = os.getenv("TABLES", "passengers,drivers,ratings")

# SCD2 history retention (in days) - default: 1 year
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "365"))

# VACUUM: Delta uses hours
VACUUM_RETAIN_HOURS = int(os.getenv("VACUUM_RETAIN_HOURS", str(RETENTION_DAYS * 24)))
SKIP_VACUUM = os.getenv("SKIP_VACUUM", "false").lower() == "true"

# DEV ONLY: allow VACUUM < 168 hours
UNSAFE_VACUUM = os.getenv("UNSAFE_VACUUM", "0") in ("1", "true", "True")

# Optional: log row counts before delete (slower)
COUNT_BEFORE_DELETE = os.getenv("COUNT_BEFORE_DELETE", "false").lower() == "true"


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

    if UNSAFE_VACUUM:
        # DEV ONLY: disables safety check that protects time travel / concurrent readers
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        logging.warning("UNSAFE_VACUUM=1 => Delta retentionDurationCheck DISABLED (DEV only)")

    return spark


def retention_delete_table(spark: SparkSession, table_path: str, retention_days: int) -> None:
    if not DeltaTable.isDeltaTable(spark, table_path):
        logging.warning(f"SKIP (no Delta table): {table_path}")
        return

    df = spark.read.format("delta").load(table_path)
    cols = set(df.columns)

    # Silver expected SCD2 columns
    if ("is_current" not in cols) or ("valid_to" not in cols):
        logging.warning(
            f"SKIP (no SCD2 columns is_current/valid_to): {table_path}. "
            f"Columns: {sorted(list(cols))}"
        )
        return

    # Delete ONLY closed history (never current):
    # is_current=false AND valid_to < now() - N days
    condition = (
        (col("is_current") == expr("false")) &
        col("valid_to").isNotNull() &
        (col("valid_to") < expr(f"current_timestamp() - INTERVAL {retention_days} DAYS"))
    )
    condition_desc = f"is_current=false AND valid_to < current_timestamp() - INTERVAL {retention_days} DAYS"

    if COUNT_BEFORE_DELETE:
        n_to_delete = df.filter(condition).count()
        logging.info(f"{table_path} | rows matching retention condition: {n_to_delete}")

    logging.info(f"{table_path} | DELETE where {condition_desc}")
    delta_tbl = DeltaTable.forPath(spark, table_path)
    delta_tbl.delete(condition)

    if not SKIP_VACUUM:
        logging.info(f"{table_path} | VACUUM RETAIN {VACUUM_RETAIN_HOURS} HOURS (unsafe={UNSAFE_VACUUM})")
        delta_tbl.vacuum(VACUUM_RETAIN_HOURS)


def main():
    spark = build_spark(f"silver_retention_cleanup_{ENV}")

    try:
        tables = [t.strip() for t in TABLES.split(",") if t.strip()]

        logging.info("========================================")
        logging.info("Silver retention cleanup (SCD2 history)")
        logging.info(f"ENV={ENV}")
        logging.info(f"SILVER_BASE_PATH={SILVER_BASE_PATH}")
        logging.info(f"TABLES={tables}")
        logging.info(f"RETENTION_DAYS={RETENTION_DAYS}")
        logging.info(f"VACUUM_RETAIN_HOURS={VACUUM_RETAIN_HOURS} (skip={SKIP_VACUUM})")
        logging.info(f"UNSAFE_VACUUM={UNSAFE_VACUUM}")
        logging.info("========================================")

        for t in tables:
            path = os.path.join(SILVER_BASE_PATH, t)
            retention_delete_table(spark, path, RETENTION_DAYS)

        logging.info("Silver retention cleanup finished OK")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
