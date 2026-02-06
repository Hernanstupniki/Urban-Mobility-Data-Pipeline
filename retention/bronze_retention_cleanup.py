import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, date_sub, expr, current_timestamp
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

ENV = os.getenv("ENV", "dev")
BRONZE_BASE_PATH = os.getenv("BRONZE_BASE_PATH", f"data/{ENV}/bronze")

# Tablas con PII / identificadores fuertes por defecto (GDPR scope)
TABLES = os.getenv("TABLES", "passengers,drivers,vehicles,ratings,trips,payments")

# Retención lógica (borrado de filas) por edad
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "30"))

# Retención física (vacuum) por horas
# Mejor práctica: 168h (7 días) por defecto. En DEV podés bajar, en PROD no.
VACUUM_RETAIN_HOURS = int(os.getenv("VACUUM_RETAIN_HOURS", "168"))
SKIP_VACUUM = os.getenv("SKIP_VACUUM", "false").lower() == "true"

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

    # tuning dev/WSL
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    spark.conf.set("spark.default.parallelism", "4")
    return spark


def retention_delete_table(spark: SparkSession, table_path: str, retention_days: int) -> None:
    if not DeltaTable.isDeltaTable(spark, table_path):
        logging.warning(f"SKIP (no Delta table): {table_path}")
        return

    df = spark.read.format("delta").load(table_path)
    cols = set(df.columns)

    cutoff_date_expr = date_sub(current_date(), retention_days)

    # Preferido: load_date (particionado)
    if "load_date" in cols:
        condition = col("load_date") < cutoff_date_expr
        condition_desc = f"load_date < date_sub(current_date(), {retention_days})"

    # Alternativa: raw_loaded_at (timestamp)
    elif "raw_loaded_at" in cols:
        condition = expr(f"raw_loaded_at < (current_timestamp() - INTERVAL {retention_days} DAYS)")
        condition_desc = f"raw_loaded_at < current_timestamp() - INTERVAL {retention_days} DAYS"

    else:
        logging.warning(
            f"SKIP (no load_date/raw_loaded_at for retention): {table_path}. "
            f"Columns: {sorted(list(cols))}"
        )
        return

    if COUNT_BEFORE_DELETE:
        n_to_delete = df.filter(condition).count()
        logging.info(f"{table_path} | rows matching retention condition: {n_to_delete}")

    logging.info(f"{table_path} | DELETE where {condition_desc}")
    delta_tbl = DeltaTable.forPath(spark, table_path)
    delta_tbl.delete(condition)

    if not SKIP_VACUUM:
        logging.info(f"{table_path} | VACUUM RETAIN {VACUUM_RETAIN_HOURS} HOURS")
        delta_tbl.vacuum(VACUUM_RETAIN_HOURS)


def main():
    spark = build_spark(f"bronze_retention_cleanup_{ENV}")

    try:
        tables = [t.strip() for t in TABLES.split(",") if t.strip()]

        logging.info("========================================")
        logging.info("Bronze retention cleanup")
        logging.info(f"ENV={ENV}")
        logging.info(f"BRONZE_BASE_PATH={BRONZE_BASE_PATH}")
        logging.info(f"TABLES={tables}")
        logging.info(f"RETENTION_DAYS={RETENTION_DAYS}")
        logging.info(f"VACUUM_RETAIN_HOURS={VACUUM_RETAIN_HOURS} (skip={SKIP_VACUUM})")
        logging.info(f"COUNT_BEFORE_DELETE={COUNT_BEFORE_DELETE}")
        logging.info("========================================")

        for t in tables:
            path = os.path.join(BRONZE_BASE_PATH, t)
            retention_delete_table(spark, path, RETENTION_DAYS)

        logging.info("Retention cleanup finished OK")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
