import os
from datetime import datetime
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp,
    max as spark_max, to_date
)
from delta.tables import DeltaTable

# ============================================================
# Config
# ============================================================
JOB_NAME = "ratings_oltp_to_bronze"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "mobility_oltp")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:5432/{DB_NAME}"

ENV = os.getenv("ENV", "dev")
BRONZE_BASE_PATH = f"data/{ENV}/bronze/ratings"

# Delta control table (watermarks)
CONTROL_BASE_PATH = f"data/{ENV}/_control"
ETL_CONTROL_PATH = f"{CONTROL_BASE_PATH}/etl_control"


# ============================================================
# Delta control helpers
# ============================================================
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


# ============================================================
# Main
# ============================================================
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

    try:
        # 1) Watermark desde Delta control table (UPDATED_AT en ratings)
        last_loaded_ts = read_last_loaded_ts(spark)
        print(f"[{JOB_NAME}] last_loaded_ts(updated_at): {last_loaded_ts}")

        # 2) Read incremental ratings from OLTP (watermark = updated_at)
        ratings_df = (
            spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", "mobility.ratings")
            .option("user", DB_USER)
            .option("password", DB_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .load()
            .filter(col("updated_at") > lit(last_loaded_ts))
        )

        if ratings_df.rdd.isEmpty():
            print("No new ratings to load")
            spark.stop()
            return

        # 3) RAW metadata
        batch_id = str(uuid.uuid4())

        ratings_df = (
            ratings_df
            .withColumn("source_system", lit("mobility_oltp"))
            .withColumn("raw_loaded_at", current_timestamp())
            .withColumn("batch_id", lit(batch_id))
            .withColumn("load_date", to_date(col("raw_loaded_at")))
        )

        # 4) Write Bronze (Delta Lake)
        (
            ratings_df.write
            .format("delta")
            .mode("append")
            .partitionBy("load_date")
            .save(BRONZE_BASE_PATH)
        )

        row_count = ratings_df.count()
        max_ts = ratings_df.select(spark_max("updated_at")).first()[0]

        print(f"Wrote {row_count} ratings to Bronze (Delta)")
        print(f"[{JOB_NAME}] new watermark(updated_at): {max_ts}")

        # 5) Update watermark en Delta control table
        upsert_etl_control(spark, JOB_NAME, max_ts, "SUCCESS")

        print("Bronze load completed + etl_control (delta) updated")
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
