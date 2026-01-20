import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, trim, current_timestamp, sha2, concat_ws, coalesce
from delta.tables import DeltaTable
from datetime import datetime

JOB_NAME = "gold_trips_dim_zone"

# OLTP
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "mobility_oltp")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:5432/{DB_NAME}"

ENV = os.getenv("ENV", "dev")
GOLD_BASE = f"data/{ENV}/gold/trips"
DIM_ZONE_PATH = f"{GOLD_BASE}/dim_zone"

CONTROL_BASE_PATH = f"data/{ENV}/_control"
ETL_CONTROL_PATH = f"{CONTROL_BASE_PATH}/etl_control"


def ensure_etl_control_table(spark: SparkSession):
    if DeltaTable.isDeltaTable(spark, ETL_CONTROL_PATH):
        return
    (spark.createDataFrame([], "job_name string, last_loaded_ts timestamp, last_success_ts timestamp, last_status string")
        .write.format("delta").mode("overwrite").save(ETL_CONTROL_PATH))


def upsert_etl_control(spark: SparkSession, job_name: str, last_loaded_ts, status: str):
    ensure_etl_control_table(spark)
    target = DeltaTable.forPath(spark, ETL_CONTROL_PATH)
    updates = (
        spark.createDataFrame([(job_name, last_loaded_ts, status)],
                              "job_name string, last_loaded_ts timestamp, last_status string")
        .withColumn("last_success_ts", current_timestamp())
    )
    (target.alias("t")
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
        .execute())


def zone_key_expr(zone_id_col):
    return sha2(concat_ws("||", lit("zone"), coalesce(zone_id_col.cast("string"), lit(""))), 256)


def main():
    spark = (
        SparkSession.builder
        .appName(JOB_NAME)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    try:
        zones = (
            spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", "mobility.zones")
            .option("user", DB_USER)
            .option("password", DB_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        dim_zone = (
            zones
            .withColumn("zone_id", col("zone_id").cast("long"))
            .withColumn("zone_name", trim(col("zone_name")))
            .withColumn("city", trim(col("city")))
            .withColumn("region", trim(col("region")))
            .withColumn("zone_key", zone_key_expr(col("zone_id")))
            .withColumn("gold_loaded_at", current_timestamp())
            .select("zone_key", "zone_id", "zone_name", "city", "region", "gold_loaded_at")
        )

        dim_zone.write.format("delta").mode("overwrite").save(DIM_ZONE_PATH)

        upsert_etl_control(spark, JOB_NAME, current_timestamp(), "SUCCESS")
        print("dim_zone written (overwrite)")

    except Exception as e:
        try:
            upsert_etl_control(spark, JOB_NAME, None, f"FAIL: {type(e).__name__}")
        except Exception:
            pass
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
