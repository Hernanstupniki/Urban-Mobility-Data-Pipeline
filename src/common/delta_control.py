"""Delta-backed watermarks with failure-safe success timestamps."""

from __future__ import annotations

from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit


EPOCH = datetime(1970, 1, 1)
ETL_CONTROL_SCHEMA = "job_name string, last_loaded_ts timestamp, last_success_ts timestamp, last_status string"


def ensure_etl_control_table(spark: SparkSession, path: str) -> None:
    if DeltaTable.isDeltaTable(spark, path):
        return
    spark.createDataFrame([], ETL_CONTROL_SCHEMA).write.format("delta").mode("errorifexists").save(path)


def read_watermark(spark: SparkSession, path: str, job_name: str) -> datetime:
    if not DeltaTable.isDeltaTable(spark, path):
        return EPOCH
    rows = spark.read.format("delta").load(path).filter(col("job_name") == lit(job_name)).select("last_loaded_ts").take(1)
    return rows[0][0] if rows and rows[0][0] is not None else EPOCH


def record_status(spark: SparkSession, path: str, job_name: str, *, status: str, watermark: datetime | None = None) -> None:
    ensure_etl_control_table(spark, path)
    source = spark.createDataFrame(
        [(job_name, watermark, status)],
        "job_name string, last_loaded_ts timestamp, last_status string",
    ).withColumn("attempted_at", current_timestamp())
    success = status == "SUCCESS"
    DeltaTable.forPath(spark, path).alias("t").merge(source.alias("s"), "t.job_name = s.job_name").whenMatchedUpdate(
        set={
            "last_loaded_ts": "coalesce(s.last_loaded_ts, t.last_loaded_ts)",
            "last_success_ts": "s.attempted_at" if success else "t.last_success_ts",
            "last_status": "s.last_status",
        }
    ).whenNotMatchedInsert(values={
        "job_name": "s.job_name",
        "last_loaded_ts": "s.last_loaded_ts",
        "last_success_ts": "s.attempted_at" if success else "CAST(NULL AS TIMESTAMP)",
        "last_status": "s.last_status",
    }).execute()
