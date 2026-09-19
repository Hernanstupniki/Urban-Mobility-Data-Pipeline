"""Reusable append-only JDBC-to-Bronze ingestion."""

from __future__ import annotations

import uuid

from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit, max as spark_max, to_date

from src.common.config import Settings
from src.common.delta_control import read_watermark, record_status
from src.common.logging import log_event
from src.common.spark import build_spark


def run_bronze(entity: str, *, watermark_column: str = "updated_at") -> None:
    if not entity.replace("_", "").isalnum() or not watermark_column.replace("_", "").isalnum():
        raise ValueError("Entity and watermark column must be simple identifiers")
    job_name = f"{entity}_oltp_to_bronze"
    settings = Settings.from_env(require_database=True)
    target_path = settings.path("bronze", entity)
    control_path = settings.path("_control", "etl_control")
    spark = build_spark(job_name)
    try:
        watermark = read_watermark(spark, control_path, job_name)
        log_event(job_name, "extract", "STARTED", watermark=watermark)
        source = (
            spark.read.format("jdbc")
            .option("url", settings.jdbc_url)
            .option("dbtable", f"mobility.{entity}")
            .option("user", settings.db_user)
            .option("password", settings.db_password)
            .option("driver", "org.postgresql.Driver")
            .load()
            .filter(col(watermark_column) >= lit(watermark))
        )
        if not source.take(1):
            log_event(job_name, "extract", "NO_DATA", watermark=watermark)
            return
        batch_id = str(uuid.uuid4())
        output = source.withColumn("source_system", lit("mobility_oltp")).withColumn(
            "raw_loaded_at", current_timestamp()
        ).withColumn("batch_id", lit(batch_id)).withColumn("load_date", to_date(col("raw_loaded_at")))
        primary_key = f"{entity[:-1]}_id"
        if DeltaTable.isDeltaTable(spark, target_path):
            existing = spark.read.format("delta").load(target_path).filter(
                col(watermark_column) >= lit(watermark)
            ).select(primary_key, watermark_column).dropDuplicates()
            output = output.alias("incoming").join(
                existing.alias("existing"),
                (col(f"incoming.{primary_key}") == col(f"existing.{primary_key}"))
                & (col(f"incoming.{watermark_column}") == col(f"existing.{watermark_column}")),
                "left_anti",
            )
        if not output.take(1):
            record_status(spark, control_path, job_name, status="SUCCESS", watermark=watermark)
            log_event(job_name, "load", "NO_DATA", watermark=watermark)
            return
        row_count = output.count()
        new_watermark = output.select(spark_max(watermark_column)).first()[0]
        output.write.format("delta").mode("append").partitionBy("load_date").save(target_path)
        record_status(spark, control_path, job_name, status="SUCCESS", watermark=new_watermark)
        log_event(job_name, "load", "SUCCESS", batch_id=batch_id, row_count=row_count, watermark=new_watermark, target=target_path)
    except Exception as exc:
        try:
            record_status(spark, control_path, job_name, status=f"FAIL:{type(exc).__name__}")
        except Exception as control_exc:
            log_event(job_name, "control", "FAIL", error=repr(control_exc))
        log_event(job_name, "load", "FAIL", error=repr(exc))
        raise
    finally:
        spark.stop()
