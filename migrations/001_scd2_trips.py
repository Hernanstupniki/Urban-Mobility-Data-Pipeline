"""Bring Silver trips onto the canonical SCD2 hash contract."""

from delta.tables import DeltaTable
from pyspark.sql.functions import col, coalesce, concat_ws, lit, sha2

from src.common.config import Settings
from src.common.logging import log_event
from src.common.spark import build_spark


JOB_NAME = "migration_001_scd2_trips"
HASH_COLUMNS = [
    "passenger_id", "driver_id", "vehicle_id", "pickup_zone_id", "dropoff_zone_id",
    "status", "requested_at", "accepted_at", "started_at", "ended_at", "canceled_at",
    "estimated_distance_km", "actual_distance_km", "start_lat", "start_lng", "end_lat", "end_lng",
    "cancel_reason", "cancel_by", "cancel_note", "fare_amount", "source_system",
]


def main() -> None:
    settings = Settings.from_env()
    path = settings.path("silver", "trips")
    spark = build_spark(JOB_NAME)
    try:
        if not DeltaTable.isDeltaTable(spark, path):
            log_event(JOB_NAME, "silver_trips", "SKIPPED", reason="not_delta", target=path)
            return
        frame = spark.read.format("delta").load(path)
        missing_source = [name for name in HASH_COLUMNS if name not in frame.columns]
        if missing_source:
            raise ValueError(f"Cannot calculate canonical hash; missing columns: {missing_source}")
        output = frame.withColumn(
            "scd_hash",
            sha2(concat_ws("||", *[coalesce(col(name).cast("string"), lit("")) for name in HASH_COLUMNS]), 256),
        )
        if "valid_from" not in output.columns:
            output = output.withColumn("valid_from", col("raw_loaded_at"))
        if "valid_to" not in output.columns:
            output = output.withColumn("valid_to", lit(None).cast("timestamp"))
        if "is_current" not in output.columns:
            output = output.withColumn("is_current", lit(True))
        output = output.cache()
        row_count = output.count()
        output.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        output.unpersist()
        log_event(JOB_NAME, "silver_trips", "SUCCESS", row_count=row_count, target=path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
