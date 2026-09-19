"""Backfill corrected vehicle and trip columns before incremental jobs resume."""

from delta.tables import DeltaTable
from pyspark.sql.functions import col, coalesce, concat_ws, lit, sha2

from src.common.config import Settings
from src.common.logging import log_event
from src.common.spark import build_spark


JOB_NAME = "migration_002_reconcile_silver_contracts"


def _hash(columns):
    return sha2(concat_ws("||", *[coalesce(col(name).cast("string"), lit("")) for name in columns]), 256)


def migrate_vehicles(spark, path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        return
    frame = spark.read.format("delta").load(path)
    if "is_deleted" not in frame.columns:
        frame = frame.withColumn("is_deleted", lit(False))
    if "deleted_at" not in frame.columns:
        frame = frame.withColumn("deleted_at", lit(None).cast("timestamp"))
    hash_columns = ["driver_id", "plate_number", "vehicle_type", "make", "model", "year", "status", "is_deleted", "deleted_at", "source_system"]
    frame = frame.withColumn("scd_hash", _hash(hash_columns))
    frame = frame.cache()
    row_count = frame.count()
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    frame.unpersist()
    log_event(JOB_NAME, "vehicles", "SUCCESS", row_count=row_count, target=path)


def migrate_trips(spark, path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        return
    frame = spark.read.format("delta").load(path)
    false_columns = {
        "distance_present_in_invalid_status": (
            col("actual_distance_km").isNotNull() & (col("actual_distance_km") > 0) & (~col("status").isin("completed", "started"))
        ),
        "completed_missing_distance": (col("status") == "completed") & col("actual_distance_km").isNull(),
        "start_coordinates_invalid": col("start_lat").isNotNull() & col("start_lng").isNotNull() & ((col("start_lat") < -90) | (col("start_lat") > 90) | (col("start_lng") < -180) | (col("start_lng") > 180)),
        "end_coordinates_invalid": col("end_lat").isNotNull() & col("end_lng").isNotNull() & ((col("end_lat") < -90) | (col("end_lat") > 90) | (col("end_lng") < -180) | (col("end_lng") > 180)),
        "coordinates_missing": col("start_lat").isNull() | col("start_lng").isNull() | col("end_lat").isNull() | col("end_lng").isNull(),
    }
    for name, expression in false_columns.items():
        frame = frame.withColumn(name, coalesce(expression.cast("boolean"), lit(False)))
    frame = frame.withColumn("has_distance_in_invalid_status", col("distance_present_in_invalid_status") | col("completed_missing_distance"))
    hash_columns = [
        "passenger_id", "driver_id", "vehicle_id", "pickup_zone_id", "dropoff_zone_id", "status",
        "requested_at", "accepted_at", "started_at", "ended_at", "canceled_at", "estimated_distance_km",
        "actual_distance_km", "start_lat", "start_lng", "end_lat", "end_lng", "cancel_reason", "cancel_by",
        "cancel_note", "fare_amount", "source_system",
    ]
    frame = frame.withColumn("scd_hash", _hash(hash_columns))
    frame = frame.cache()
    row_count = frame.count()
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    frame.unpersist()
    log_event(JOB_NAME, "trips", "SUCCESS", row_count=row_count, target=path)


def migrate_hash(spark, path: str, table: str, hash_columns: list[str]) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        return
    frame = spark.read.format("delta").load(path)
    missing = [name for name in hash_columns if name not in frame.columns]
    if missing:
        raise ValueError(f"Cannot reconcile {table} hash; missing columns: {missing}")
    frame = frame.withColumn("scd_hash", _hash(hash_columns)).cache()
    row_count = frame.count()
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    frame.unpersist()
    log_event(JOB_NAME, table, "SUCCESS", row_count=row_count, target=path)


def main() -> None:
    settings = Settings.from_env()
    spark = build_spark(JOB_NAME)
    try:
        migrate_vehicles(spark, settings.path("silver", "vehicles"))
        migrate_trips(spark, settings.path("silver", "trips"))
        migrate_hash(
            spark,
            settings.path("silver", "payments"),
            "payments",
            ["trip_id", "method", "status", "amount", "currency", "provider_ref", "paid_at", "source_system"],
        )
        migrate_hash(
            spark,
            settings.path("silver", "ratings"),
            "ratings",
            ["trip_id", "passenger_id", "driver_id", "score", "comment", "created_at", "source_system"],
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
