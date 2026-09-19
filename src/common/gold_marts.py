"""Explicit Gold fact and aggregate contracts.

Facts are rebuilt from the current Silver snapshot. This intentionally trades a
small amount of local compute for deterministic schema backfills and guarantees
that corrections moving a trip between dates or drivers cannot leave stale
aggregate rows behind.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg as spark_avg,
    col,
    coalesce,
    countDistinct,
    current_timestamp,
    date_format,
    length,
    lit,
    lower,
    max as spark_max,
    pmod,
    sum as spark_sum,
    to_date,
    trim,
    when,
    xxhash64,
)

from src.common.config import Settings
from src.common.logging import log_event
from src.common.spark import build_spark


TRIP_COLUMNS = [
    "trip_id", "passenger_id", "driver_id", "vehicle_id", "pickup_zone_id", "dropoff_zone_id",
    "start_lat", "start_lng", "end_lat", "end_lng", "status", "requested_at", "accepted_at",
    "started_at", "ended_at", "canceled_at", "cancel_reason", "cancel_by", "estimated_distance_km",
    "actual_distance_km", "fare_amount", "created_at", "updated_at", "raw_loaded_at",
    "distance_present_in_invalid_status", "completed_missing_distance", "is_distance_outlier",
    "completed_but_ended_at_null", "accepted_before_requested", "started_before_accepted",
    "ended_before_started", "start_coordinates_invalid", "end_coordinates_invalid", "coordinates_missing",
    "driver_vehicle_mismatch", "vehicle_driver_unverifiable", "acceptance_delay_minutes",
    "trip_duration_minutes", "is_acceptance_delay_outlier", "is_trip_duration_outlier",
    "cancel_note_contains_potential_pii",
]

PAYMENT_COLUMNS = [
    "payment_id", "trip_id", "method", "status", "amount", "currency", "paid_at", "created_at",
    "updated_at", "raw_loaded_at", "amount_invalid", "currency_invalid", "paid_but_paid_at_null",
    "pending_but_paid_at_not_null", "provider_ref_missing",
    "provider_ref_contains_potential_pii", "duplicate_provider_ref", "canonical_payment_id",
    "currency_was_normalized",
]


def _require(frame: DataFrame, columns: list[str], source: str) -> None:
    missing = [name for name in columns if name not in frame.columns]
    if missing:
        raise ValueError(f"{source} is missing contract columns: {missing}")


def _overwrite(frame: DataFrame, path: str) -> int:
    cached = frame.cache()
    row_count = cached.count()
    cached.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    cached.unpersist()
    return row_count


def _validated_key(spark, fact: DataFrame, fact_column: str, dim_path: str, dim_column: str) -> DataFrame:
    if not DeltaTable.isDeltaTable(spark, dim_path):
        raise RuntimeError(f"Required Gold dimension not found: {dim_path}")
    marker = f"__{fact_column}_valid"
    dimension = spark.read.format("delta").load(dim_path).select(col(dim_column).cast("long").alias(marker)).dropDuplicates([marker])
    return fact.join(dimension, fact[fact_column] == dimension[marker], "left").withColumn(
        fact_column, when(col(marker).isNull(), lit(0)).otherwise(col(fact_column))
    ).drop(marker)


def build_fact_trips() -> None:
    settings = Settings.from_env()
    job_name = "fact_trips"
    source_path = settings.path("silver", "trips")
    target_path = settings.path("gold", "_marts", "facts", "fact_trips")
    spark = build_spark(job_name)
    try:
        if not DeltaTable.isDeltaTable(spark, source_path):
            raise RuntimeError(f"Required Silver table not found: {source_path}")
        source = spark.read.format("delta").load(source_path)
        _require(source, TRIP_COLUMNS + ["is_current"], source_path)
        fact = source.filter(col("is_current") == lit(True)).select(*TRIP_COLUMNS)
        fare_quantiles = fact.filter(col("fare_amount").isNotNull()).approxQuantile(
            "fare_amount", [0.5], 0.01
        )
        median_fare = fare_quantiles[0] if fare_quantiles else None
        fact = fact.withColumn(
            "fare_amount_was_imputed",
            col("fare_amount").isNull() & lit(median_fare is not None),
        ).withColumn(
            "fare_amount_analytical",
            coalesce(col("fare_amount"), lit(median_fare).cast(source.schema["fare_amount"].dataType)),
        )
        passenger_dimension = spark.read.format("delta").load(
            settings.path("gold", "_conformed", "snapshot", "dim_passenger")
        ).select(
            col("passenger_id").cast("long").alias("__source_passenger_id"),
            col("canonical_passenger_id").cast("long").alias("__canonical_passenger_id"),
        )
        fact = fact.join(
            passenger_dimension,
            fact["passenger_id"] == passenger_dimension["__source_passenger_id"],
            "left",
        )
        fact = (
            fact.withColumn(
                "passenger_key",
                coalesce(col("__canonical_passenger_id"), col("passenger_id"), lit(0)).cast("long"),
            )
            .withColumn("driver_key", coalesce(col("driver_id"), lit(0)).cast("long"))
            .withColumn("vehicle_key", coalesce(col("vehicle_id"), lit(0)).cast("long"))
            .withColumn("pickup_zone_key", coalesce(col("pickup_zone_id"), lit(0)).cast("long"))
            .withColumn("dropoff_zone_key", coalesce(col("dropoff_zone_id"), lit(0)).cast("long"))
            .withColumn("request_date_key", coalesce(date_format(to_date(col("requested_at")), "yyyyMMdd").cast("int"), lit(0)))
            .withColumn("dwh_loaded_at", current_timestamp())
            .drop("__source_passenger_id", "__canonical_passenger_id")
        )
        for fact_col, dim_parts, dim_col in [
            ("passenger_key", ("snapshot", "dim_passenger"), "passenger_id"),
            ("driver_key", ("snapshot", "dim_driver"), "driver_id"),
            ("vehicle_key", ("snapshot", "dim_vehicle"), "vehicle_id"),
            ("pickup_zone_key", ("static", "dim_zone"), "zone_id"),
            ("dropoff_zone_key", ("static", "dim_zone"), "zone_id"),
        ]:
            fact = _validated_key(spark, fact, fact_col, settings.path("gold", "_conformed", *dim_parts), dim_col)
        row_count = _overwrite(fact, target_path)
        log_event(job_name, "build", "SUCCESS", row_count=row_count, target=target_path)
    finally:
        spark.stop()


def build_fact_payments() -> None:
    settings = Settings.from_env()
    job_name = "fact_payments"
    source_path = settings.path("silver", "payments")
    target_path = settings.path("gold", "_marts", "facts", "fact_payments")
    dim_path = settings.path("gold", "_conformed", "static", "dim_payment_method")
    spark = build_spark(job_name)
    try:
        if not DeltaTable.isDeltaTable(spark, source_path):
            raise RuntimeError(f"Required Silver table not found: {source_path}")
        source = spark.read.format("delta").load(source_path)
        _require(source, PAYMENT_COLUMNS + ["is_current"], source_path)
        fact = source.filter(
            (col("is_current") == lit(True)) & (~coalesce(col("duplicate_provider_ref"), lit(False)))
        ).select(*PAYMENT_COLUMNS)
        normalized = lower(trim(col("method")))
        fact = fact.withColumn("method_norm", when(normalized.isNull() | (length(normalized) == 0) | normalized.isin("null", "n/a", "none", "-"), lit(None)).otherwise(normalized))
        fact = fact.withColumn(
            "payment_method_key",
            when(col("method_norm").isNull(), lit(0)).otherwise((pmod(xxhash64(col("method_norm")), lit(2147483646)) + lit(1)).cast("int")),
        ).withColumn(
            "payment_date_key",
            coalesce(date_format(to_date(coalesce(col("paid_at"), col("created_at"), col("raw_loaded_at"))), "yyyyMMdd").cast("int"), lit(0)),
        ).withColumn("dwh_loaded_at", current_timestamp()).drop("method_norm")
        if not DeltaTable.isDeltaTable(spark, dim_path):
            raise RuntimeError(f"Required payment dimension not found: {dim_path}")
        dim = spark.read.format("delta").load(dim_path).select(col("payment_method_key").cast("int").alias("__pmk"))
        fact = fact.join(dim, fact["payment_method_key"] == dim["__pmk"], "left").withColumn(
            "payment_method_key", when(col("__pmk").isNull(), lit(0)).otherwise(col("payment_method_key"))
        ).drop("__pmk")
        row_count = _overwrite(fact, target_path)
        log_event(job_name, "build", "SUCCESS", row_count=row_count, target=target_path)
    finally:
        spark.stop()


def build_aggregate(name: str) -> None:
    if name not in {"trips_daily", "driver_daily"}:
        raise ValueError("Unsupported aggregate")
    settings = Settings.from_env()
    job_name = f"agg_{name}"
    source_path = settings.path("gold", "_marts", "facts", "fact_trips")
    target_path = settings.path("gold", "_marts", "aggregates", f"agg_{name}")
    spark = build_spark(job_name)
    try:
        if not DeltaTable.isDeltaTable(spark, source_path):
            raise RuntimeError(f"Required fact table not found: {source_path}")
        fact = spark.read.format("delta").load(source_path).withColumn("trip_date", to_date(col("requested_at")))
        if name == "trips_daily":
            aggregate = fact.groupBy(col("request_date_key").alias("date_key"), "trip_date").agg(
                countDistinct("trip_id").cast("long").alias("trips_total"),
                spark_sum(when(col("status") == "completed", 1).otherwise(0)).cast("long").alias("trips_completed"),
                spark_sum(when(col("status").isin("cancelled", "canceled"), 1).otherwise(0)).cast("long").alias("trips_cancelled"),
                spark_sum(when(col("status").isin("requested", "accepted", "started", "in_progress"), 1).otherwise(0)).cast("long").alias("trips_active"),
                spark_sum("fare_amount").alias("sum_fare_amount"), spark_avg("fare_amount").alias("avg_fare_amount"),
                spark_sum("fare_amount_analytical").alias("sum_fare_amount_analytical"),
                spark_avg("fare_amount_analytical").alias("avg_fare_amount_analytical"),
                spark_sum("actual_distance_km").alias("sum_distance_km"), spark_avg("actual_distance_km").alias("avg_distance_km"),
                spark_max("raw_loaded_at").alias("max_fact_raw_loaded_at"),
            ).withColumnRenamed("trip_date", "date")
        else:
            aggregate = fact.groupBy(col("request_date_key").alias("trip_date_key"), "trip_date", "driver_key").agg(
                countDistinct("trip_id").cast("long").alias("trips_count"),
                spark_sum(when(col("status") == "completed", 1).otherwise(0)).cast("long").alias("completed_trips"),
                spark_sum(when(col("status").isin("cancelled", "canceled"), 1).otherwise(0)).cast("long").alias("canceled_trips"),
                countDistinct("passenger_key").cast("long").alias("unique_passengers"),
                spark_sum("fare_amount").alias("total_fare_amount"), spark_sum("actual_distance_km").alias("total_distance_km"),
                spark_sum("fare_amount_analytical").alias("total_fare_amount_analytical"),
                spark_max("raw_loaded_at").alias("max_raw_loaded_at"),
            ).withColumnRenamed("driver_key", "driver_id")
        aggregate = aggregate.withColumn("dwh_loaded_at", current_timestamp())
        row_count = _overwrite(aggregate, target_path)
        log_event(job_name, "build", "SUCCESS", row_count=row_count, target=target_path)
    finally:
        spark.stop()
