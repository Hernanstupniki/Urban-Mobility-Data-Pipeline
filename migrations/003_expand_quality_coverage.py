"""Backfill the extended dirty-data quality contract in existing Silver tables."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    initcap,
    length,
    lit,
    lower,
    max as spark_max,
    min as spark_min,
    regexp_replace,
    sha2,
    trim,
    when,
)

from src.common.config import Settings
from src.common.data_quality import (
    MAX_ACCEPTANCE_DELAY_MINUTES,
    MAX_TRIP_DURATION_MINUTES,
    NULL_LIKE_VALUES,
    PII_ANY_PATTERN,
    PII_CONTACT_SUFFIX_PATTERN,
    PII_EMAIL_PATTERN,
    PII_PHONE_PATTERN,
)
from src.common.logging import log_event
from src.common.spark import build_spark


JOB_NAME = "migration_003_expand_quality_coverage"


def _existing_bool(frame: DataFrame, name: str):
    return coalesce(col(name).cast("boolean"), lit(False)) if name in frame.columns else lit(False)


def _hash(columns: list[str]):
    return sha2(concat_ws("||", *[coalesce(col(name).cast("string"), lit("")) for name in columns]), 256)


def _overwrite(frame: DataFrame, path: str, table: str) -> None:
    cached = frame.cache()
    row_count = cached.count()
    cached.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    cached.unpersist()
    log_event(JOB_NAME, table, "SUCCESS", row_count=row_count, target=path)


def migrate_passengers(spark, path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        return
    frame = spark.read.format("delta").load(path)
    frame = frame.withColumn("email", lower(trim(col("email")))).withColumn("city", initcap(trim(col("city"))))
    phone_digits = regexp_replace(col("phone"), r"\D", "")
    frame = frame.withColumn(
        "invalid_phone_format",
        when(col("phone").isNull(), lit(False)).otherwise(
            (length(phone_digits) < lit(7)) | (length(phone_digits) > lit(15))
        ),
    )
    canonical = (
        frame.filter((col("is_current") == lit(True)) & col("email").isNotNull())
        .groupBy("email")
        .agg(spark_min("passenger_id").alias("__canonical_passenger_id"))
    )
    frame = frame.join(canonical, "email", "left").withColumn(
        "canonical_passenger_id",
        when(col("email").isNull(), col("passenger_id")).otherwise(
            coalesce(col("__canonical_passenger_id"), col("passenger_id"))
        ),
    ).withColumn(
        "potential_duplicate_passenger",
        col("email").isNotNull() & (col("passenger_id") != col("canonical_passenger_id")),
    ).drop("__canonical_passenger_id")
    frame = frame.withColumn(
        "scd_hash",
        _hash([
            "full_name", "email", "phone", "city", "is_deleted", "deleted_at",
            "canonical_passenger_id", "potential_duplicate_passenger", "source_system",
        ]),
    )
    _overwrite(frame, path, "passengers")


def migrate_vehicles(spark, path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        return
    frame = spark.read.format("delta").load(path)
    raw_type = col("vehicle_type")
    normalized = regexp_replace(lower(trim(raw_type)), r"\s+", " ")
    canonical = (
        when(normalized.isin("sedan", "saloon"), lit("sedan"))
        .when(normalized.isin("hatchback", "hatch back"), lit("hatchback"))
        .when(normalized.isin("motorbike", "motorcycle", "moto", "bike"), lit("motorbike"))
        .otherwise(normalized)
    )
    frame = frame.withColumn(
        "vehicle_type_was_normalized",
        _existing_bool(frame, "vehicle_type_was_normalized")
        | (raw_type.isNotNull() & (raw_type != canonical)),
    ).withColumn("vehicle_type", canonical)
    frame = frame.withColumn(
        "scd_hash",
        _hash([
            "driver_id", "plate_number", "vehicle_type", "vehicle_type_was_normalized", "make",
            "model", "year", "status", "is_deleted", "deleted_at", "source_system",
        ]),
    )
    _overwrite(frame, path, "vehicles")


def migrate_trips(spark, path: str, vehicles_path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        return
    frame = spark.read.format("delta").load(path)
    pii_flag = _existing_bool(frame, "cancel_note_contains_potential_pii") | coalesce(
        col("cancel_note").rlike(PII_ANY_PATTERN), lit(False)
    )
    frame = frame.withColumn("cancel_note_contains_potential_pii", pii_flag)
    frame = frame.withColumn("cancel_note", regexp_replace(col("cancel_note"), PII_CONTACT_SUFFIX_PATTERN, ""))
    frame = frame.withColumn("cancel_note", regexp_replace(col("cancel_note"), PII_EMAIL_PATTERN, "[REDACTED]"))
    frame = frame.withColumn("cancel_note", regexp_replace(col("cancel_note"), PII_PHONE_PATTERN, "[REDACTED]"))
    frame = frame.withColumn("cancel_note", trim(col("cancel_note"))).withColumn(
        "cancel_note",
        when(col("cancel_note").isNull() | lower(col("cancel_note")).isin(*NULL_LIKE_VALUES), lit(None))
        .otherwise(col("cancel_note")),
    )
    if DeltaTable.isDeltaTable(spark, vehicles_path):
        vehicles = (
            spark.read.format("delta").load(vehicles_path)
            .filter(col("is_current") == lit(True))
            .select(col("vehicle_id").alias("__vehicle_id"), col("driver_id").alias("__registered_driver_id"))
            .dropDuplicates(["__vehicle_id"])
        )
        frame = frame.join(vehicles, frame["vehicle_id"] == vehicles["__vehicle_id"], "left").drop("__vehicle_id")
    else:
        frame = frame.withColumn("__registered_driver_id", lit(None).cast("long"))
    frame = frame.withColumn(
        "driver_vehicle_mismatch",
        col("driver_id").isNotNull() & col("vehicle_id").isNotNull()
        & col("__registered_driver_id").isNotNull() & (col("driver_id") != col("__registered_driver_id")),
    ).withColumn(
        "vehicle_driver_unverifiable",
        col("vehicle_id").isNotNull() & col("__registered_driver_id").isNull(),
    ).drop("__registered_driver_id")
    frame = frame.withColumn(
        "acceptance_delay_minutes",
        when(
            col("accepted_at").isNotNull() & col("requested_at").isNotNull(),
            (col("accepted_at").cast("long") - col("requested_at").cast("long")) / lit(60.0),
        ).otherwise(lit(None).cast("double")),
    ).withColumn(
        "trip_duration_minutes",
        when(
            col("ended_at").isNotNull() & col("started_at").isNotNull(),
            (col("ended_at").cast("long") - col("started_at").cast("long")) / lit(60.0),
        ).otherwise(lit(None).cast("double")),
    ).withColumn(
        "is_acceptance_delay_outlier", coalesce(col("acceptance_delay_minutes") > lit(MAX_ACCEPTANCE_DELAY_MINUTES), lit(False))
    ).withColumn(
        "is_trip_duration_outlier", coalesce(col("trip_duration_minutes") > lit(MAX_TRIP_DURATION_MINUTES), lit(False))
    )
    frame = frame.fillna(False, subset=[
        "driver_vehicle_mismatch", "vehicle_driver_unverifiable", "cancel_note_contains_potential_pii",
    ]).withColumn(
        "scd_hash",
        _hash([
            "passenger_id", "driver_id", "vehicle_id", "pickup_zone_id", "dropoff_zone_id", "status",
            "requested_at", "accepted_at", "started_at", "ended_at", "canceled_at",
            "estimated_distance_km", "actual_distance_km", "start_lat", "start_lng", "end_lat", "end_lng",
            "cancel_reason", "cancel_by", "cancel_note", "cancel_note_contains_potential_pii",
            "fare_amount", "source_system",
        ]),
    )
    _overwrite(frame, path, "trips")


def migrate_payments(spark, path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        return
    frame = spark.read.format("delta").load(path)
    prior_duplicate = _existing_bool(frame, "duplicate_provider_ref")
    prior_canonical = (
        col("canonical_payment_id").cast("long")
        if "canonical_payment_id" in frame.columns
        else lit(None).cast("long")
    )
    prior_evidence = frame.select(
        "payment_id",
        prior_duplicate.alias("__was_duplicate_provider_ref"),
        prior_canonical.alias("__was_canonical_payment_id"),
    ).groupBy("payment_id").agg(
        spark_max(col("__was_duplicate_provider_ref").cast("int"))
        .cast("boolean").alias("__prior_duplicate_provider_ref"),
        spark_min(
            when(col("__was_duplicate_provider_ref"), col("__was_canonical_payment_id"))
        ).alias("__prior_canonical_payment_id"),
    )
    raw_currency = trim(col("currency"))
    currency_norm = lower(raw_currency)
    frame = frame.withColumn(
        "currency_was_normalized",
        _existing_bool(frame, "currency_was_normalized")
        | (currency_norm.isin("usd", "us$") & (raw_currency != lit("USD"))),
    ).withColumn(
        "currency", when(currency_norm.isin("usd", "us$"), lit("USD")).otherwise(raw_currency),
    )
    frame = frame.withColumn("__provider_ref_dedup_key", trim(col("provider_ref")))
    pii_flag = _existing_bool(frame, "provider_ref_contains_potential_pii") | coalesce(
        col("provider_ref").rlike(PII_ANY_PATTERN), lit(False)
    )
    frame = frame.withColumn("provider_ref_contains_potential_pii", pii_flag).withColumn(
        "provider_ref", when(col("provider_ref_contains_potential_pii"), lit(None)).otherwise(trim(col("provider_ref")))
    )
    canonical = (
        frame.filter(
            (col("is_current") == lit(True)) & col("__provider_ref_dedup_key").isNotNull()
        )
        .groupBy("__provider_ref_dedup_key")
        .agg(spark_min("payment_id").alias("__canonical_payment_id"))
    )
    frame = frame.join(canonical, "__provider_ref_dedup_key", "left").join(
        prior_evidence, "payment_id", "left"
    ).withColumn(
        "canonical_payment_id",
        when(
            col("__provider_ref_dedup_key").isNull(),
            when(
                coalesce(col("__prior_duplicate_provider_ref"), lit(False)),
                coalesce(col("__prior_canonical_payment_id"), col("payment_id")),
            ).otherwise(col("payment_id")),
        ).otherwise(
            coalesce(col("__canonical_payment_id"), col("payment_id"))
        ),
    ).withColumn(
        "duplicate_provider_ref",
        when(
            col("__provider_ref_dedup_key").isNull(),
            coalesce(col("__prior_duplicate_provider_ref"), lit(False)),
        ).otherwise(col("payment_id") != col("canonical_payment_id")),
    ).drop(
        "__provider_ref_dedup_key",
        "__canonical_payment_id",
        "__prior_duplicate_provider_ref",
        "__prior_canonical_payment_id",
    )
    frame = frame.withColumn("provider_ref_missing", col("provider_ref").isNull()).withColumn(
        "currency_invalid", col("currency").isNull() | (col("currency") != lit("USD"))
    ).fillna(False, subset=[
        "provider_ref_contains_potential_pii", "duplicate_provider_ref", "provider_ref_missing",
        "currency_was_normalized", "currency_invalid",
    ]).withColumn(
        "scd_hash",
        _hash([
            "trip_id", "method", "status", "amount", "currency", "provider_ref",
            "provider_ref_contains_potential_pii", "duplicate_provider_ref", "canonical_payment_id",
            "paid_at", "source_system",
        ]),
    )
    _overwrite(frame, path, "payments")


def migrate_ratings(spark, path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        return
    frame = spark.read.format("delta").load(path)
    pii_flag = _existing_bool(frame, "comment_contains_potential_pii") | coalesce(
        col("comment").rlike(PII_ANY_PATTERN), lit(False)
    )
    frame = frame.withColumn("comment_contains_potential_pii", pii_flag)
    frame = frame.withColumn("comment", regexp_replace(col("comment"), PII_CONTACT_SUFFIX_PATTERN, ""))
    frame = frame.withColumn("comment", regexp_replace(col("comment"), PII_EMAIL_PATTERN, "[REDACTED]"))
    frame = frame.withColumn("comment", regexp_replace(col("comment"), PII_PHONE_PATTERN, "[REDACTED]"))
    frame = frame.withColumn("comment", trim(col("comment"))).withColumn(
        "comment",
        when(col("comment").isNull() | lower(col("comment")).isin(*NULL_LIKE_VALUES), lit(None))
        .otherwise(col("comment")),
    ).withColumn("comment_missing", col("comment").isNull()).withColumn(
        "scd_hash",
        _hash([
            "trip_id", "passenger_id", "driver_id", "score", "comment",
            "comment_contains_potential_pii", "created_at", "source_system",
        ]),
    )
    _overwrite(frame, path, "ratings")


def main() -> None:
    settings = Settings.from_env()
    spark = build_spark(JOB_NAME)
    try:
        passengers = settings.path("silver", "passengers")
        vehicles = settings.path("silver", "vehicles")
        migrate_passengers(spark, passengers)
        migrate_vehicles(spark, vehicles)
        migrate_trips(spark, settings.path("silver", "trips"), vehicles)
        migrate_payments(spark, settings.path("silver", "payments"))
        migrate_ratings(spark, settings.path("silver", "ratings"))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
