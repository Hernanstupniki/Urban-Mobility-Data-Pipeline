"""Correct-by-construction Gold dimension builders.

Silver owns the SCD2 history: the versioning MERGE (business key, scd_hash,
valid_from/valid_to, is_current, close-then-insert, idempotent replay) lives
exclusively in the Silver jobs. Gold must never recompute a second SCD2, so:

- ``hist`` is a pure projection of the Silver SCD2 rows. It only adds a
  deterministic ``surrogate_key`` (hash of business key + valid_from) and an
  unknown member, so facts can point at the exact version that was valid when
  an event happened via a temporal lookup (see gold_marts._temporal_skeys).
- ``snapshot`` is the current-version view of the same Silver rows (the same
  surrogate key for the current version), used as the convenience "today"
  dimension.

A former ``scd3`` variant (prev_* columns) was retired because no fact,
aggregate or serving consumer used it; point-in-time analysis is served by the
hist + temporal-lookup pattern instead.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import Column, col, current_timestamp, lit, pmod, xxhash64

from src.common.config import Settings
from src.common.contracts import DIMENSION_COLUMNS, DIMENSION_KEYS
from src.common.logging import log_event
from src.common.spark import build_spark


SCD2_COLUMNS = ["scd_hash", "valid_from", "valid_to", "is_current"]

SURROGATE_KEY_COLUMN = "surrogate_key"


def _require(frame: DataFrame, columns: list[str], source: str) -> None:
    missing = [name for name in columns if name not in frame.columns]
    if missing:
        raise ValueError(f"{source} is missing contract columns: {missing}")


def _write(frame: DataFrame, path: str) -> int:
    cached = frame.cache()
    count = cached.count()
    cached.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    cached.unpersist()
    return count


def surrogate_key(business_key: Column, valid_from: Column) -> Column:
    """Deterministic, stable-across-reruns surrogate for one SCD2 version.

    The pair (business key, valid_from) uniquely identifies a version because
    the Silver merge only opens a new version when scd_hash changed and
    raw_loaded_at strictly increased. The hash keeps reruns idempotent (same
    data -> same keys) without needing an sequence/row_number that would shift
    when unrelated entities gain versions.
    """
    return (
        pmod(xxhash64(business_key.cast("string"), valid_from.cast("string")), lit(2**62 - 1))
        + lit(1)
    ).cast("long").alias(SURROGATE_KEY_COLUMN)


def _with_unknown_member(spark, frame: DataFrame, key: str) -> DataFrame:
    expressions = []
    for field in frame.schema.fields:
        if field.name == key:
            value = lit(0)
        elif field.name == SURROGATE_KEY_COLUMN:
            value = lit(0)
        elif field.name == "dwh_loaded_at":
            value = current_timestamp()
        elif field.dataType.typeName() == "boolean":
            value = lit(False)
        elif field.dataType.typeName() in {"byte", "short", "integer", "long", "float", "double", "decimal"}:
            value = lit(0)
        elif field.dataType.typeName() == "string" and field.name in {"full_name", "license_number", "plate_number", "status"}:
            value = lit("UNKNOWN")
        else:
            value = lit(None)
        expressions.append(value.cast(field.dataType).alias(field.name))
    return spark.range(1).select(*expressions).unionByName(frame)


def build_hist_frame(source: DataFrame, entity: str) -> DataFrame:
    """Project Silver SCD2 rows verbatim plus the version surrogate key."""
    key = DIMENSION_KEYS[entity]
    _require(source, DIMENSION_COLUMNS[entity] + SCD2_COLUMNS, "silver")
    return source.select(
        *DIMENSION_COLUMNS[entity],
        *SCD2_COLUMNS,
        surrogate_key(col(key), col("valid_from")),
    )


def build_dimension(entity: str, variant: str) -> None:
    if entity not in DIMENSION_KEYS or variant not in {"snapshot", "hist"}:
        raise ValueError("Unsupported dimension contract")
    settings = Settings.from_env()
    job_name = f"dim_{entity}_{variant}"
    silver_path = settings.path("silver", f"{entity}s" if entity != "passenger" else "passengers")
    table_name = f"dim_{entity}_hist" if variant == "hist" else f"dim_{entity}"
    gold_path = settings.path("gold", "_conformed", variant, table_name)
    spark = build_spark(job_name)
    try:
        if not DeltaTable.isDeltaTable(spark, silver_path):
            raise RuntimeError(f"Required Silver table not found: {silver_path}")
        source = spark.read.format("delta").load(silver_path)
        key = DIMENSION_KEYS[entity]

        if variant == "hist":
            output = build_hist_frame(source, entity)
            output = output.withColumn("dwh_loaded_at", current_timestamp())
            output = _with_unknown_member(spark, output, key)
        else:
            output = (
                source.filter(col("is_current") == lit(True))
                .select(*DIMENSION_COLUMNS[entity], surrogate_key(col(key), col("valid_from")))
                .withColumn("dwh_loaded_at", current_timestamp())
            )
            output = _with_unknown_member(spark, output, key)

        row_count = _write(output, gold_path)
        log_event(job_name, "build", "SUCCESS", row_count=row_count, target=gold_path)
    finally:
        spark.stop()
