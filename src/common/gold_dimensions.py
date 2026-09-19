"""Correct-by-construction Gold dimension builders."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lag, lit
from pyspark.sql.window import Window

from src.common.config import Settings
from src.common.contracts import DIMENSION_COLUMNS, DIMENSION_KEYS, SCD3_PREVIOUS_COLUMNS
from src.common.logging import log_event
from src.common.spark import build_spark


SCD2_COLUMNS = ["scd_hash", "valid_from", "valid_to", "is_current"]


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


def _with_unknown_member(spark, frame: DataFrame, key: str) -> DataFrame:
    expressions = []
    for field in frame.schema.fields:
        if field.name == key:
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


def build_scd3_frame(source: DataFrame, entity: str) -> DataFrame:
    """Return the current row plus the immediate prior business attributes."""
    key = DIMENSION_KEYS[entity]
    columns = DIMENSION_COLUMNS[entity]
    order = Window.partitionBy(key).orderBy(col("valid_from").asc(), col("raw_loaded_at").asc())
    enriched = source
    previous = SCD3_PREVIOUS_COLUMNS[entity]
    for name in previous:
        enriched = enriched.withColumn(f"prev_{name}", lag(col(name)).over(order))
    return enriched.filter(col("is_current") == lit(True)).select(*columns, *[f"prev_{name}" for name in previous])


def build_dimension(entity: str, variant: str) -> None:
    if entity not in DIMENSION_KEYS or variant not in {"snapshot", "hist", "scd3"}:
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
        columns = DIMENSION_COLUMNS[entity]
        key = DIMENSION_KEYS[entity]
        _require(source, columns + SCD2_COLUMNS, silver_path)

        if variant == "hist":
            output = source.select(*columns, *SCD2_COLUMNS).withColumn("dwh_loaded_at", current_timestamp())
        elif variant == "snapshot":
            output = source.filter(col("is_current") == lit(True)).select(*columns).withColumn("dwh_loaded_at", current_timestamp())
            output = _with_unknown_member(spark, output, key)
        else:
            output = build_scd3_frame(source, entity).withColumn("dwh_loaded_at", current_timestamp())

        row_count = _write(output, gold_path)
        log_event(job_name, "build", "SUCCESS", row_count=row_count, target=gold_path)
    finally:
        spark.stop()
