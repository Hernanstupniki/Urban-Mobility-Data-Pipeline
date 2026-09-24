"""Spark expressions for source text that needs explicit normalization."""

from pyspark.sql.functions import (
    abs as spark_abs,
    coalesce,
    col,
    initcap,
    lit,
    lower,
    regexp_replace,
    trim,
    try_to_timestamp,
    to_utc_timestamp,
    unix_timestamp,
    upper,
    when,
)


def normalized_person_name(value):
    compact = regexp_replace(trim(value), r"\s+", " ")
    return when(upper(compact) == "ANONYMIZED", lit("ANONYMIZED")).otherwise(
        initcap(lower(compact))
    )


def normalize_requested_at_source(frame):
    """Parse Buenos Aires wall time and keep typed UTC time if text is bad."""
    if "requested_at_source" not in frame.columns:
        frame = frame.withColumn("requested_at_source", lit(None).cast("string"))
    source = trim(col("requested_at_source"))
    parsed = coalesce(*(
        try_to_timestamp(source, lit(pattern))
        for pattern in (
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy/MM/dd HH:mm:ss",
            "dd/MM/yyyy HH:mm:ss",
        )
    ))
    parsed_utc = to_utc_timestamp(parsed, "America/Argentina/Buenos_Aires")
    typed = col("requested_at").cast("timestamp")
    consistent = parsed_utc.isNotNull() & typed.isNotNull() & (
        spark_abs(unix_timestamp(parsed_utc) - unix_timestamp(typed)) <= 1
    )
    present = source.isNotNull() & (source != "")
    return (
        frame.withColumn(
            "requested_at_was_normalized",
            coalesce(consistent & ~source.rlike(r"^\d{4}-\d{2}-\d{2}T"), lit(False)),
        )
        .withColumn(
            "requested_at_source_invalid",
            coalesce(present & ~consistent, lit(False)),
        )
        .withColumn("requested_at", when(consistent, parsed_utc).otherwise(typed))
    )
