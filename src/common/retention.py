"""Safe retention policies for Bronze raw data, Silver SCD2 history and Gold physical cleanup.

Bronze/Silver delete aged rows and then vacuum. Gold is derived and rebuilt
deterministically, so it is vacuum-only: no row deletion, just removal of
obsolete files left behind by each rebuild.
"""

from __future__ import annotations

import os
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_date, date_sub, expr

from src.common.config import Settings, env_bool, safe_child, safe_table_list
from src.common.logging import log_event
from src.common.spark import build_spark


ALL_TABLES = "passengers,drivers,vehicles,ratings,trips,payments,zones"

# Whitelist of relative paths inside DATA_ROOT/ENV/gold for vacuum-only cleanup.
GOLD_TABLES = [
    "_conformed/hist/dim_driver_hist",
    "_conformed/hist/dim_passenger_hist",
    "_conformed/hist/dim_vehicle_hist",
    "_conformed/snapshot/dim_driver",
    "_conformed/snapshot/dim_passenger",
    "_conformed/snapshot/dim_vehicle",
    "_conformed/scd3/dim_driver",
    "_conformed/scd3/dim_passenger",
    "_conformed/scd3/dim_vehicle",
    "_conformed/static/dim_date",
    "_conformed/static/dim_payment_method",
    "_conformed/static/dim_zone",
    "_marts/facts/fact_trips",
    "_marts/facts/fact_payments",
    "_marts/aggregates/agg_trips_daily",
    "_marts/aggregates/agg_driver_daily",
]


def _positive_int(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 0:
        raise ValueError(f"{name} cannot be negative")
    return value


def _validated_base(settings: Settings, layer: str) -> str:
    env_name = f"{layer.upper()}_BASE_PATH"
    base = os.getenv(env_name, settings.path(layer))
    allowed = Path(settings.env_root).resolve()
    resolved = Path(base).resolve()
    try:
        resolved.relative_to(allowed)
    except ValueError as exc:
        raise ValueError(f"{env_name} must stay inside {allowed}") from exc
    return str(resolved)


def run_retention(layer: str) -> None:
    if layer not in {"bronze", "silver", "gold"}:
        raise ValueError("layer must be bronze, silver or gold")
    settings = Settings.from_env()
    job_name = f"{layer}_retention_cleanup"
    base = _validated_base(settings, layer)
    if layer == "gold":
        tables = [t.strip() for t in os.getenv("TABLES", ",".join(GOLD_TABLES)).split(",") if t.strip()]
        unknown = [t for t in tables if t not in GOLD_TABLES]
        if unknown:
            raise ValueError(f"Unknown gold table(s): {unknown}")
    else:
        tables = safe_table_list(os.getenv("TABLES", ALL_TABLES))
    retention_days = _positive_int("RETENTION_DAYS", 14 if layer == "bronze" else 30)
    default_vacuum_hours = 168 if layer == "bronze" else (retention_days * 24 if layer == "silver" else 60 * 24)
    vacuum_hours = _positive_int("VACUUM_RETAIN_HOURS", default_vacuum_hours)
    skip_vacuum = env_bool("SKIP_VACUUM", False)
    unsafe_vacuum = env_bool("UNSAFE_VACUUM", False)
    count_before = env_bool("COUNT_BEFORE_DELETE", False)

    if vacuum_hours < 168 and not (settings.env == "dev" and unsafe_vacuum):
        raise ValueError("VACUUM below 168 hours requires ENV=dev and UNSAFE_VACUUM=1")
    if unsafe_vacuum and settings.env != "dev":
        raise ValueError("UNSAFE_VACUUM is restricted to ENV=dev")

    if unsafe_vacuum:
        os.environ.setdefault("DELTA_RETENTION_DURATION_CHECK_ENABLED", "false")
    spark = build_spark(job_name)
    try:
        for table in tables:
            path = safe_child(base, table)
            if not DeltaTable.isDeltaTable(spark, path):
                log_event(job_name, table, "SKIPPED", reason="not_delta", target=path)
                continue
            if layer == "gold":
                # Derived layer: vacuum obsolete rebuild files, never delete rows.
                target = DeltaTable.forPath(spark, path)
                if not skip_vacuum:
                    target.vacuum(vacuum_hours)
                log_event(job_name, table, "SUCCESS", vacuum_hours=None if skip_vacuum else vacuum_hours)
                continue
            frame = spark.read.format("delta").load(path)
            if layer == "bronze":
                if "load_date" in frame.columns:
                    condition = col("load_date") < date_sub(current_date(), retention_days)
                elif "raw_loaded_at" in frame.columns:
                    condition = col("raw_loaded_at") < expr(f"current_timestamp() - INTERVAL {retention_days} DAYS")
                else:
                    raise ValueError(f"{path} has no retention timestamp")
            else:
                required = {"is_current", "valid_to"}
                if not required.issubset(frame.columns):
                    raise ValueError(f"{path} is missing Silver SCD2 columns: {sorted(required - set(frame.columns))}")
                condition = (
                    (col("is_current") == expr("false"))
                    & col("valid_to").isNotNull()
                    & (col("valid_to") < expr(f"current_timestamp() - INTERVAL {retention_days} DAYS"))
                )
            row_count = frame.filter(condition).count() if count_before else None
            target = DeltaTable.forPath(spark, path)
            target.delete(condition)
            if not skip_vacuum:
                target.vacuum(vacuum_hours)
            log_event(job_name, table, "SUCCESS", row_count=row_count, vacuum_hours=None if skip_vacuum else vacuum_hours)
    finally:
        spark.stop()
